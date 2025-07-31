import asyncio
import gzip
import io
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import websockets
from websockets.asyncio.client import ClientConnection

from .backfiller import HistoryBackfiller
from .config import Config
from .health import HealthCheckServer, SystemLoadMonitor, find_free_port
from .models import Candle, ConnectionState, StreamStats
from .persistence import ParquetDataset
from .processor import DataProcessor

log = logging.getLogger("bingx.client")


class BingXCompleteClient:
    """Complete BingX client with history backfill and WebSocket streaming"""

    def __init__(self, config: Config, save_data: bool = True) -> None:
        self.config = config
        self.symbol: str = self.config.SYMBOL
        self.interval: str = self.config.INTERVAL
        self.save_data: bool = save_data
        self.backfill_days: int = self.config.HISTORY_DAYS

        self.subscribe_msg: Dict[str, str] = {
            "id": f"{self.symbol}-{self.interval}",
            "reqType": "sub",
            "dataType": f"{self.symbol}@kline_{self.interval}",
        }

        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self._shutdown: asyncio.Event = asyncio.Event()
        self._ws: Optional[ClientConnection] = None

        self.stats: StreamStats = StreamStats()

        self.processor: DataProcessor = DataProcessor(
            config=self.config, stats=self.stats, save_data=self.save_data
        )
        self.backfiller: HistoryBackfiller = HistoryBackfiller(config=self.config)

        self.dataset: Optional[ParquetDataset] = None
        if self.config.USE_MEMORY_MAPPED_FILES:
            log.info(
                "Memory-mapped file option is enabled. Initializing ParquetDataset."
            )
            self.dataset = ParquetDataset(
                directory=self.config.OUTPUT_DIR / "parquet", use_memory_map=True
            )

        self._tasks: List[asyncio.Task] = []
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=3)
        self._lock: threading.Lock = threading.Lock()

        # Health check server
        self.health_server: Optional[HealthCheckServer] = None

    @property
    def last_processed_ts(self) -> Optional[int]:
        return self.processor.last_processed_ts

    def is_healthy(self) -> Tuple[bool, str]:
        """Check if the client is healthy"""
        if self._shutdown.is_set():
            return False, "Client is shutting down"
        if self.state in [ConnectionState.DISCONNECTED, ConnectionState.RECONNECTING]:
            return False, f"Client is in {self.state.value} state"
        if self.last_processed_ts:
            time_since_last_candle: float = (
                datetime.now(timezone.utc).timestamp() * 1000 - self.last_processed_ts
            ) / 1000
            if time_since_last_candle > 300:  # 5 minutes
                return False, f"No candle processed in {time_since_last_candle:.1f}s"
        return True, "OK"

    async def run(self) -> None:
        """Main entry point - run the client with all features"""
        log.info(f"Starting BingX Complete Client for {self.symbol} {self.interval}")
        if self.processor.persistence:
            state: Optional[Dict[str, Any]] = self.processor.persistence.load_state()
            if state:
                self.processor.last_processed_ts = state.get("last_candle_timestamp")
                log.info(
                    f"Loaded state: last candle at {self.processor.last_processed_ts}"
                )
                if "persistence_buffer" in state:
                    self.processor.persistence.buffer = state["persistence_buffer"]
                    log.info(
                        f"Restored {len(self.processor.persistence.buffer)} items to persistence buffer"
                    )

        if self.config.BACKFILL_ON_START:
            await self._initial_backfill()

        # Start health check server
        port: int = self.config.HEALTH_CHECK_PORT or find_free_port()
        self.health_server = HealthCheckServer(port, self)
        self.health_server.start()

        self._tasks = [
            asyncio.create_task(self._connection_loop()),
            asyncio.create_task(self._stats_reporter()),
            asyncio.create_task(self._auto_save_loop()),
        ]

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            log.info("Client shutdown initiated")
        finally:
            await self._cleanup()

    async def _initial_backfill(self) -> None:
        """Perform initial historical data backfill from Parquet or API"""
        self.state = ConnectionState.BACKFILLING
        if self.dataset and self.dataset.table is not None:
            log.info("Loading historical data from Parquet dataset...")
            try:
                df: Optional[pd.DataFrame] = self.dataset.get_full_data()
                if df is not None:
                    df = df.sort_values(by="open_ts").reset_index(drop=True)
                    for _, row in df.iterrows():
                        candle = Candle(
                            open_ts=row["open_ts"],
                            close_ts=row["close_ts"],
                            open=row["open"],
                            high=row["high"],
                            low=row["low"],
                            close=row["close"],
                            volume=row["volume"],
                            trades=row.get("trades", 0),
                        )
                        self.processor.recent_candles.append(candle)
                        self.processor.aggregator.add_candle(candle)
                        self.processor.last_processed_ts = candle.close_ts
                    self.stats.historical_candles_loaded = len(df)
                    self.stats.buckets_completed = len(
                        self.processor.aggregator.completed_buckets
                    )
                    log.info(
                        f"Loaded {len(df)} candles and {self.stats.buckets_completed} buckets from Parquet."
                    )
                    return
            except Exception as e:
                log.error(
                    f"Failed to load from Parquet, falling back to API: {e}",
                    exc_info=True,
                )

        log.info(f"Starting historical backfill from API for {self.backfill_days} days")

        def process_chunk(candles: List[Dict[str, Any]]) -> None:
            log.info(f"Processing chunk of {len(candles)} historical candles")
            converted: List[Dict[str, Any]] = [
                self.backfiller.convert_to_websocket_format(c) for c in candles
            ]
            for candle_data in converted:
                self.processor.process_historical_candle(candle_data)
            self.stats.historical_candles_loaded += len(candles)

        try:
            loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
            backfiller: HistoryBackfiller = HistoryBackfiller(config=self.config)
            backfiller.config.SYMBOL = self.symbol
            await loop.run_in_executor(
                self._executor, backfiller.backfill, self.backfill_days, process_chunk
            )
            log.info(
                f"Historical backfill complete: {self.stats.historical_candles_loaded} candles, "
                f"{self.stats.buckets_completed} buckets"
            )
        except Exception as e:
            log.error(f"Historical backfill failed: {e}", exc_info=True)
            self.stats.api_errors += 1
            self.stats.total_errors += 1

    async def _connection_loop(self) -> None:
        """Main connection loop with exponential backoff"""
        delay: float = self.config.INITIAL_RECONNECT_DELAY
        consecutive_errors: int = 0

        while not self._shutdown.is_set():
            try:
                self.state = ConnectionState.CONNECTING
                if self.config.FILL_GAPS_ON_RECONNECT and self.last_processed_ts:
                    await self._fill_gaps()

                await self._connect_and_stream()

                delay = self.config.INITIAL_RECONNECT_DELAY
                consecutive_errors = 0

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.WebSocketException,
                asyncio.TimeoutError,
                ConnectionError,
            ) as e:
                self.stats.ws_errors += 1
                self.stats.total_errors += 1
                self.stats.disconnections += 1
                log.warning(f"Connection error ({type(e).__name__}): {e}")

                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    if consecutive_errors > 5:
                        delay = min(delay * 2, self.config.MAX_RECONNECT_DELAY)
                        log.warning(
                            f"Multiple connection failures ({consecutive_errors}), increasing delay to {delay}s"
                        )

                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    delay = min(
                        delay * self.config.RECONNECT_MULTIPLIER,
                        self.config.MAX_RECONNECT_DELAY,
                    )
                    self.stats.reconnect_count += 1

            except Exception as e:
                self.stats.total_errors += 1
                log.error(f"Unexpected error in connection loop: {e}", exc_info=True)
                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    delay = min(
                        delay * self.config.RECONNECT_MULTIPLIER,
                        self.config.MAX_RECONNECT_DELAY,
                    )
                    self.stats.reconnect_count += 1

    async def _fill_gaps(self) -> None:
        """Fill gaps between last processed candle and current time"""
        if not self.last_processed_ts:
            return

        gaps_found: bool = False

        def process_chunk(candles: List[Dict[str, Any]]) -> None:
            nonlocal gaps_found
            if not candles:
                return
            gaps_found = True
            log.info(f"Filling gap with chunk of {len(candles)} candles")
            converted: List[Dict[str, Any]] = [
                self.backfiller.convert_to_websocket_format(c) for c in candles
            ]
            for candle_data in converted:
                self.processor.process_historical_candle(candle_data)

        try:
            log.info("Checking for gaps in data...")
            loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self.backfiller.fill_gap,
                self.last_processed_ts,
                self.symbol,
                process_chunk,
            )
            if gaps_found:
                self.stats.gaps_filled += 1
                log.info("Gap filled successfully")
            else:
                log.info("No gap detected")
        except Exception as e:
            log.error(f"Gap filling failed: {e}", exc_info=True)
            self.stats.api_errors += 1
            self.stats.total_errors += 1

    async def _connect_and_stream(self) -> None:
        """Establish WebSocket connection and stream data"""
        log.info(f"Connecting to {self.config.WS_URL}")
        try:
            async with websockets.connect(
                self.config.WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                self._ws = ws
                self.state = ConnectionState.CONNECTED
                self.stats.connected_at = datetime.now(timezone.utc)
                await self._subscribe()
                await self._message_loop(ws)
        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError) as e:
            log.warning(f"WebSocket connection issue: {e}")
            raise
        except Exception as e:
            log.error(f"WebSocket error: {e}")
            raise

    async def _subscribe(self) -> None:
        """Send subscription message"""
        if not self._ws:
            return
        self.state = ConnectionState.SUBSCRIBING
        log.info(f"Subscribing to {self.symbol} {self.interval} klines")
        await self._ws.send(json.dumps(self.subscribe_msg))
        self.state = ConnectionState.STREAMING

    async def _message_loop(self, ws: ClientConnection) -> None:
        """Process incoming WebSocket messages"""
        try:
            async for message in ws:
                if self._shutdown.is_set():
                    break
                self.stats.messages_received += 1
                try:
                    text: str
                    if isinstance(message, bytes):
                        self.stats.bytes_received += len(message)
                        text = self._decompress_message(message)
                    else:
                        text = str(message)

                    if text == "Ping":
                        await ws.send("Pong")
                        continue

                    await self._process_message(json.loads(text))
                    await SystemLoadMonitor.sleep_if_under_load(self.config)

                except json.JSONDecodeError:
                    log.warning(f"Failed to parse message as JSON: {message}")
                    self.stats.processing_errors += 1
                    self.stats.total_errors += 1
                except Exception as e:
                    self.stats.processing_errors += 1
                    self.stats.total_errors += 1
                    log.error(f"Error processing message: {e}", exc_info=True)
        except websockets.exceptions.ConnectionClosed:
            raise
        except Exception as e:
            log.error(f"Unexpected error in message loop: {e}", exc_info=True)
            raise

    def _decompress_message(self, data: bytes) -> str:
        """Decompress gzip-compressed message"""
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            return gz.read().decode("utf-8")

    async def _process_message(self, message: Dict[str, Any]) -> None:
        """Process parsed WebSocket message"""
        data: Optional[List[Dict[str, Any]]] = message.get("data")
        if not isinstance(data, list):
            return
        for kline_data in data:
            try:
                self.processor.process_kline(kline_data)
            except Exception as e:
                self.stats.processing_errors += 1
                self.stats.total_errors += 1
                log.error(f"Error processing kline: {e}", exc_info=True)

    async def _stats_reporter(self) -> None:
        """Periodically report statistics"""
        while not self._shutdown.is_set():
            await asyncio.sleep(self.config.STATS_INTERVAL_SECONDS)
            if self.state in [ConnectionState.STREAMING, ConnectionState.BACKFILLING]:
                log.info(f"Stats: {self.stats}")

    async def _auto_save_loop(self) -> None:
        """Periodically save state"""
        while not self._shutdown.is_set():
            await asyncio.sleep(self.config.SAVE_INTERVAL_SECONDS)
            if self.processor.persistence and (
                self.stats.candles_processed > 0
                or self.stats.historical_candles_loaded > 0
            ):
                self.processor.persistence.save_state(
                    self.processor.aggregator, self.stats, self.last_processed_ts
                )

    async def _cleanup(self) -> None:
        """Clean up resources on shutdown"""
        log.info("Cleaning up resources...")
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self.processor.persistence and self.processor.persistence.buffer:
            log.info(
                f"Flushing remaining {len(self.processor.persistence.buffer)} items from buffer..."
            )
            self.processor.persistence.flush_buffer()

        if self.processor.persistence:
            self.processor.persistence.save_state(
                self.processor.aggregator, self.stats, self.last_processed_ts
            )
            log.info("Final state saved")

        if self._ws and not self._ws.closed:
            await self._ws.close()

        if self.health_server:
            self.health_server.shutdown()

        self._executor.shutdown(wait=True)
        log.info("Cleanup complete")

    def stop(self) -> None:
        """Signal shutdown"""
        log.info("Shutdown signal received")
        self._shutdown.set()

    # Methods to access processed data, delegated to the processor
    def get_recent_candles(self, count: int = 100) -> List[Candle]:
        return self.processor.get_recent_candles(count)

    def get_aggregated_candles(self, count: int = 50) -> List[Dict[str, Any]]:
        return self.processor.get_aggregated_candles(count)

    def get_current_ma_values(self) -> Dict[str, Optional[Any]]:
        if (
            not self.processor.aggregator.calculate_ma
            or not self.processor.aggregator.ma_calculator
        ):
            return {"ma_short": None, "ma_long": None, "trend": None}
        return {
            "ma_short": self.processor.aggregator.ma_calculator.calculate_ma(
                self.config.MA_SHORT_PERIOD
            ),
            "ma_long": self.processor.aggregator.ma_calculator.calculate_ma(
                self.config.MA_LONG_PERIOD
            ),
            "trend": self.processor.aggregator.ma_calculator.get_trend(),
        }

    def get_current_atr_value(self) -> Dict[str, Optional[float]]:
        if (
            not self.processor.aggregator.calculate_atr
            or not self.processor.aggregator.atr_calculator
        ):
            return {"atr": None, "atr_percent": None}
        atr: Optional[
            float
        ] = self.processor.aggregator.atr_calculator.get_current_atr()
        if atr and self.processor.recent_candles:
            last_close: float = self.processor.recent_candles[-1].close
            atr_percent: float = (atr / last_close) * 100 if last_close > 0 else 0.0
            return {"atr": atr, "atr_percent": atr_percent}
        return {"atr": None, "atr_percent": None}
