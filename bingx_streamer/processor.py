import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

from .aggregator import CandleAggregator
from .config import Config
from .models import Candle, StreamStats
from .persistence import DataPersistence

log = logging.getLogger("bingx.processor")


class DataProcessor:
    """Handles processing of incoming candle data"""

    def __init__(
        self, config: Config, stats: StreamStats, save_data: bool = True
    ) -> None:
        self.config: Config = config
        self.stats: StreamStats = stats
        self._lock: threading.Lock = threading.Lock()

        self.aggregator: CandleAggregator = CandleAggregator(
            config=self.config,
            on_bucket_complete=self._on_bucket_complete,
        )
        self.persistence: Optional[DataPersistence] = (
            DataPersistence(config=self.config) if save_data else None
        )

        self.recent_candles: Deque[Candle] = deque(
            maxlen=self.config.MAX_MEMORY_CANDLES
        )
        self.current_candle_ts: Optional[int] = None
        self.last_candle_data: Optional[Dict[str, Any]] = None
        self.last_processed_ts: Optional[int] = None

    def process_historical_candle(self, kline: Dict[str, Any]) -> None:
        """Process a historical candle (already marked as closed)"""
        candle: Candle = Candle(
            open_ts=kline["t"],
            close_ts=kline["T"],
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            trades=kline.get("n", 0),
        )
        with self._lock:
            self.last_processed_ts = candle.close_ts
            self.recent_candles.append(candle)

        if self.aggregator.add_candle(candle):
            with self._lock:
                self.stats.buckets_completed += 1

    def process_kline(self, kline: Dict[str, Any]) -> None:
        """Process individual kline data - detect closes by timestamp changes"""
        with self._lock:
            current_ts: int = kline["T"]

            if (
                self.current_candle_ts is not None
                and current_ts > self.current_candle_ts
                and self.last_candle_data
            ):
                candle: Candle = Candle(
                    open_ts=self.last_candle_data["T"],
                    close_ts=self.last_candle_data["T"] + 3 * 60 * 1000 - 1,
                    open=float(self.last_candle_data["o"]),
                    high=float(self.last_candle_data["h"]),
                    low=float(self.last_candle_data["l"]),
                    close=float(self.last_candle_data["c"]),
                    volume=float(self.last_candle_data["v"]),
                    trades=self.last_candle_data.get("n", 0),
                )
                self.stats.candles_processed += 1
                self.stats.last_candle_time = candle.timestamp_utc
                self.last_processed_ts = candle.close_ts
                log.info(f"Closed candle: {candle}")
                self.recent_candles.append(candle)

                if self.aggregator.add_candle(candle):
                    self.stats.buckets_completed += 1

            self.current_candle_ts = current_ts
            self.last_candle_data = kline.copy()

            if self.stats.messages_received % 30 == 1:
                ts: datetime = datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc)
                log.debug(
                    f"Live update: {ts:%H:%M:%S} UTC, Price: {float(kline['c']):.2f}"
                )

    def _on_bucket_complete(self, aggregated: Dict[str, Any]) -> None:
        """Handle completed bucket"""
        start: datetime = datetime.fromtimestamp(
            aggregated["open_ts"] / 1000, tz=timezone.utc
        )
        end: datetime = datetime.fromtimestamp(
            aggregated["close_ts"] / 1000, tz=timezone.utc
        )

        log_msg: str = (
            f"Bucket complete: {start:%Y-%m-%d %H:%M:%S}-{end:%H:%M:%S} | "
            f"O: {aggregated['open']:.2f} | H: {aggregated['high']:.2f} | "
            f"L: {aggregated['low']:.2f} | C: {aggregated['close']:.2f} | "
            f"V: {aggregated['volume']:.2f} | Candles: {aggregated['candle_count']}"
        )

        if "ma_short" in aggregated and aggregated["ma_short"]:
            log_msg += f" | MA{self.aggregator.ma_calculator.short_period}: {aggregated['ma_short']:.2f}"
        if "ma_long" in aggregated and aggregated["ma_long"]:
            log_msg += f" | MA{self.aggregator.ma_calculator.long_period}: {aggregated['ma_long']:.2f}"
        if "trend" in aggregated and aggregated["trend"]:
            log_msg += f" | Trend: {aggregated['trend']}"
        if "ma_cross" in aggregated and aggregated["ma_cross"]:
            log_msg += f" | 🚨 {aggregated['ma_cross']}"
        if "atr" in aggregated and aggregated["atr"]:
            log_msg += f" | ATR({self.aggregator.atr_calculator.period}): {aggregated['atr']:.2f} ({aggregated['atr_percent']:.2f}%)"

        log.debug(log_msg)

        if self.persistence:
            self.persistence.save_aggregated_candle(aggregated)

    def get_recent_candles(self, count: int = 100) -> List[Candle]:
        """Get recent candles from buffer"""
        with self._lock:
            return list(self.recent_candles)[-count:]

    def get_aggregated_candles(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get recent aggregated candles"""
        with self.aggregator._lock:
            return list(self.aggregator.completed_buckets)[-count:]
