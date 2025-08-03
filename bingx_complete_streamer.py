#!/usr/bin/env python3
"""
bingx_complete_stream.py
───────────────────────
Complete BingX streaming solution with integrated history backfilling and ATR calculation.

Features:
• Historical data backfill on startup
• Seamless transition to live streaming
• Gap detection and automatic filling on reconnect
• Unified candle processing pipeline
• State persistence and recovery
• Production-ready error handling
• Moving Average calculations with crossover detection
• ATR (Average True Range) calculation for volatility measurement
"""

import asyncio
import argparse
import gzip
import io
import json
import logging
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Deque, Callable, Set
import csv
import threading
from concurrent.futures import ThreadPoolExecutor
import psutil
from http.server import HTTPServer, BaseHTTPRequestHandler
import socket
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from memory_profiler import profile
import httpx

import websockets
from websockets.asyncio.client import ClientConnection
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# ═══════════════════════════════════════════ Configuration ═══════════════════════════════════════════

class Config:
    """Centralized configuration management"""
    # WebSocket settings
    WS_URL: str = "wss://open-api-swap.bingx.com/swap-market"
    SYMBOL: str = "BTC-USDT"
    INTERVAL: str = "3m"
    
    # REST API settings
    REST_BASE_URL: str = "https://open-api.bingx.com"
    REST_ENDPOINT: str = "/openApi/swap/v2/quote/klines"
    
    # Backfill settings
    HISTORY_DAYS: int = 7
    BACKFILL_ON_START: bool = True
    FILL_GAPS_ON_RECONNECT: bool = True
    MAX_CANDLES_PER_REQUEST: int = 1000
    
    # Reconnection settings
    INITIAL_RECONNECT_DELAY: float = 1.0
    MAX_RECONNECT_DELAY: float = 60.0
    RECONNECT_MULTIPLIER: float = 1.5
    
    # Aggregation settings
    STANDARD_BUCKET_MINUTES: int = 21  # 7 × 3m candles
    FINAL_BUCKET_MINUTES: int = 12     # 4 × 3m candles
    FINAL_BUCKET_START_HOUR: int = 23
    FINAL_BUCKET_START_MINUTE: int = 48
    SKIP_PARTIAL_BUCKETS: bool = False
    
    # Data persistence
    OUTPUT_DIR: Path = Path("bingx_data")
    MAX_MEMORY_CANDLES: int = 1000  # Reduced from 10000
    SAVE_INTERVAL_SECONDS: int = 300
    
    # Logging
    LOG_LEVEL: int = logging.INFO
    LOG_FORMAT: str = "%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s"
    LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
    
    # Technical indicators - DEFAULT VALUES THAT CAN BE OVERRIDDEN
    MA_SHORT_PERIOD: int = 55  # Changed from 9 to 3 for faster testing
    MA_LONG_PERIOD: int = 123   # Changed from 21 to 7 for faster testing
    CALCULATE_MA: bool = True
    ATR_PERIOD: int = 14        # ATR period (TradingView default)
    CALCULATE_ATR: bool = True   # Enable ATR calculation
    
    # Performance monitoring
    STATS_INTERVAL_SECONDS: int = 60

    # Graceful degradation
    CPU_THRESHOLD: float = 85.0  # %
    MEMORY_THRESHOLD: float = 85.0  # %
    MAX_SLEEP_ON_LOAD: float = 1.0  # seconds

    # Health check
    HEALTH_CHECK_PORT: int = 0  # 0 means find a free port

    # Memory-mapped files
    USE_MEMORY_MAPPED_FILES: bool = False

# ═══════════════════════════════════════════ System Health & Load ═══════════════════════════════════════════

class SystemLoadMonitor:
    """Monitors system CPU and memory usage"""
    @staticmethod
    def get_load() -> Dict[str, float]:
        """Get current CPU and memory usage"""
        return {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent,
        }

    @staticmethod
    async def sleep_if_under_load(config: Config = Config()) -> None:
        """Sleep for a duration proportional to system load if it exceeds thresholds"""
        load: Dict[str, float] = SystemLoadMonitor.get_load()
        cpu: float = load["cpu_percent"]
        mem: float = load["memory_percent"]

        if cpu > config.CPU_THRESHOLD or mem > config.MEMORY_THRESHOLD:
            # Calculate sleep duration, more load = longer sleep
            cpu_factor: float = max(0, (cpu - config.CPU_THRESHOLD) / (100 - config.CPU_THRESHOLD))
            mem_factor: float = max(0, (mem - config.MEMORY_THRESHOLD) / (100 - config.MEMORY_THRESHOLD))
            load_factor: float = max(cpu_factor, mem_factor)

            sleep_duration: float = config.MAX_SLEEP_ON_LOAD * load_factor

            log.warning(
                f"High system load detected (CPU: {cpu:.1f}%, Mem: {mem:.1f}%). "
                f"Throttling for {sleep_duration:.3f}s."
            )
            await asyncio.sleep(sleep_duration)

class HealthCheckServer(threading.Thread):
    """Simple HTTP server for health checks"""
    def __init__(self, port: int, client: "BingXCompleteClient") -> None:
        super().__init__(daemon=True)
        self.port: int = port
        self.client: "BingXCompleteClient" = client
        self.server: Optional[HTTPServer] = None

    def run(self) -> None:
        handler = self._create_handler()
        self.server = HTTPServer(("", self.port), handler)
        log.info(f"Health check server started on http://localhost:{self.port}")
        self.server.serve_forever()

    def _create_handler(self) -> Callable[..., BaseHTTPRequestHandler]:
        client = self.client
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == '/health':
                    is_healthy, reason = client.is_healthy()
                    if is_healthy:
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({"status": "ok"}).encode())
                    else:
                        self.send_response(503)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({"status": "unhealthy", "reason": reason}).encode())
                else:
                    self.send_response(404)
                    self.end_headers()
        return HealthCheckHandler

    def shutdown(self) -> None:
        if self.server:
            log.info("Shutting down health check server...")
            self.server.shutdown()

def find_free_port() -> int:
    """Find an available port on the host"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

# ═══════════════════════════════════════════ Logging Setup ═══════════════════════════════════════════

def setup_logging(level: int = Config.LOG_LEVEL) -> logging.Logger:
    """Configure structured logging with multiple handlers"""
    log_dir: Path = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Get the logger
    logger: logging.Logger = logging.getLogger("bingx")
    logger.setLevel(level)
    logger.propagate = False  # Prevent duplicate logs
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Console handler
    console_handler: logging.StreamHandler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter(Config.LOG_FORMAT, Config.LOG_DATE_FORMAT)
    )
    logger.addHandler(console_handler)
    
    # File handler
    file_handler: logging.FileHandler = logging.FileHandler(
        log_dir / f"bingx_{datetime.now():%Y%m%d_%H%M%S}.log",
        encoding='utf-8'  # Ensure UTF-8 encoding
    )
    file_handler.setFormatter(
        logging.Formatter(Config.LOG_FORMAT, Config.LOG_DATE_FORMAT)
    )
    logger.addHandler(file_handler)
    
    return logger

log: logging.Logger = setup_logging()

# ═══════════════════════════════════════════ Data Models ═══════════════════════════════════════════

class ConnectionState(Enum):
    """WebSocket connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    SUBSCRIBING = "subscribing"
    STREAMING = "streaming"
    RECONNECTING = "reconnecting"
    BACKFILLING = "backfilling"

@dataclass(slots=True)
class Candle:
    """Individual kline/candle data"""
    open_ts: int
    close_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int = 0
    
    @property
    def duration_minutes(self) -> float:
        """Calculate candle duration in minutes"""
        return (self.close_ts - self.open_ts) / (1000 * 60)
    
    @property
    def timestamp_utc(self) -> datetime:
        """Get open timestamp as UTC datetime"""
        return datetime.fromtimestamp(self.open_ts / 1000, tz=timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            **asdict(self),
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "duration_minutes": self.duration_minutes
        }
    
    def __str__(self) -> str:
        ts: datetime = self.timestamp_utc
        return (f"{ts:%Y-%m-%d %H:%M:%S} | "
                f"O: {self.open:.2f} | H: {self.high:.2f} | "
                f"L: {self.low:.2f} | C: {self.close:.2f} | "
                f"V: {self.volume:.2f}")

@dataclass(slots=True)
class StreamStats:
    """Performance and connection statistics"""
    connected_at: Optional[datetime] = None
    messages_received: int = 0
    bytes_received: int = 0
    candles_processed: int = 0
    historical_candles_loaded: int = 0
    buckets_completed: int = 0
    gaps_filled: int = 0
    reconnect_count: int = 0
    last_candle_time: Optional[datetime] = None

    # Comprehensive error metrics
    total_errors: int = 0
    ws_errors: int = 0
    api_errors: int = 0
    processing_errors: int = 0
    disconnections: int = 0
    
    def uptime_seconds(self) -> float:
        """Calculate connection uptime"""
        if not self.connected_at:
            return 0.0
        return (datetime.now(timezone.utc) - self.connected_at).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization, handling datetimes."""
        data = asdict(self)
        if self.connected_at:
            data['connected_at'] = self.connected_at.isoformat()
        if self.last_candle_time:
            data['last_candle_time'] = self.last_candle_time.isoformat()
        return data
    
    def __str__(self) -> str:
        uptime: timedelta = timedelta(seconds=int(self.uptime_seconds()))
        error_summary = (f"Errors(T/W/A/P/D): {self.total_errors}/{self.ws_errors}/"
                         f"{self.api_errors}/{self.processing_errors}/{self.disconnections}")
        return (f"Uptime: {uptime} | Msgs: {self.messages_received} | "
                f"Candles: {self.candles_processed} (Hist: {self.historical_candles_loaded}) | "
                f"Buckets: {self.buckets_completed} | Gaps: {self.gaps_filled} | "
                f"Reconnects: {self.reconnect_count} | {error_summary}")

@dataclass(slots=True)
class ATRData:
    """ATR calculation result"""
    tr: float
    atr: float
    atr_percent: float

# ═══════════════════════════════════════════ History Backfiller ═══════════════════════════════════════════

class NetworkError(Exception):
    """Network-related errors"""
    pass

class HistoryBackfiller:
    """Handles historical data fetching and gap filling"""

    def __init__(self, config: Config = Config()) -> None:
        self.config: Config = config
        self.log: logging.Logger = logging.getLogger("backfiller")
        self.client: httpx.AsyncClient = httpx.AsyncClient(timeout=30)

    @property
    def rest_url(self) -> str:
        return f"{self.config.REST_BASE_URL}{self.config.REST_ENDPOINT}"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NetworkError),
    )
    async def fetch_history_chunk(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch a single chunk of historical candles"""
        symbol = symbol or self.config.SYMBOL
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": self.config.INTERVAL,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": self.config.MAX_CANDLES_PER_REQUEST,
        }

        try:
            self.log.debug(
                f"Fetching {symbol} from {start_time:%Y-%m-%d %H:%M} "
                f"to {end_time:%Y-%m-%d %H:%M}"
            )
            self.log.debug("Making request to REST API")
            response: httpx.Response = await self.client.get(self.rest_url, params=params)
            self.log.debug("Request complete")
            response.raise_for_status()

            data: Dict[str, Any] = response.json()
            if data.get("code") != 0:
                raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

            candles: List[Dict[str, Any]] = data.get("data", [])
            self.log.info(
                f"Fetched {len(candles)} candles for {symbol} "
                f"({start_time:%H:%M} - {end_time:%H:%M})"
            )
            return sorted(candles, key=lambda x: int(x["time"]))

        except httpx.RequestError as e:
            raise NetworkError(f"Network error: {e}") from e
        except Exception:
            # Catch any other unexpected errors during API call
            self.log.error("An unexpected error occurred during history fetch.", exc_info=True)
            raise

    async def stream_history(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Optional[str] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> None:
        """Stream historical candles chunk by chunk and process them"""
        symbol = symbol or self.config.SYMBOL
        current_start: datetime = start_time
        total_fetched: int = 0
        self.log.info("In stream_history")
        while current_start < end_time:
            chunk_end: datetime = min(
                current_start + timedelta(hours=50),
                end_time
            )
            self.log.info(f"Fetching chunk from {current_start} to {chunk_end}")
            candles: List[Dict[str, Any]] = await self.fetch_history_chunk(current_start, chunk_end, symbol)
            self.log.info(f"Fetched {len(candles)} candles")
            if candles:
                total_fetched += len(candles)
                if process_chunk:
                    process_chunk(candles)

            # Move to next chunk
            current_start = chunk_end

            # Small delay to avoid rate limits
            if current_start < end_time:
                await asyncio.sleep(0.5)

        self.log.info(f"Total fetched: {total_fetched} candles for {symbol}")

    def convert_to_websocket_format(self, api_candle: Dict[str, Any]) -> Dict[str, Any]:
        """Convert REST API candle format to WebSocket format"""
        timestamp: int = int(api_candle["time"])
        return {
            "T": timestamp,
            "t": timestamp,
            "o": api_candle["open"],
            "h": api_candle["high"],
            "l": api_candle["low"],
            "c": api_candle["close"],
            "v": api_candle["volume"],
            "n": api_candle.get("count", 0),
            "x": True,  # Mark as closed
        }

    async def backfill(
        self,
        days: Optional[int] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> None:
        """Perform initial backfill by streaming chunks"""
        days = days or self.config.HISTORY_DAYS
        end_time: datetime = datetime.now(timezone.utc)
        start_time: datetime = end_time - timedelta(days=days)

        self.log.info(f"Starting backfill for {days} days of {self.config.SYMBOL}")
        self.log.info("In backfill method")
        try:
            self.log.info("Calling stream_history")
            await self.stream_history(start_time, end_time, process_chunk=process_chunk)
            self.log.info("stream_history finished")
        except Exception as e:
            self.log.error(f"Backfill failed: {e}")

    async def fill_gap(
        self,
        last_timestamp: int,
        symbol: Optional[str] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> None:
        """Fill gap by streaming chunks"""
        symbol = symbol or self.config.SYMBOL
        start_time: datetime = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)
        end_time: datetime = datetime.now(timezone.utc)

        gap_minutes: float = (end_time - start_time).total_seconds() / 60

        if gap_minutes < 3:  # Less than one candle
            return

        self.log.info(
            f"Filling gap for {symbol}: {gap_minutes:.1f} minutes "
            f"from {start_time:%Y-%m-%d %H:%M}"
        )

        try:
            await self.stream_history(start_time, end_time, symbol, process_chunk)
        except Exception as e:
            self.log.error(f"Gap fill failed: {e}")

# ═══════════════════════════════════════════ Technical Indicators ═══════════════════════════════════════

class MovingAverageCalculator:
    """Calculate moving averages for aggregated candles"""
    
    def __init__(self, short_period: int = 9, long_period: int = 21) -> None:
        self.short_period: int = short_period
        self.long_period: int = long_period
        self.price_history: Deque[float] = deque(maxlen=max(short_period, long_period) + 1)
        self.prev_short_ma: Optional[float] = None
        self.prev_long_ma: Optional[float] = None
        self._lock: threading.Lock = threading.Lock()
        
    def add_price(self, close_price: float) -> Dict[str, Optional[Any]]:
        """Add a new close price and calculate MAs"""
        with self._lock:
            # Calculate MAs before adding new price (for previous values)
            prev_short: Optional[float] = self._calculate_ma_unsafe(self.short_period)
            prev_long: Optional[float] = self._calculate_ma_unsafe(self.long_period)

            # Add new price
            self.price_history.append(close_price)

            # Calculate new MAs
            ma_short: Optional[float] = self._calculate_ma_unsafe(self.short_period)
            ma_long: Optional[float] = self._calculate_ma_unsafe(self.long_period)
        
        with self._lock:
            # Detect crossover using stored previous values
            cross_signal: Optional[str] = None
            if (self.prev_short_ma is not None and self.prev_long_ma is not None and
                ma_short is not None and ma_long is not None):

                tolerance: float = 0.01

                # Previous: short below long, Current: short above long = Golden Cross
                if self.prev_short_ma <= self.prev_long_ma + tolerance and ma_short > ma_long + tolerance:
                    cross_signal = "GOLDEN_CROSS"
                    log.debug(f"📊 MA Crossover detected: GOLDEN CROSS | "
                            f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                            f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}")

                # Previous: short above long, Current: short below long = Death Cross
                elif self.prev_short_ma >= self.prev_long_ma - tolerance and ma_short < ma_long - tolerance:
                    cross_signal = "DEATH_CROSS"
                    log.debug(f"📊 MA Crossover detected: DEATH CROSS | "
                            f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                            f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}")
            
            # Update previous values for next iteration
            self.prev_short_ma = ma_short
            self.prev_long_ma = ma_long
            
            # Get trend
            trend: Optional[str] = self._get_trend_unsafe()
            
            # Log MA values periodically for debugging
            if ma_short is not None and ma_long is not None and len(self.price_history) % 5 == 0:
                log.debug(f"MA Update: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f} "
                         f"Diff={(ma_short - ma_long):.2f} Trend={trend}")

            return {
                "ma_short": ma_short,
                "ma_long": ma_long,
                "ma_cross": cross_signal,
                "trend": trend
            }
    
    def _calculate_ma_unsafe(self, period: int) -> Optional[float]:
        """Calculate simple moving average for given period (not thread-safe)"""
        if len(self.price_history) < period:
            return None
        
        relevant_prices: List[float] = list(self.price_history)[-period:]
        return sum(relevant_prices) / period
    
    def calculate_ma(self, period: int) -> Optional[float]:
        """Calculate simple moving average for given period (thread-safe)"""
        with self._lock:
            return self._calculate_ma_unsafe(period)

    def _get_trend_unsafe(self) -> Optional[str]:
        """Get current trend based on MA positions (not thread-safe)"""
        short_ma: Optional[float] = self._calculate_ma_unsafe(self.short_period)
        long_ma: Optional[float] = self._calculate_ma_unsafe(self.long_period)
        
        if not short_ma or not long_ma:
            return None
            
        tolerance: float = 0.01
        
        if short_ma > long_ma + tolerance:
            return "BULLISH"
        elif short_ma < long_ma - tolerance:
            return "BEARISH"
        else:
            return "NEUTRAL"

    def get_trend(self) -> Optional[str]:
        """Get current trend based on MA positions (thread-safe)"""
        with self._lock:
            return self._get_trend_unsafe()

class ATRCalculator:
    """
    Calculate Average True Range (ATR) using TradingView's RMA method.
    
    The RMA (Running Moving Average) formula used by TradingView:
    - First value: Simple average of first N periods
    - Subsequent: (Previous RMA × (N-1) + Current Value) / N
    """
    
    def __init__(self, period: int = 14) -> None:
        """
        Initialize ATR calculator.
        
        Args:
            period: Number of periods for ATR calculation (default: 14)
        """
        self.period: int = period
        self.prev_close: Optional[float] = None
        self.tr_values: Deque[float] = deque(maxlen=period)
        self.atr: Optional[float] = None
        self.candle_count: int = 0
        self._lock: threading.Lock = threading.Lock()
        
    def add_candle(self, high: float, low: float, close: float) -> Optional[ATRData]:
        """
        Add a new candle and calculate ATR.
        
        Args:
            high: High price of the candle
            low: Low price of the candle
            close: Close price of the candle
            
        Returns:
            ATRData object if ATR can be calculated, None otherwise
        """
        with self._lock:
            tr: float
            if self.prev_close is None:
                tr = high - low
            else:
                tr = max(
                    high - low,
                    abs(high - self.prev_close),
                    abs(low - self.prev_close)
                )

            self.tr_values.append(tr)
            self.candle_count += 1

            atr_value: Optional[float] = None
            atr_percent: Optional[float] = None

            if self.candle_count == self.period:
                atr_value = sum(self.tr_values) / self.period
                self.atr = atr_value
                atr_percent = (atr_value / close) * 100 if close > 0 else 0.0
            elif self.candle_count > self.period:
                if self.atr is not None:
                    atr_value = ((self.atr * (self.period - 1)) + tr) / self.period
                    self.atr = atr_value
                    atr_percent = (atr_value / close) * 100 if close > 0 else 0.0

            self.prev_close = close

            if atr_value is not None and atr_percent is not None:
                return ATRData(
                    tr=tr,
                    atr=atr_value,
                    atr_percent=atr_percent
                )
            return None
    
    def get_current_atr(self) -> Optional[float]:
        """Get current ATR value"""
        with self._lock:
            return self.atr
    
    def reset(self) -> None:
        """Reset calculator state"""
        with self._lock:
            self.prev_close = None
            self.tr_values.clear()
            self.atr = None
            self.candle_count = 0

# ═══════════════════════════════════════════ Aggregation Logic ═══════════════════════════════════════

class CandleAggregator:
    """Handles candle bucketing and aggregation logic"""
    
    def __init__(self, on_bucket_complete: Optional[Callable[[Dict[str, Any]], None]] = None,
                 skip_partial_buckets: bool = Config.SKIP_PARTIAL_BUCKETS,
                 calculate_ma: bool = Config.CALCULATE_MA,
                 ma_short: int = Config.MA_SHORT_PERIOD,
                 ma_long: int = Config.MA_LONG_PERIOD,
                 calculate_atr: bool = Config.CALCULATE_ATR,
                 atr_period: int = Config.ATR_PERIOD) -> None:
        self.buckets: Dict[Tuple[str, int], List[Candle]] = {}
        self.on_bucket_complete: Optional[Callable[[Dict[str, Any]], None]] = on_bucket_complete
        self.completed_buckets: Deque[Dict[str, Any]] = deque(maxlen=200)
        self.skip_partial_buckets: bool = skip_partial_buckets
        self.partial_bucket_warned: Set[Tuple[str, int]] = set()
        
        self.calculate_ma: bool = calculate_ma
        self.ma_calculator: Optional[MovingAverageCalculator] = MovingAverageCalculator(ma_short, ma_long) if calculate_ma else None
        
        self.calculate_atr: bool = calculate_atr
        self.atr_calculator: Optional[ATRCalculator] = ATRCalculator(atr_period) if calculate_atr else None
        self._lock: threading.Lock = threading.Lock()
        
    def get_bucket_info(self, dt: datetime) -> Tuple[int, int]:
        """
        Determine bucket index and expected candle count for a given time.
        Returns: (bucket_index, expected_candle_count)
        """
        minute_of_day: int = dt.hour * 60 + dt.minute
        final_start: int = Config.FINAL_BUCKET_START_HOUR * 60 + Config.FINAL_BUCKET_START_MINUTE
        
        if minute_of_day >= final_start:
            bucket_idx: int = 68
            expected_count: int = Config.FINAL_BUCKET_MINUTES // 3
        else:
            bucket_idx = minute_of_day // Config.STANDARD_BUCKET_MINUTES
            expected_count = Config.STANDARD_BUCKET_MINUTES // 3
            
        return bucket_idx, expected_count
    
    def add_candle(self, candle: Candle) -> Optional[Dict[str, Any]]:
        """
        Add a candle to the appropriate bucket.
        Returns aggregated candle dict if bucket is complete, None otherwise.
        """
        dt: datetime = candle.timestamp_utc
        date_str: str = dt.strftime("%Y-%m-%d")
        bucket_idx, expected_count = self.get_bucket_info(dt)
        key: Tuple[str, int] = (date_str, bucket_idx)

        with self._lock:
            bucket: List[Candle] = self.buckets.setdefault(key, [])

            # Prevent adding duplicates
            if any(existing.open_ts == candle.open_ts for existing in bucket):
                log.warning(f"Duplicate candle detected at {candle.timestamp_utc}, skipping")
                return None
            
            bucket.append(candle)
            
            log.debug(f"Added to bucket {key}: {len(bucket)}/{expected_count} candles")
            
            if len(bucket) in [1, expected_count // 2, expected_count - 1]:
                log.debug(f"Bucket progress {key}: {len(bucket)}/{expected_count} candles")
            
            if len(bucket) == expected_count:
                aggregated: Dict[str, Any] = self.aggregate_candles(bucket)
                aggregated["bucket_info"] = {
                    "date": date_str,
                    "index": bucket_idx,
                    "candle_count": len(bucket)
                }
                
                self.completed_buckets.append(aggregated)
                del self.buckets[key]

                # Perform calculations outside the main lock if possible
                if self.calculate_ma and self.ma_calculator:
                    ma_data: Dict[str, Optional[Any]] = self.ma_calculator.add_price(aggregated["close"])
                    aggregated.update(ma_data)

                if self.calculate_atr and self.atr_calculator:
                    atr_data: Optional[ATRData] = self.atr_calculator.add_candle(
                        aggregated["high"], aggregated["low"], aggregated["close"]
                    )
                    if atr_data:
                        aggregated.update(asdict(atr_data))

                if self.on_bucket_complete:
                    self.on_bucket_complete(aggregated)

                return aggregated

            return None
    
    @staticmethod
    def aggregate_candles(candles: List[Candle]) -> Dict[str, Any]:
        """Aggregate multiple candles into a single larger candle"""
        if not candles:
            raise ValueError("Cannot aggregate empty candle list")
        
        sorted_candles: List[Candle] = sorted(candles, key=lambda c: c.open_ts)
        
        for i in range(1, len(sorted_candles)):
            expected_ts: int = sorted_candles[i-1].open_ts + (3 * 60 * 1000)
            if sorted_candles[i].open_ts != expected_ts:
                gap_minutes: float = (sorted_candles[i].open_ts - sorted_candles[i-1].open_ts) / (60 * 1000)
                log.warning(
                    f"Gap detected: {gap_minutes:.1f} minutes between candles {i-1} and {i} "
                    f"({sorted_candles[i-1].timestamp_utc:%H:%M:%S} -> {sorted_candles[i].timestamp_utc:%H:%M:%S})"
                )
        
        first_candle: Candle = sorted_candles[0]
        last_candle: Candle = sorted_candles[-1]
        
        log.debug(f"Aggregating {len(sorted_candles)} candles:")
        log.debug(f"  First: {first_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Open: {first_candle.open:.2f}")
        log.debug(f"  Last:  {last_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Close: {last_candle.close:.2f}")
        log.debug(f"  High:  {max(c.high for c in sorted_candles):.2f}")
        log.debug(f"  Low:   {min(c.low for c in sorted_candles):.2f}")
        
        if log.isEnabledFor(logging.DEBUG):
            for i, c in enumerate(sorted_candles):
                log.debug(f"    {i+1}. {c.timestamp_utc:%H:%M:%S} O:{c.open:.2f} H:{c.high:.2f} L:{c.low:.2f} C:{c.close:.2f} V:{c.volume:.2f}")
            
        return {
            "open_ts": first_candle.open_ts,
            "close_ts": last_candle.close_ts,
            "open": first_candle.open,
            "high": max(c.high for c in sorted_candles),
            "low": min(c.low for c in sorted_candles),
            "close": last_candle.close,
            "volume": sum(c.volume for c in sorted_candles),
            "trades": sum(c.trades for c in sorted_candles),
            "candle_count": len(sorted_candles),
            "timestamp_utc": first_candle.timestamp_utc.isoformat(),
            "first_candle_time": first_candle.timestamp_utc.strftime("%H:%M:%S"),
            "last_candle_time": last_candle.timestamp_utc.strftime("%H:%M:%S"),
        }
    
    def get_incomplete_buckets(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all incomplete buckets for persistence"""
        with self._lock:
            result: Dict[str, List[Dict[str, Any]]] = {}
            for (date_str, bucket_idx), candles in self.buckets.items():
                key: str = f"{date_str}_bucket_{bucket_idx}"
                result[key] = [c.to_dict() for c in candles]
            return result
    
    def clear(self) -> None:
        """Clear all buckets"""
        with self._lock:
            self.buckets.clear()
            self.completed_buckets.clear()
            self.partial_bucket_warned.clear()

# ═══════════════════════════════════════════ Data Persistence ═══════════════════════════════════════

class ParquetDataset:
    """Handles loading and accessing of Parquet data"""

    def __init__(self, directory: Path, use_memory_map: bool = False) -> None:
        self.directory: Path = directory
        self.use_memory_map: bool = use_memory_map
        self.dataset: Optional[pq.ParquetDataset] = None
        self.table: Optional[pa.Table] = None

        if not self.directory.exists():
            log.warning(f"Parquet directory not found: {self.directory}")
            return

        try:
            self.dataset = pq.ParquetDataset(self.directory)
            log.info(f"Loaded Parquet dataset with {len(self.dataset.fragments)} fragments.")
            self.table = self.dataset.read(use_threads=True, use_pandas_metadata=False)
            log.info("Memory-mapping enabled." if self.use_memory_map else "Dataset loaded into memory.")
        except Exception as e:
            log.error(f"Error loading Parquet dataset: {e}", exc_info=True)

    def get_full_data(self) -> Optional[pd.DataFrame]:
        """Return the entire dataset as a Pandas DataFrame"""
        if not self.table:
            return None
        return self.table.to_pandas()

    def get_date_range(self) -> Optional[Tuple[datetime, datetime]]:
        """Get the min and max date from the dataset partitions"""
        if not self.dataset or not self.dataset.partitions:
            return None

        dates: List[datetime] = [
            datetime.strptime(part.partition_string.split('=')[1], "%Y-%m-%d")
            for part in self.dataset.partitions
        ]
        return min(dates), max(dates)

class DataPersistence:
    """Handles saving candle data to disk using Parquet format"""
    
    def __init__(self, output_dir: Path = Config.OUTPUT_DIR, buffer_size: int = 100) -> None:
        log.info("Initializing DataPersistence")
        self.output_dir: Path = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        self.parquet_dir: Path = self.output_dir / "parquet"
        self.parquet_dir.mkdir(exist_ok=True)
        log.info(f"Parquet directory: {self.parquet_dir}")
        
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_size: int = buffer_size
        self._lock: threading.Lock = threading.Lock()
        
    def save_aggregated_candle(self, candle: Dict[str, Any]) -> None:
        """Add aggregated candle to buffer and flush if full"""
        with self._lock:
            self.buffer.append(candle)
            should_flush: bool = len(self.buffer) >= self.buffer_size

        if should_flush:
            self.flush_buffer()
            
    def flush_buffer(self) -> None:
        """Write buffered candles to Parquet files"""
        with self._lock:
            if not self.buffer:
                return
            
            buffer_copy: List[Dict[str, Any]] = list(self.buffer)
            self.buffer.clear()

        log.info(f"Flushing buffer with {len(buffer_copy)} items.")

        df: pd.DataFrame = pd.DataFrame(buffer_copy)

        try:
            df['date'] = df['bucket_info'].apply(lambda x: x['date'])
            df['bucket_index'] = df['bucket_info'].apply(lambda x: x['index'])

            schema: pa.Schema = pa.schema([
                pa.field("open_ts", pa.int64()), pa.field("close_ts", pa.int64()),
                pa.field("open", pa.float64()), pa.field("high", pa.float64()),
                pa.field("low", pa.float64()), pa.field("close", pa.float64()),
                pa.field("volume", pa.float64()), pa.field("trades", pa.int64()),
                pa.field("candle_count", pa.int32()), pa.field("timestamp_utc", pa.string()),
                pa.field("first_candle_time", pa.string()), pa.field("last_candle_time", pa.string()),
                pa.field("bucket_info", pa.struct([
                    pa.field("date", pa.string()), pa.field("index", pa.int32()),
                    pa.field("candle_count", pa.int32())
                ])),
                pa.field("ma_short", pa.float64()), pa.field("ma_long", pa.float64()),
                pa.field("ma_cross", pa.string()), pa.field("trend", pa.string()),
                pa.field("tr", pa.float64()), pa.field("atr", pa.float64()),
                pa.field("atr_percent", pa.float64()),
                pa.field("date", pa.string()), pa.field("bucket_index", pa.int32())
            ])

            table: pa.Table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

            pq.write_to_dataset(
                table,
                root_path=self.parquet_dir,
                partition_cols=['date'],
                existing_data_behavior='overwrite_or_ignore'
            )
            log.info(f"Flushed {len(df)} records to Parquet dataset")

        except Exception as e:
            log.error(f"Failed to flush buffer to Parquet: {e}", exc_info=True)
            with self._lock:
                self.buffer.extend(df.to_dict('records'))
            
    def save_state(self, aggregator: "CandleAggregator", stats: "StreamStats",
                   last_candle_ts: Optional[int] = None) -> None:
        """Save current state for recovery"""
        with self._lock:
            state: Dict[str, Any] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "last_candle_timestamp": last_candle_ts,
                "stats": stats.to_dict(),
                "incomplete_buckets": aggregator.get_incomplete_buckets(),
                "completed_buckets_count": len(aggregator.completed_buckets),
                "persistence_buffer": self.buffer,
            }
        
        state_file: Path = self.output_dir / "state.json"
        try:
            with open(state_file, "w", encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            log.info(f"State saved to {state_file}")
        except (TypeError, OverflowError, IOError) as e:
            log.error(f"Error saving state to JSON: {e}")
            # Fallback for serialization errors
            try:
                with open(state_file.with_suffix(".log"), "w", encoding='utf-8') as f:
                    f.write(str(state))
                log.warning(f"State saved to fallback log file: {state_file.with_suffix('.log')}")
            except Exception as log_e:
                log.error(f"Could not write to fallback log file: {log_e}")
    
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load saved state"""
        state_file: Path = self.output_dir / "state.json"
        if not state_file.exists():
            return None
            
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f"Failed to load state: {e}")
            return None

# ═══════════════════════════════════════════ Data Processor ═══════════════════════════════════════════

class DataProcessor:
    """Handles processing of incoming candle data"""
    def __init__(self, client: "BingXCompleteClient", config: Config):
        self.client: "BingXCompleteClient" = client
        self.config: Config = config
        self.stats: StreamStats = client.stats
        self._lock: threading.Lock = threading.Lock()

        self.aggregator: CandleAggregator = CandleAggregator(
            on_bucket_complete=self._on_bucket_complete,
            skip_partial_buckets=self.config.SKIP_PARTIAL_BUCKETS,
            calculate_ma=self.config.CALCULATE_MA,
            ma_short=self.config.MA_SHORT_PERIOD,
            ma_long=self.config.MA_LONG_PERIOD,
            calculate_atr=self.config.CALCULATE_ATR,
            atr_period=self.config.ATR_PERIOD
        )
        self.persistence: Optional[DataPersistence] = DataPersistence() if self.client.save_data else None

        self.recent_candles: Deque[Candle] = deque(maxlen=self.config.MAX_MEMORY_CANDLES)
        self.current_candle_ts: Optional[int] = None
        self.last_candle_data: Optional[Dict[str, Any]] = None
        self.last_processed_ts: Optional[int] = None

    def process_historical_candle(self, kline: Dict[str, Any]) -> None:
        """Process a historical candle (already marked as closed)"""
        candle: Candle = Candle(
            open_ts=kline["t"], close_ts=kline["T"],
            open=float(kline["o"]), high=float(kline["h"]),
            low=float(kline["l"]), close=float(kline["c"]),
            volume=float(kline["v"]), trades=kline.get("n", 0)
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

            if self.current_candle_ts is not None and current_ts > self.current_candle_ts and self.last_candle_data:
                candle: Candle = Candle(
                    open_ts=self.last_candle_data["T"],
                    close_ts=self.last_candle_data["T"] + 3 * 60 * 1000 - 1,
                    open=float(self.last_candle_data["o"]), high=float(self.last_candle_data["h"]),
                    low=float(self.last_candle_data["l"]), close=float(self.last_candle_data["c"]),
                    volume=float(self.last_candle_data["v"]), trades=self.last_candle_data.get("n", 0)
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
                log.debug(f"Live update: {ts:%H:%M:%S} UTC, Price: {float(kline['c']):.2f}")

    def _on_bucket_complete(self, aggregated: Dict[str, Any]) -> None:
        """Handle completed bucket"""
        start: datetime = datetime.fromtimestamp(aggregated["open_ts"] / 1000, tz=timezone.utc)
        end: datetime = datetime.fromtimestamp(aggregated["close_ts"] / 1000, tz=timezone.utc)

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

# ═══════════════════════════════════════════ Complete Client ═══════════════════════════════════════

class BingXCompleteClient:
    """Complete BingX client with history backfill and WebSocket streaming"""
    
    def __init__(
        self,
        symbol: str = Config.SYMBOL,
        interval: str = Config.INTERVAL,
        save_data: bool = True,
        backfill_days: Optional[int] = None
    ) -> None:
        self.symbol: str = symbol
        self.interval: str = interval
        self.save_data: bool = save_data
        self.backfill_days: int = backfill_days or Config.HISTORY_DAYS
        
        self.subscribe_msg: Dict[str, str] = {
            "id": f"{symbol}-{interval}",
            "reqType": "sub",
            "dataType": f"{symbol}@kline_{interval}",
        }
        
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self._shutdown: asyncio.Event = asyncio.Event()
        self._ws: Optional[ClientConnection] = None
        
        self.stats: StreamStats = StreamStats()
        self.config: Config = Config()
        
        self.processor: DataProcessor = DataProcessor(self, self.config)
        self.backfiller: HistoryBackfiller = HistoryBackfiller()
        
        self.dataset: Optional[ParquetDataset] = None
        if self.config.USE_MEMORY_MAPPED_FILES:
            log.info("Memory-mapped file option is enabled. Initializing ParquetDataset.")
            self.dataset = ParquetDataset(
                directory=self.config.OUTPUT_DIR / "parquet",
                use_memory_map=True
            )
        
        self._tasks: List[asyncio.Task] = []
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
            time_since_last_candle: float = (datetime.now(timezone.utc).timestamp() * 1000 - self.last_processed_ts) / 1000
            if time_since_last_candle > 300: # 5 minutes
                return False, f"No candle processed in {time_since_last_candle:.1f}s"
        return True, "OK"

    async def run(self) -> None:
        """Main entry point - run the client with all features"""
        log.info(f"Starting BingX Complete Client for {self.symbol} {self.interval}")
        if self.processor.persistence:
            state: Optional[Dict[str, Any]] = self.processor.persistence.load_state()
            if state:
                self.processor.last_processed_ts = state.get("last_candle_timestamp")
                log.info(f"Loaded state: last candle at {self.last_processed_ts}")
                if 'persistence_buffer' in state:
                    self.processor.persistence.buffer = state['persistence_buffer']
                    log.info(f"Restored {len(self.processor.persistence.buffer)} items to persistence buffer")
        
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
                    df = df.sort_values(by='open_ts').reset_index(drop=True)
                    for _, row in df.iterrows():
                        candle = Candle(
                            open_ts=row['open_ts'], close_ts=row['close_ts'],
                            open=row['open'], high=row['high'], low=row['low'],
                            close=row['close'], volume=row['volume'],
                            trades=row.get('trades', 0)
                        )
                        self.processor.recent_candles.append(candle)
                        self.processor.aggregator.add_candle(candle)
                        self.processor.last_processed_ts = candle.close_ts
                    self.stats.historical_candles_loaded = len(df)
                    self.stats.buckets_completed = len(self.processor.aggregator.completed_buckets)
                    log.info(f"Loaded {len(df)} candles and {self.stats.buckets_completed} buckets from Parquet.")
                    return
            except Exception as e:
                log.error(f"Failed to load from Parquet, falling back to API: {e}", exc_info=True)

        log.info(f"Starting historical backfill from API for {self.backfill_days} days")

        def process_chunk(candles: List[Dict[str, Any]]) -> None:
            log.info(f"Processing chunk of {len(candles)} historical candles")
            converted: List[Dict[str, Any]] = [self.backfiller.convert_to_websocket_format(c) for c in candles]
            for candle_data in converted:
                self.processor.process_historical_candle(candle_data)
            self.stats.historical_candles_loaded += len(candles)

        try:
            backfiller: HistoryBackfiller = HistoryBackfiller()
            backfiller.config.SYMBOL = self.symbol
            await backfiller.backfill(
                days=self.backfill_days,
                process_chunk=process_chunk
            )
            log.info(f"Historical backfill complete: {self.stats.historical_candles_loaded} candles, "
                     f"{self.stats.buckets_completed} buckets")
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
                
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.WebSocketException,
                    asyncio.TimeoutError, ConnectionError) as e:
                self.stats.ws_errors += 1
                self.stats.total_errors += 1
                self.stats.disconnections += 1
                consecutive_errors += 1
                log.warning(f"Connection error ({type(e).__name__}): {e}")
                
                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    if consecutive_errors > 5:
                        delay = min(delay * 2, self.config.MAX_RECONNECT_DELAY)
                        log.warning(f"Multiple connection failures ({consecutive_errors}), increasing delay to {delay}s")
                    
                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    delay = min(delay * self.config.RECONNECT_MULTIPLIER, self.config.MAX_RECONNECT_DELAY)
                    self.stats.reconnect_count += 1
                    
            except Exception as e:
                self.stats.total_errors += 1
                log.error(f"Unexpected error in connection loop: {e}", exc_info=True)
                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    delay = min(delay * self.config.RECONNECT_MULTIPLIER, self.config.MAX_RECONNECT_DELAY)
                    self.stats.reconnect_count += 1
                    
    async def _fill_gaps(self) -> None:
        """Fill gaps between last processed candle and current time"""
        if not self.last_processed_ts:
            return

        gaps_found: bool = False
        def process_chunk(candles: List[Dict[str, Any]]) -> None:
            nonlocal gaps_found
            if not candles: return
            gaps_found = True
            log.info(f"Filling gap with chunk of {len(candles)} candles")
            converted: List[Dict[str, Any]] = [self.backfiller.convert_to_websocket_format(c) for c in candles]
            for candle_data in converted:
                self.processor.process_historical_candle(candle_data)

        try:
            log.info("Checking for gaps in data...")
            await self.backfiller.fill_gap(
                last_timestamp=self.last_processed_ts,
                symbol=self.symbol,
                process_chunk=process_chunk
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
                self.config.WS_URL, ping_interval=20, ping_timeout=20,
                close_timeout=10, max_size=10 * 1024 * 1024
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
        if not self._ws: return
        self.state = ConnectionState.SUBSCRIBING
        log.info(f"Subscribing to {self.symbol} {self.interval} klines")
        await self._ws.send(json.dumps(self.subscribe_msg))
        self.state = ConnectionState.STREAMING
        
    async def _message_loop(self, ws: ClientConnection) -> None:
        """Process incoming WebSocket messages"""
        try:
            async for message in ws:
                if self._shutdown.is_set(): break
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
            if self.processor.persistence and (self.stats.candles_processed > 0 or self.stats.historical_candles_loaded > 0):
                self.processor.persistence.save_state(self.processor.aggregator, self.stats, self.last_processed_ts)
                    
    async def _cleanup(self) -> None:
        """Clean up resources on shutdown"""
        log.info("Cleaning up resources...")
        for task in self._tasks:
            if not task.done():
                task.cancel()
                
        if self.processor.persistence and self.processor.persistence.buffer:
            log.info(f"Flushing remaining {len(self.processor.persistence.buffer)} items from buffer...")
            self.processor.persistence.flush_buffer()

        if self.processor.persistence:
            self.processor.persistence.save_state(self.processor.aggregator, self.stats, self.last_processed_ts)
            log.info("Final state saved")
                
        if self._ws and not self._ws.closed:
            await self._ws.close()

        if self.health_server:
            self.health_server.shutdown()
            
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
        if not self.processor.aggregator.calculate_ma or not self.processor.aggregator.ma_calculator:
            return {"ma_short": None, "ma_long": None, "trend": None}
        return {
            "ma_short": self.processor.aggregator.ma_calculator.calculate_ma(self.config.MA_SHORT_PERIOD),
            "ma_long": self.processor.aggregator.ma_calculator.calculate_ma(self.config.MA_LONG_PERIOD),
            "trend": self.processor.aggregator.ma_calculator.get_trend()
        }

    def get_current_atr_value(self) -> Dict[str, Optional[float]]:
        if not self.processor.aggregator.calculate_atr or not self.processor.aggregator.atr_calculator:
            return {"atr": None, "atr_percent": None}
        atr: Optional[float] = self.processor.aggregator.atr_calculator.get_current_atr()
        if atr and self.processor.recent_candles:
            last_close: float = self.processor.recent_candles[-1].close
            atr_percent: float = (atr / last_close) * 100 if last_close > 0 else 0.0
            return {"atr": atr, "atr_percent": atr_percent}
        return {"atr": None, "atr_percent": None}

# ═══════════════════════════════════════════ Utility Functions ═══════════════════════════════════════

def print_banner(config: Config = Config()) -> None:
    """Print startup banner"""
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║        BingX Complete Streamer with History v3.0             ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║ Symbol:       {config.SYMBOL:<46} ║")
    print(f"║ Interval:     {config.INTERVAL:<46} ║")
    print(f"║ History Days: {config.HISTORY_DAYS:<46} ║")
    print(f"║ Output:       {str(config.OUTPUT_DIR):<46} ║")
    if config.CALCULATE_MA:
        print(f"║ MA Periods:   Short: {config.MA_SHORT_PERIOD}, Long: {config.MA_LONG_PERIOD:<29} ║")
    if config.CALCULATE_ATR:
        print(f"║ ATR Period:   {config.ATR_PERIOD:<46} ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║ Features:                                                    ║")
    print("║ • Historical backfill on startup                             ║")
    print("║ • Automatic gap detection and filling                        ║")
    print("║ • State persistence and recovery                             ║")
    print("║ • Real-time aggregation to 21/12 minute candles              ║")
    if config.CALCULATE_MA:
        print("║ • Moving average calculations with crossover detection       ║")
    if config.CALCULATE_ATR:
        print("║ • ATR calculation for volatility measurement                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

# ═══════════════════════════════════════════ Main Entry Point ═══════════════════════════════════════

async def main() -> None:
    """Main application entry point"""
    print_banner()
    client: BingXCompleteClient = BingXCompleteClient(
        symbol=Config.SYMBOL,
        interval=Config.INTERVAL,
        save_data=True,
        backfill_days=Config.HISTORY_DAYS
    )
    
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, client.stop)
        
    try:
        await client.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error in main: {e}", exc_info=True)
        # Make sure to increment the total_errors counter
        if client:
            client.stats.total_errors += 1
    finally:
        if client:
            log.info(f"Final Stats: {client.stats}")
        log.info("Application terminated")

# ═══════════════════════════════════════════ CLI Interface ═══════════════════════════════════════════

def create_cli() -> argparse.ArgumentParser:
    """Create command-line interface"""
    parser = argparse.ArgumentParser(
        description="BingX Complete Streamer with Historical Backfill and ATR"
    )
    
    parser.add_argument("--symbol", default="BTC-USDT", help="Trading symbol (default: BTC-USDT)")
    parser.add_argument("--interval", default="3m", choices=["1m", "3m", "5m", "15m", "30m", "1h"], help="Candle interval (default: 3m)")
    parser.add_argument("--history-days", type=int, default=3, help="Days of history to backfill (default: 3)")
    parser.add_argument("--no-backfill", action="store_true", help="Skip initial historical backfill")
    parser.add_argument("--no-gap-fill", action="store_true", help="Disable automatic gap filling on reconnect")
    parser.add_argument("--skip-partial", action="store_true", help="Skip partial buckets when starting mid-day")
    parser.add_argument("--output", type=Path, default=Path("bingx_data"), help="Output directory for data files")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verify-bucket", type=str, help="Verify a specific bucket (format: YYYY-MM-DD HH:MM)")
    parser.add_argument("--show-alignment", action="store_true", help="Show TradingView alignment guide")
    parser.add_argument("--ma-short", type=int, default=3, help="Short moving average period (default: 3)")
    parser.add_argument("--ma-long", type=int, default=7, help="Long moving average period (default: 7)")
    parser.add_argument("--no-ma", action="store_true", help="Disable moving average calculations")
    parser.add_argument("--atr-period", type=int, default=14, help="ATR period (default: 14)")
    parser.add_argument("--no-atr", action="store_true", help="Disable ATR calculations")
    parser.add_argument("--memory-mapped", action="store_true", help="Enable memory-mapped files for large datasets")
    
    return parser

def verify_bucket_alignment(symbol: str, bucket_time: str) -> None:
    """Verify bucket alignment with TradingView"""
    try:
        dt: datetime = datetime.strptime(bucket_time, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        
        minute_of_day: int = dt.hour * 60 + dt.minute
        bucket_minutes: int
        if minute_of_day >= Config.FINAL_BUCKET_START_HOUR * 60 + Config.FINAL_BUCKET_START_MINUTE:
            bucket_minutes = Config.FINAL_BUCKET_MINUTES
        else:
            bucket_minutes = Config.STANDARD_BUCKET_MINUTES
            
        bucket_start_minute: int = (minute_of_day // bucket_minutes) * bucket_minutes
        bucket_start: datetime = dt.replace(hour=bucket_start_minute // 60, minute=bucket_start_minute % 60, second=0, microsecond=0)
        bucket_end: datetime = bucket_start + timedelta(minutes=bucket_minutes - 1, seconds=59)
        
        print(f"\n📊 Bucket Analysis for {symbol} at {bucket_time}")
        print(f"{'='*60}")
        print(f"Bucket period: {bucket_start:%Y-%m-%d %H:%M:%S} - {bucket_end:%H:%M:%S} UTC")
        print(f"Duration: {bucket_minutes} minutes")
        print(f"Expected 3-minute candles: {bucket_minutes // 3}")
        print("\n3-minute candles in this bucket:")
        
        for i in range(bucket_minutes // 3):
            candle_start: datetime = bucket_start + timedelta(minutes=i * 3)
            candle_end: datetime = candle_start + timedelta(minutes=2, seconds=59)
            print(f"  {i+1}. {candle_start:%H:%M:%S} - {candle_end:%H:%M:%S}")
            
        print("\n💡 To verify in TradingView:")
        print(f"1. Set chart to {symbol}")
        print(f"2. Set timeframe to {bucket_minutes} minutes")
        print(f"3. Find candle starting at {bucket_start:%Y-%m-%d %H:%M} UTC")
        print("4. Compare OHLC values with aggregated output")
        
    except ValueError:
        print("❌ Error: Invalid date format. Use YYYY-MM-DD HH:MM")

def verify_tradingview_alignment() -> None:
    """Print TradingView alignment information"""
    print("\n📊 TradingView Alignment Guide")
    print("=" * 80)
    print("\n21-Minute Buckets (Standard):")
    print("Each bucket contains 7 consecutive 3-minute candles")
    print("\nBucket | Start Time | End Time   | 3-min Candles Included")
    print("-" * 60)
    
    for hour in range(24):
        for minute in [0, 21, 42]:
            if hour == 23 and minute > 42: continue
            start_time: str = f"{hour:02d}:{minute:02d}:00"
            end_minute: int = minute + 20
            end_hour: int = hour
            if end_minute >= 60:
                end_minute -= 60
                end_hour += 1
            end_time: str = f"{end_hour:02d}:{end_minute:02d}:59"
            
            candles: List[str] = []
            for i in range(7):
                c_min: int = minute + (i * 3)
                c_hour: int = hour
                if c_min >= 60:
                    c_min -= 60
                    c_hour += 1
                candles.append(f"{c_hour:02d}:{c_min:02d}")
            
            print(f"  {hour * 3 + minute // 21:<3d}  | {start_time} | {end_time} | {', '.join(candles)}")
    
    print("\n12-Minute Bucket (Final):")
    print("  68   | 23:48:00 | 23:59:59 | 23:48, 23:51, 23:54, 23:57")
    print("\n💡 Important Notes:")
    print("1. All times are in UTC")
    print("2. Open price = Open of FIRST 3-min candle")
    print("3. Close price = Close of LAST 3-min candle")
    print("4. High = Highest high of all candles in bucket")
    print("5. Low = Lowest low of all candles in bucket")
    
async def main_cli() -> None:
    """CLI entry point"""
    parser: argparse.ArgumentParser = create_cli()
    args: argparse.Namespace = parser.parse_args()
    
    if args.show_alignment:
        verify_tradingview_alignment()
        return
        
    if args.verify_bucket:
        verify_bucket_alignment(args.symbol, args.verify_bucket)
        return
    
    Config.SYMBOL = args.symbol
    Config.INTERVAL = args.interval
    Config.HISTORY_DAYS = args.history_days
    Config.BACKFILL_ON_START = not args.no_backfill
    Config.FILL_GAPS_ON_RECONNECT = not args.no_gap_fill
    Config.SKIP_PARTIAL_BUCKETS = args.skip_partial
    Config.OUTPUT_DIR = args.output
    Config.MA_SHORT_PERIOD = args.ma_short
    Config.MA_LONG_PERIOD = args.ma_long
    Config.CALCULATE_MA = not args.no_ma
    Config.ATR_PERIOD = args.atr_period
    Config.CALCULATE_ATR = not args.no_atr
    Config.USE_MEMORY_MAPPED_FILES = args.memory_mapped
    
    if args.debug:
        Config.LOG_LEVEL = logging.DEBUG
        
    global log
    log = setup_logging(Config.LOG_LEVEL)
    
    await main()

if __name__ == "__main__":
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    if len(sys.argv) > 1:
        asyncio.run(main_cli())
    else:
        asyncio.run(main())
