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
from typing import Dict, List, Optional, Tuple, Deque, Callable, Set
import csv
import threading
from concurrent.futures import ThreadPoolExecutor
from memory_profiler import profile

import websockets
from websockets.exceptions import WebSocketException
import requests
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
    WS_URL = "wss://open-api-swap.bingx.com/swap-market"
    SYMBOL = "BTC-USDT"
    INTERVAL = "3m"
    
    # REST API settings
    REST_BASE_URL = "https://open-api.bingx.com"
    REST_ENDPOINT = "/openApi/swap/v2/quote/klines"
    
    # Backfill settings
    HISTORY_DAYS = 7
    BACKFILL_ON_START = True
    FILL_GAPS_ON_RECONNECT = True
    MAX_CANDLES_PER_REQUEST = 1000
    
    # Reconnection settings
    INITIAL_RECONNECT_DELAY = 1.0
    MAX_RECONNECT_DELAY = 60.0
    RECONNECT_MULTIPLIER = 1.5
    
    # Aggregation settings
    STANDARD_BUCKET_MINUTES = 21  # 7 × 3m candles
    FINAL_BUCKET_MINUTES = 12     # 4 × 3m candles
    FINAL_BUCKET_START_HOUR = 23
    FINAL_BUCKET_START_MINUTE = 48
    SKIP_PARTIAL_BUCKETS = False
    
    # Data persistence
    OUTPUT_DIR = Path("bingx_data")
    MAX_MEMORY_CANDLES = 1000  # Reduced from 10000
    SAVE_INTERVAL_SECONDS = 300
    
    # Logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    # Technical indicators - DEFAULT VALUES THAT CAN BE OVERRIDDEN
    MA_SHORT_PERIOD = 55  # Changed from 9 to 3 for faster testing
    MA_LONG_PERIOD = 123   # Changed from 21 to 7 for faster testing
    CALCULATE_MA = True
    ATR_PERIOD = 14        # ATR period (TradingView default)
    CALCULATE_ATR = True   # Enable ATR calculation
    
    # Performance monitoring
    STATS_INTERVAL_SECONDS = 60

# ═══════════════════════════════════════════ Logging Setup ═══════════════════════════════════════════

def setup_logging(level: int = Config.LOG_LEVEL) -> logging.Logger:
    """Configure structured logging with multiple handlers"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Get the logger
    logger = logging.getLogger("bingx")
    logger.setLevel(level)
    logger.propagate = False  # Prevent duplicate logs
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter(Config.LOG_FORMAT, Config.LOG_DATE_FORMAT)
    )
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(
        log_dir / f"bingx_{datetime.now():%Y%m%d_%H%M%S}.log",
        encoding='utf-8'  # Ensure UTF-8 encoding
    )
    file_handler.setFormatter(
        logging.Formatter(Config.LOG_FORMAT, Config.LOG_DATE_FORMAT)
    )
    logger.addHandler(file_handler)
    
    return logger

log = setup_logging()

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
    open_ts: int      # Open time in milliseconds
    close_ts: int     # Close time in milliseconds
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
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            **asdict(self),
            "timestamp_utc": self.timestamp_utc.isoformat(),
            "duration_minutes": self.duration_minutes
        }
    
    def __str__(self) -> str:
        ts = self.timestamp_utc
        return (f"{ts:%Y-%m-%d %H:%M:%S} | "
                f"O: {self.open:.2f} | H: {self.high:.2f} | "
                f"L: {self.low:.2f} | C: {self.close:.2f} | "
                f"V: {self.volume:.2f}")

@dataclass(slots=True)
class StreamStats:
    """Performance and connection statistics"""
    connected_at: Optional[datetime] = None
    messages_received: int = 0
    candles_processed: int = 0
    historical_candles_loaded: int = 0
    buckets_completed: int = 0
    errors_count: int = 0
    reconnect_count: int = 0
    gaps_filled: int = 0
    last_candle_time: Optional[datetime] = None
    bytes_received: int = 0
    
    def uptime_seconds(self) -> float:
        """Calculate connection uptime"""
        if not self.connected_at:
            return 0.0
        return (datetime.now(timezone.utc) - self.connected_at).total_seconds()
    
    def __str__(self) -> str:
        uptime = timedelta(seconds=int(self.uptime_seconds()))
        return (f"Uptime: {uptime} | Messages: {self.messages_received} | "
                f"Candles: {self.candles_processed} (Historical: {self.historical_candles_loaded}) | "
                f"Buckets: {self.buckets_completed} | Gaps: {self.gaps_filled} | "
                f"Errors: {self.errors_count} | Reconnects: {self.reconnect_count}")

@dataclass(slots=True)
class ATRData:
    """ATR calculation result"""
    tr: float           # True Range
    atr: float          # Average True Range
    atr_percent: float  # ATR as percentage of close price

# ═══════════════════════════════════════════ History Backfiller ═══════════════════════════════════════════

class NetworkError(Exception):
    """Network-related errors"""
    pass

class HistoryBackfiller:
    """Handles historical data fetching and gap filling"""
    
    def __init__(self, config: Config = Config()):
        self.config = config
        self.log = logging.getLogger("backfiller")
        self.session = requests.Session()
        
    @property
    def rest_url(self) -> str:
        return f"{self.config.REST_BASE_URL}{self.config.REST_ENDPOINT}"
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NetworkError),
    )
    def fetch_history_chunk(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Optional[str] = None
    ) -> List[Dict]:
        """Fetch a single chunk of historical candles"""
        symbol = symbol or self.config.SYMBOL
        params = {
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

            response = self.session.get(self.rest_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") != 0:
                raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

            candles = data.get("data", [])
            self.log.info(
                f"Fetched {len(candles)} candles for {symbol} "
                f"({start_time:%H:%M} - {end_time:%H:%M})"
            )
            return sorted(candles, key=lambda x: int(x["time"]))

        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Network error: {e}") from e

    def stream_history(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Optional[str] = None,
        process_chunk: Callable[[List[Dict]], None] = None
    ):
        """Stream historical candles chunk by chunk and process them"""
        symbol = symbol or self.config.SYMBOL
        current_start = start_time
        total_fetched = 0
        
        while current_start < end_time:
            chunk_end = min(
                current_start + timedelta(hours=50),
                end_time
            )
            
            candles = self.fetch_history_chunk(current_start, chunk_end, symbol)
            
            if candles:
                total_fetched += len(candles)
                if process_chunk:
                    process_chunk(candles)

            # Move to next chunk
            current_start = chunk_end

            # Small delay to avoid rate limits
            if current_start < end_time:
                time.sleep(0.5)
        
        self.log.info(f"Total fetched: {total_fetched} candles for {symbol}")
    
    def convert_to_websocket_format(self, api_candle: Dict) -> Dict:
        """Convert REST API candle format to WebSocket format"""
        timestamp = int(api_candle["time"])
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
    
    def backfill(
        self,
        days: Optional[int] = None,
        process_chunk: Callable[[List[Dict]], None] = None
    ) -> None:
        """Perform initial backfill by streaming chunks"""
        days = days or self.config.HISTORY_DAYS
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        self.log.info(f"Starting backfill for {days} days of {self.config.SYMBOL}")
        
        try:
            self.stream_history(start_time, end_time, process_chunk=process_chunk)
        except Exception as e:
            self.log.error(f"Backfill failed: {e}")

    def fill_gap(
        self,
        last_timestamp: int,
        symbol: Optional[str] = None,
        process_chunk: Callable[[List[Dict]], None] = None
    ) -> None:
        """Fill gap by streaming chunks"""
        symbol = symbol or self.config.SYMBOL
        start_time = datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc)
        end_time = datetime.now(timezone.utc)
        
        gap_minutes = (end_time - start_time).total_seconds() / 60
        
        if gap_minutes < 3:  # Less than one candle
            return
        
        self.log.info(
            f"Filling gap for {symbol}: {gap_minutes:.1f} minutes "
            f"from {start_time:%Y-%m-%d %H:%M}"
        )
        
        try:
            self.stream_history(start_time, end_time, symbol, process_chunk)
        except Exception as e:
            self.log.error(f"Gap fill failed: {e}")

# ═══════════════════════════════════════════ Technical Indicators ═══════════════════════════════════════

class MovingAverageCalculator:
    """Calculate moving averages for aggregated candles"""
    
    def __init__(self, short_period: int = 9, long_period: int = 21):
        self.short_period = short_period
        self.long_period = long_period
        self.price_history: Deque[float] = deque(maxlen=max(short_period, long_period) + 1)
        # Track previous MA values for crossover detection
        self.prev_short_ma: Optional[float] = None
        self.prev_long_ma: Optional[float] = None
        
    def add_price(self, close_price: float) -> Dict[str, Optional[float]]:
        """Add a new close price and calculate MAs"""
        # Calculate MAs before adding new price (for previous values)
        prev_short = self.calculate_ma(self.short_period)
        prev_long = self.calculate_ma(self.long_period)
        
        # Add new price
        self.price_history.append(close_price)
        
        # Calculate new MAs
        ma_short = self.calculate_ma(self.short_period)
        ma_long = self.calculate_ma(self.long_period)
        
        # Detect crossover using stored previous values
        cross_signal = None
        if (self.prev_short_ma is not None and self.prev_long_ma is not None and 
            ma_short is not None and ma_long is not None):
            
            tolerance = 0.01
            
            # Previous: short below long, Current: short above long = Golden Cross
            if self.prev_short_ma <= self.prev_long_ma + tolerance and ma_short > ma_long + tolerance:
                cross_signal = "GOLDEN_CROSS"
                log.info(f"📊 MA Crossover detected: GOLDEN CROSS | "
                        f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                        f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}")
            
            # Previous: short above long, Current: short below long = Death Cross
            elif self.prev_short_ma >= self.prev_long_ma - tolerance and ma_short < ma_long - tolerance:
                cross_signal = "DEATH_CROSS"
                log.info(f"📊 MA Crossover detected: DEATH CROSS | "
                        f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                        f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}")
        
        # Update previous values for next iteration
        self.prev_short_ma = ma_short
        self.prev_long_ma = ma_long
        
        # Get trend
        trend = self.get_trend()
        
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
    
    def calculate_ma(self, period: int) -> Optional[float]:
        """Calculate simple moving average for given period"""
        if len(self.price_history) < period:
            return None
        
        relevant_prices = list(self.price_history)[-period:]
        return sum(relevant_prices) / period
    
    def get_trend(self) -> Optional[str]:
        """Get current trend based on MA positions"""
        short_ma = self.calculate_ma(self.short_period)
        long_ma = self.calculate_ma(self.long_period)
        
        if not short_ma or not long_ma:
            return None
            
        # Use a small tolerance to avoid floating point comparison issues
        tolerance = 0.01
        
        if short_ma > long_ma + tolerance:
            return "BULLISH"
        elif short_ma < long_ma - tolerance:
            return "BEARISH"
        else:
            return "NEUTRAL"

class ATRCalculator:
    """
    Calculate Average True Range (ATR) using TradingView's RMA method.
    
    The RMA (Running Moving Average) formula used by TradingView:
    - First value: Simple average of first N periods
    - Subsequent: (Previous RMA × (N-1) + Current Value) / N
    """
    
    def __init__(self, period: int = 14):
        """
        Initialize ATR calculator.
        
        Args:
            period: Number of periods for ATR calculation (default: 14)
        """
        self.period = period
        self.prev_close: Optional[float] = None
        self.tr_values: deque[float] = deque(maxlen=period)
        self.atr: Optional[float] = None
        self.candle_count = 0
        
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
        # Calculate True Range
        if self.prev_close is None:
            # First candle: TR = High - Low
            tr = high - low
        else:
            # TR = max of:
            # 1. Current High - Current Low
            # 2. abs(Current High - Previous Close)
            # 3. abs(Current Low - Previous Close)
            tr = max(
                high - low,
                abs(high - self.prev_close),
                abs(low - self.prev_close)
            )
        
        self.tr_values.append(tr)
        self.candle_count += 1
        
        # Calculate ATR
        if self.candle_count < self.period:
            # Not enough data yet
            atr_value = None
            atr_percent = None
        elif self.candle_count == self.period:
            # First ATR: Simple average
            atr_value = sum(self.tr_values) / self.period
            self.atr = atr_value
            atr_percent = (atr_value / close) * 100 if close > 0 else 0
        else:
            # Subsequent ATR: RMA formula
            # ATR = ((Previous ATR × (N-1)) + Current TR) / N
            atr_value = ((self.atr * (self.period - 1)) + tr) / self.period
            self.atr = atr_value
            atr_percent = (atr_value / close) * 100 if close > 0 else 0
        
        # Update previous close for next calculation
        self.prev_close = close
        
        if atr_value is not None:
            return ATRData(
                tr=tr,
                atr=atr_value,
                atr_percent=atr_percent
            )
        return None
    
    def get_current_atr(self) -> Optional[float]:
        """Get current ATR value"""
        return self.atr
    
    def reset(self):
        """Reset calculator state"""
        self.prev_close = None
        self.tr_values.clear()
        self.atr = None
        self.candle_count = 0

# ═══════════════════════════════════════════ Aggregation Logic ═══════════════════════════════════════

class CandleAggregator:
    """Handles candle bucketing and aggregation logic"""
    
    def __init__(self, on_bucket_complete: Optional[Callable[[dict], None]] = None,
                 skip_partial_buckets: bool = Config.SKIP_PARTIAL_BUCKETS,
                 calculate_ma: bool = Config.CALCULATE_MA,
                 ma_short: int = Config.MA_SHORT_PERIOD,
                 ma_long: int = Config.MA_LONG_PERIOD,
                 calculate_atr: bool = Config.CALCULATE_ATR,
                 atr_period: int = Config.ATR_PERIOD):
        self.buckets: Dict[Tuple[str, int], List[Candle]] = {}
        self.on_bucket_complete = on_bucket_complete
        self.completed_buckets: Deque[dict] = deque(maxlen=200)  # Reduced from 1000
        self.skip_partial_buckets = skip_partial_buckets
        self.partial_bucket_warned: Set[Tuple[str, int]] = set()
        
        # Moving average calculator
        self.calculate_ma = calculate_ma
        self.ma_calculator = MovingAverageCalculator(ma_short, ma_long) if calculate_ma else None
        
        # ATR calculator
        self.calculate_atr = calculate_atr
        self.atr_calculator = ATRCalculator(atr_period) if calculate_atr else None
        
    def get_bucket_info(self, dt: datetime) -> Tuple[int, int]:
        """
        Determine bucket index and expected candle count for a given time.
        Returns: (bucket_index, expected_candle_count)
        """
        minute_of_day = dt.hour * 60 + dt.minute
        
        final_start = Config.FINAL_BUCKET_START_HOUR * 60 + Config.FINAL_BUCKET_START_MINUTE
        
        if minute_of_day >= final_start:
            bucket_idx = 68
            expected_count = Config.FINAL_BUCKET_MINUTES // 3
        else:
            bucket_idx = minute_of_day // Config.STANDARD_BUCKET_MINUTES
            expected_count = Config.STANDARD_BUCKET_MINUTES // 3
            
        return bucket_idx, expected_count
    
    def add_candle(self, candle: Candle) -> Optional[dict]:
        """
        Add a candle to the appropriate bucket.
        Returns aggregated candle dict if bucket is complete, None otherwise.
        """
        dt = candle.timestamp_utc
        date_str = dt.strftime("%Y-%m-%d")
        bucket_idx, expected_count = self.get_bucket_info(dt)
        
        key = (date_str, bucket_idx)
        bucket = self.buckets.setdefault(key, [])
        
        # Check if this is the first candle in a partial bucket
        if len(bucket) == 0 and key not in self.partial_bucket_warned:
            minute_in_bucket = (dt.hour * 60 + dt.minute) % Config.STANDARD_BUCKET_MINUTES
            if minute_in_bucket > 0:
                self.partial_bucket_warned.add(key)
                log.warning(
                    f"Starting partial bucket {key} at minute {minute_in_bucket}/{Config.STANDARD_BUCKET_MINUTES}. "
                    f"This bucket will only have {(Config.STANDARD_BUCKET_MINUTES - minute_in_bucket) // 3} candles."
                )
                
                if self.skip_partial_buckets:
                    log.info(f"Skipping partial bucket {key} (skip_partial_buckets=True)")
                    return None
        
        # Check for duplicate timestamps
        for existing in bucket:
            if existing.open_ts == candle.open_ts:
                log.warning(f"Duplicate candle detected at {candle.timestamp_utc}, skipping")
                return None
        
        bucket.append(candle)
        
        log.debug(f"Added to bucket {key}: {len(bucket)}/{expected_count} candles")
        
        if len(bucket) in [1, expected_count // 2, expected_count - 1]:
            log.info(f"Bucket progress {key}: {len(bucket)}/{expected_count} candles")
        
        if len(bucket) == expected_count:
            aggregated = self.aggregate_candles(bucket)
            aggregated["bucket_info"] = {
                "date": date_str,
                "index": bucket_idx,
                "candle_count": len(bucket)
            }
            
            # Calculate moving averages
            if self.calculate_ma and self.ma_calculator:
                ma_data = self.ma_calculator.add_price(aggregated["close"])
                aggregated["ma_short"] = ma_data["ma_short"]
                aggregated["ma_long"] = ma_data["ma_long"]
                aggregated["ma_cross"] = ma_data["ma_cross"]
                aggregated["trend"] = ma_data["trend"]
                
                # Debug log for MA values - Fixed formatting for None values
                ma_short_str = f"{ma_data['ma_short']:.2f}" if ma_data['ma_short'] is not None else "N/A"
                ma_long_str = f"{ma_data['ma_long']:.2f}" if ma_data['ma_long'] is not None else "N/A"
                
                log.debug(
                    f"MA calculation - Close: {aggregated['close']:.2f} | "
                    f"MA{self.ma_calculator.short_period}: {ma_short_str} | "
                    f"MA{self.ma_calculator.long_period}: {ma_long_str} | "
                    f"Cross: {ma_data['ma_cross'] or 'None'}"
                )
            
            # Calculate ATR
            if self.calculate_atr and self.atr_calculator:
                atr_data = self.atr_calculator.add_candle(
                    aggregated["high"], 
                    aggregated["low"], 
                    aggregated["close"]
                )
                
                if atr_data:
                    aggregated["tr"] = atr_data.tr
                    aggregated["atr"] = atr_data.atr
                    aggregated["atr_percent"] = atr_data.atr_percent
                    
                    # Log ATR values
                    log.debug(
                        f"ATR calculation - TR: {atr_data.tr:.2f} | "
                        f"ATR({self.atr_calculator.period}): {atr_data.atr:.2f} | "
                        f"ATR%: {atr_data.atr_percent:.2f}%"
                    )
                else:
                    aggregated["tr"] = None
                    aggregated["atr"] = None
                    aggregated["atr_percent"] = None
            
            self.completed_buckets.append(aggregated)
            del self.buckets[key]
            
            if self.on_bucket_complete:
                self.on_bucket_complete(aggregated)
                
            return aggregated
            
        return None
    
    @staticmethod
    def aggregate_candles(candles: List[Candle]) -> dict:
        """Aggregate multiple candles into a single larger candle"""
        if not candles:
            raise ValueError("Cannot aggregate empty candle list")
        
        # Sort candles by timestamp to ensure correct order
        sorted_candles = sorted(candles, key=lambda c: c.open_ts)
        
        # Verify no gaps in the sequence
        for i in range(1, len(sorted_candles)):
            expected_ts = sorted_candles[i-1].open_ts + (3 * 60 * 1000)  # 3 minutes in ms
            if sorted_candles[i].open_ts != expected_ts:
                gap_minutes = (sorted_candles[i].open_ts - sorted_candles[i-1].open_ts) / (60 * 1000)
                log.warning(
                    f"Gap detected: {gap_minutes:.1f} minutes between candles {i-1} and {i} "
                    f"({sorted_candles[i-1].timestamp_utc:%H:%M:%S} -> {sorted_candles[i].timestamp_utc:%H:%M:%S})"
                )
        
        # Log details for debugging
        first_candle = sorted_candles[0]
        last_candle = sorted_candles[-1]
        
        log.debug(f"Aggregating {len(sorted_candles)} candles:")
        log.debug(f"  First: {first_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Open: {first_candle.open:.2f}")
        log.debug(f"  Last:  {last_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Close: {last_candle.close:.2f}")
        log.debug(f"  High:  {max(c.high for c in sorted_candles):.2f}")
        log.debug(f"  Low:   {min(c.low for c in sorted_candles):.2f}")
        
        # For very detailed debugging, log all candles
        if log.isEnabledFor(logging.DEBUG):
            for i, c in enumerate(sorted_candles):
                log.debug(f"    {i+1}. {c.timestamp_utc:%H:%M:%S} O:{c.open:.2f} H:{c.high:.2f} L:{c.low:.2f} C:{c.close:.2f} V:{c.volume:.2f}")
            
        return {
            "open_ts": first_candle.open_ts,
            "close_ts": last_candle.close_ts,
            "open": first_candle.open,  # First candle's open
            "high": max(c.high for c in sorted_candles),  # Highest high
            "low": min(c.low for c in sorted_candles),   # Lowest low
            "close": last_candle.close,  # Last candle's close
            "volume": sum(c.volume for c in sorted_candles),
            "trades": sum(c.trades for c in sorted_candles),
            "candle_count": len(sorted_candles),
            "timestamp_utc": first_candle.timestamp_utc.isoformat(),
            # Add detailed info for verification
            "first_candle_time": first_candle.timestamp_utc.strftime("%H:%M:%S"),
            "last_candle_time": last_candle.timestamp_utc.strftime("%H:%M:%S"),
        }
    
    def get_incomplete_buckets(self) -> Dict[str, List[dict]]:
        """Get all incomplete buckets for persistence"""
        result = {}
        for (date_str, bucket_idx), candles in self.buckets.items():
            key = f"{date_str}_bucket_{bucket_idx}"
            result[key] = [c.to_dict() for c in candles]
        return result
    
    def clear(self) -> None:
        """Clear all buckets"""
        self.buckets.clear()
        self.completed_buckets.clear()
        self.partial_bucket_warned.clear()

# ═══════════════════════════════════════════ Data Persistence ═══════════════════════════════════════

class DataPersistence:
    """Handles saving candle data to disk"""
    
    def __init__(self, output_dir: Path = Config.OUTPUT_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        
        self.json_dir = self.output_dir / "json"
        self.csv_dir = self.output_dir / "csv"
        self.json_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        
    def save_aggregated_candle(self, candle: dict) -> None:
        """Save a single aggregated candle to daily files"""
        date_str = candle["bucket_info"]["date"]
        
        json_file = self.json_dir / f"aggregated_{date_str}.json"
        self._append_json(json_file, candle)
        
        csv_file = self.csv_dir / f"aggregated_{date_str}.csv"
        self._append_csv(csv_file, candle)
        
    def _append_json(self, file_path: Path, data: dict) -> None:
        """Append data to JSON file (one object per line)"""
        with open(file_path, "a") as f:
            f.write(json.dumps(data) + "\n")
            
    def _append_csv(self, file_path: Path, candle: dict) -> None:
        """Append candle data to CSV file"""
        file_exists = file_path.exists()
        
        with open(file_path, "a", newline="") as f:
            fieldnames = [
                "timestamp_utc", "open_ts", "close_ts",
                "open", "high", "low", "close", "volume", "trades",
                "candle_count", "bucket_index", 
                "ma_short", "ma_long", "trend", "ma_cross",
                "tr", "atr", "atr_percent"  # ATR fields
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            
            if not file_exists:
                writer.writeheader()
                
            row = {k: candle.get(k, '') for k in fieldnames}  # Use empty string for None values
            row["bucket_index"] = candle["bucket_info"]["index"]
            # Ensure ma_cross is explicitly set
            row["ma_cross"] = candle.get("ma_cross", "") or ""  # Convert None to empty string
            writer.writerow(row)
            
    def save_state(self, aggregator: CandleAggregator, stats: StreamStats,
                   last_candle_ts: Optional[int] = None) -> None:
        """Save current state for recovery"""
        state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "last_candle_timestamp": last_candle_ts,
            "stats": {
                "messages_received": stats.messages_received,
                "candles_processed": stats.candles_processed,
                "historical_candles_loaded": stats.historical_candles_loaded,
                "buckets_completed": stats.buckets_completed,
                "gaps_filled": stats.gaps_filled,
                "errors_count": stats.errors_count,
                "reconnect_count": stats.reconnect_count,
            },
            "incomplete_buckets": aggregator.get_incomplete_buckets(),
            "completed_buckets_count": len(aggregator.completed_buckets),
        }
        
        state_file = self.output_dir / "state.json"
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)
            
        log.info(f"State saved to {state_file}")
    
    def load_state(self) -> Optional[Dict]:
        """Load saved state"""
        state_file = self.output_dir / "state.json"
        if not state_file.exists():
            return None
            
        try:
            with open(state_file) as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Failed to load state: {e}")
            return None

# ═══════════════════════════════════════════ Complete Client ═══════════════════════════════════════

class BingXCompleteClient:
    """Complete BingX client with history backfill and WebSocket streaming"""
    
    def __init__(
        self,
        symbol: str = Config.SYMBOL,
        interval: str = Config.INTERVAL,
        save_data: bool = True,
        backfill_days: Optional[int] = None
    ):
        self.symbol = symbol
        self.interval = interval
        self.save_data = save_data
        self.backfill_days = backfill_days or Config.HISTORY_DAYS
        
        # WebSocket subscription message
        self.subscribe_msg = {
            "id": f"{symbol}-{interval}",
            "reqType": "sub",
            "dataType": f"{symbol}@kline_{interval}",
        }
        
        # State management
        self.state = ConnectionState.DISCONNECTED
        self._shutdown = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        
        # Statistics
        self.stats = StreamStats()
        
        # Components
        self.aggregator = CandleAggregator(
            on_bucket_complete=self._on_bucket_complete,
            skip_partial_buckets=Config.SKIP_PARTIAL_BUCKETS,
            calculate_ma=Config.CALCULATE_MA,
            ma_short=Config.MA_SHORT_PERIOD,
            ma_long=Config.MA_LONG_PERIOD,
            calculate_atr=Config.CALCULATE_ATR,
            atr_period=Config.ATR_PERIOD
        )
        self.persistence = DataPersistence() if save_data else None
        self.backfiller = HistoryBackfiller()
        
        # Recent candles buffer
        self.recent_candles: Deque[Candle] = deque(maxlen=Config.MAX_MEMORY_CANDLES)
        
        # Track current candle for timestamp-based close detection
        self.current_candle_ts: Optional[int] = None
        self.last_candle_data: Optional[dict] = None
        self.last_processed_ts: Optional[int] = None
        
        # Tasks
        self._tasks: List[asyncio.Task] = []
        
        # Thread pool for sync operations
        self._executor = ThreadPoolExecutor(max_workers=2)
        
    async def run(self) -> None:
        """Main entry point - run the client with all features"""
        log.info(f"Starting BingX Complete Client for {self.symbol} {self.interval}")
        
        # Load previous state if available
        if self.persistence:
            state = self.persistence.load_state()
            if state:
                self.last_processed_ts = state.get("last_candle_timestamp")
                log.info(f"Loaded state: last candle at {self.last_processed_ts}")
        
        # Perform initial backfill
        if Config.BACKFILL_ON_START:
            await self._initial_backfill()
        
        # Start background tasks
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
        """Perform initial historical data backfill"""
        self.state = ConnectionState.BACKFILLING
        log.info(f"Starting historical backfill for {self.backfill_days} days")

        def process_chunk(candles: List[Dict]):
            log.info(f"Processing chunk of {len(candles)} historical candles")
            converted = [self.backfiller.convert_to_websocket_format(c) for c in candles]
            for candle_data in converted:
                self._process_historical_candle(candle_data)
            self.stats.historical_candles_loaded += len(candles)

        try:
            # Run backfill in thread pool
            loop = asyncio.get_event_loop()
            backfiller = HistoryBackfiller()
            backfiller.config.SYMBOL = self.symbol

            await loop.run_in_executor(
                self._executor,
                backfiller.backfill,
                self.backfill_days,
                process_chunk
            )
            
            log.info(
                f"Historical backfill complete: {self.stats.historical_candles_loaded} candles, "
                f"{self.stats.buckets_completed} buckets"
            )
                
        except Exception as e:
            log.error(f"Historical backfill failed: {e}", exc_info=True)
            
    def _process_historical_candle(self, kline: dict) -> None:
        """Process a historical candle (already marked as closed)"""
        # Create candle object
        candle = Candle(
            open_ts=kline["t"],
            close_ts=kline["T"],
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            trades=kline.get("n", 0),
        )
        
        # Update last processed timestamp
        self.last_processed_ts = candle.close_ts
        
        # Store in recent buffer
        self.recent_candles.append(candle)
        
        # Add to aggregator
        aggregated = self.aggregator.add_candle(candle)
        if aggregated:
            self.stats.buckets_completed += 1
            
    async def _connection_loop(self) -> None:
        """Main connection loop with exponential backoff"""
        delay = Config.INITIAL_RECONNECT_DELAY
        consecutive_errors = 0
        
        while not self._shutdown.is_set():
            try:
                self.state = ConnectionState.CONNECTING
                
                # Check for gaps and fill if needed
                if Config.FILL_GAPS_ON_RECONNECT and self.last_processed_ts:
                    await self._fill_gaps()
                
                # Connect and stream
                await self._connect_and_stream()
                
                # Reset counters on successful connection
                delay = Config.INITIAL_RECONNECT_DELAY
                consecutive_errors = 0
                
            except asyncio.CancelledError:
                raise
            except (websockets.exceptions.ConnectionClosedError, 
                    websockets.exceptions.WebSocketException,
                    asyncio.TimeoutError,
                    ConnectionError) as e:
                self.stats.errors_count += 1
                consecutive_errors += 1
                
                error_type = type(e).__name__
                log.warning(f"Connection error ({error_type}): {e}")
                
                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    
                    # If we get too many consecutive errors, increase delay more aggressively
                    if consecutive_errors > 5:
                        delay = min(delay * 2, Config.MAX_RECONNECT_DELAY)
                        log.warning(f"Multiple connection failures ({consecutive_errors}), increasing delay to {delay}s")
                    
                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    
                    # Exponential backoff
                    delay = min(delay * Config.RECONNECT_MULTIPLIER, Config.MAX_RECONNECT_DELAY)
                    self.stats.reconnect_count += 1
                    
            except Exception as e:
                self.stats.errors_count += 1
                log.error(f"Unexpected error: {e}", exc_info=True)
                
                if not self._shutdown.is_set():
                    self.state = ConnectionState.RECONNECTING
                    log.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                    
                    delay = min(delay * Config.RECONNECT_MULTIPLIER, Config.MAX_RECONNECT_DELAY)
                    self.stats.reconnect_count += 1
                    
    async def _fill_gaps(self) -> None:
        """Fill gaps between last processed candle and current time"""
        if not self.last_processed_ts:
            return

        gaps_found = False
        def process_chunk(candles: List[Dict]):
            nonlocal gaps_found
            if not candles:
                return
            gaps_found = True
            log.info(f"Filling gap with chunk of {len(candles)} candles")
            converted = [self.backfiller.convert_to_websocket_format(c) for c in candles]
            for candle_data in converted:
                self._process_historical_candle(candle_data)

        try:
            log.info("Checking for gaps in data...")
            
            # Run gap fill in thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self.backfiller.fill_gap,
                self.last_processed_ts,
                self.symbol,
                process_chunk
            )
            
            if gaps_found:
                self.stats.gaps_filled += 1
                log.info(f"Gap filled successfully")
            else:
                log.info("No gap detected")
                
        except Exception as e:
            log.error(f"Gap filling failed: {e}", exc_info=True)
            
    async def _connect_and_stream(self) -> None:
        """Establish WebSocket connection and stream data"""
        log.info(f"Connecting to {Config.WS_URL}")
        
        try:
            async with websockets.connect(
                Config.WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
                max_size=10 * 1024 * 1024,  # 10MB max message size
            ) as ws:
                self._ws = ws
                self.state = ConnectionState.CONNECTED
                self.stats.connected_at = datetime.now(timezone.utc)
                
                # Subscribe to kline stream
                await self._subscribe()
                
                # Main message loop
                await self._message_loop(ws)
                
        except websockets.exceptions.ConnectionClosedError as e:
            log.warning(f"WebSocket connection closed: {e}")
            raise
        except asyncio.TimeoutError:
            log.warning("WebSocket connection timed out")
            raise
        except Exception as e:
            log.error(f"WebSocket error: {e}")
            raise
            
    async def _subscribe(self) -> None:
        """Send subscription message"""
        self.state = ConnectionState.SUBSCRIBING
        log.info(f"Subscribing to {self.symbol} {self.interval} klines")
        
        await self._ws.send(json.dumps(self.subscribe_msg))
        self.state = ConnectionState.STREAMING
        
    async def _message_loop(self, ws) -> None:
        """Process incoming WebSocket messages"""
        try:
            async for message in ws:
                if self._shutdown.is_set():
                    break
                    
                self.stats.messages_received += 1
                
                try:
                    # Handle different message types
                    if isinstance(message, (bytes, bytearray)):
                        self.stats.bytes_received += len(message)
                        text = self._decompress_message(message)
                    else:
                        text = message
                        
                    # Handle ping/pong
                    if text == "Ping":
                        await ws.send("Pong")
                        continue
                        
                    # Parse JSON message
                    data = json.loads(text)
                    await self._process_message(data)
                    
                except json.JSONDecodeError as e:
                    log.warning(f"Failed to parse message as JSON: {e}")
                except Exception as e:
                    self.stats.errors_count += 1
                    log.error(f"Error processing message: {e}", exc_info=True)
                    
        except websockets.exceptions.ConnectionClosedError:
            # This is expected when connection closes, just propagate
            raise
        except Exception as e:
            log.error(f"Unexpected error in message loop: {e}", exc_info=True)
            raise
                
    def _decompress_message(self, data: bytes) -> str:
        """Decompress gzip-compressed message"""
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            return gz.read().decode("utf-8")
            
    async def _process_message(self, message: dict) -> None:
        """Process parsed WebSocket message"""
        data = message.get("data")
        if not isinstance(data, list):
            return
            
        for kline_data in data:
            try:
                self._process_kline(kline_data)
            except Exception as e:
                self.stats.errors_count += 1
                log.error(f"Error processing kline: {e}", exc_info=True)
                
    def _process_kline(self, kline: dict) -> None:
        """Process individual kline data - detect closes by timestamp changes"""
        current_ts = kline["T"]
        
        # If this is a new timestamp and we have previous candle data, the previous candle has closed
        if self.current_candle_ts is not None and current_ts > self.current_candle_ts and self.last_candle_data:
            # Process the closed candle
            candle = Candle(
                open_ts=self.last_candle_data["T"],
                close_ts=self.last_candle_data["T"] + 3 * 60 * 1000 - 1,  # 3 minutes minus 1ms
                open=float(self.last_candle_data["o"]),
                high=float(self.last_candle_data["h"]),
                low=float(self.last_candle_data["l"]),
                close=float(self.last_candle_data["c"]),
                volume=float(self.last_candle_data["v"]),
                trades=self.last_candle_data.get("n", 0),
            )
            
            # Update statistics
            self.stats.candles_processed += 1
            self.stats.last_candle_time = candle.timestamp_utc
            self.last_processed_ts = candle.close_ts
            
            # Log closed candle
            log.info(f"Closed candle: {candle}")
            
            # Store in recent buffer
            self.recent_candles.append(candle)
            
            # Add to aggregator
            aggregated = self.aggregator.add_candle(candle)
            if aggregated:
                self.stats.buckets_completed += 1
        
        # Update current candle tracking
        self.current_candle_ts = current_ts
        self.last_candle_data = kline.copy()
        
        # Show live updates periodically
        if self.stats.messages_received % 30 == 1:
            ts = datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc)
            log.debug(f"Live update: {ts:%H:%M:%S} UTC, Price: {float(kline['c']):.2f}")
            
    def _on_bucket_complete(self, aggregated: dict) -> None:
        """Handle completed bucket"""
        # Format display
        start = datetime.fromtimestamp(aggregated["open_ts"] / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp(aggregated["close_ts"] / 1000, tz=timezone.utc)
        
        # Basic candle info
        log_msg = (
            f"Bucket complete: {start:%Y-%m-%d %H:%M:%S}-{end:%H:%M:%S} | "
            f"O: {aggregated['open']:.2f} | H: {aggregated['high']:.2f} | "
            f"L: {aggregated['low']:.2f} | C: {aggregated['close']:.2f} | "
            f"V: {aggregated['volume']:.2f} | Candles: {aggregated['candle_count']} "
            f"({aggregated.get('first_candle_time', 'N/A')}-{aggregated.get('last_candle_time', 'N/A')})"
        )
        
        # Add MA info if available
        if "ma_short" in aggregated and aggregated["ma_short"]:
            ma_short_period = self.aggregator.ma_calculator.short_period if self.aggregator.ma_calculator else Config.MA_SHORT_PERIOD
            log_msg += f" | MA{ma_short_period}: {aggregated['ma_short']:.2f}"
        if "ma_long" in aggregated and aggregated["ma_long"]:
            ma_long_period = self.aggregator.ma_calculator.long_period if self.aggregator.ma_calculator else Config.MA_LONG_PERIOD
            log_msg += f" | MA{ma_long_period}: {aggregated['ma_long']:.2f}"
        if "trend" in aggregated and aggregated["trend"]:
            log_msg += f" | Trend: {aggregated['trend']}"
        if "ma_cross" in aggregated and aggregated["ma_cross"]:
            log_msg += f" | 🚨 {aggregated['ma_cross']}"
            
        # Add ATR info if available
        if "atr" in aggregated and aggregated["atr"]:
            atr_period = self.aggregator.atr_calculator.period if self.aggregator.atr_calculator else Config.ATR_PERIOD
            log_msg += f" | ATR({atr_period}): {aggregated['atr']:.2f} ({aggregated['atr_percent']:.2f}%)"
            
        log.info(log_msg)
        
        # Save to disk
        if self.persistence:
            try:
                self.persistence.save_aggregated_candle(aggregated)
            except Exception as e:
                log.error(f"Failed to save aggregated candle: {e}", exc_info=True)
                
    async def _stats_reporter(self) -> None:
        """Periodically report statistics"""
        while not self._shutdown.is_set():
            await asyncio.sleep(Config.STATS_INTERVAL_SECONDS)
            
            if self.state in [ConnectionState.STREAMING, ConnectionState.BACKFILLING]:
                log.info(f"Stats: {self.stats}")
                
    async def _auto_save_loop(self) -> None:
        """Periodically save state"""
        while not self._shutdown.is_set():
            await asyncio.sleep(Config.SAVE_INTERVAL_SECONDS)
            
            if self.persistence and (self.stats.candles_processed > 0 or self.stats.historical_candles_loaded > 0):
                try:
                    self.persistence.save_state(
                        self.aggregator,
                        self.stats,
                        self.last_processed_ts
                    )
                except Exception as e:
                    log.error(f"Failed to save state: {e}", exc_info=True)
                    
    async def _cleanup(self) -> None:
        """Clean up resources on shutdown"""
        log.info("Cleaning up resources...")
        
        # Cancel background tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                
        # Save final state
        if self.persistence:
            try:
                self.persistence.save_state(
                    self.aggregator,
                    self.stats,
                    self.last_processed_ts
                )
                log.info("Final state saved")
            except Exception as e:
                log.error(f"Failed to save final state: {e}")
                
        # Close WebSocket
        if self._ws and not self._ws.closed:
            await self._ws.close()
            
        # Shutdown executor
        self._executor.shutdown(wait=True)
            
        log.info("Cleanup complete")
        
    def stop(self) -> None:
        """Signal shutdown"""
        log.info("Shutdown signal received")
        self._shutdown.set()
        
    def get_recent_candles(self, count: int = 100) -> List[Candle]:
        """Get recent candles from buffer"""
        return list(self.recent_candles)[-count:]
    
    def get_aggregated_candles(self, count: int = 50) -> List[dict]:
        """Get recent aggregated candles"""
        return list(self.aggregator.completed_buckets)[-count:]
    
    def get_current_ma_values(self) -> Dict[str, Optional[float]]:
        """Get current moving average values and trend"""
        if not self.aggregator.calculate_ma or not self.aggregator.ma_calculator:
            return {"ma_short": None, "ma_long": None, "trend": None}
            
        return {
            "ma_short": self.aggregator.ma_calculator.calculate_ma(Config.MA_SHORT_PERIOD),
            "ma_long": self.aggregator.ma_calculator.calculate_ma(Config.MA_LONG_PERIOD),
            "trend": self.aggregator.ma_calculator.get_trend()
        }
        
    def get_current_atr_value(self) -> Dict[str, Optional[float]]:
        """Get current ATR value"""
        if not self.aggregator.calculate_atr or not self.aggregator.atr_calculator:
            return {"atr": None, "atr_percent": None}
            
        atr = self.aggregator.atr_calculator.get_current_atr()
        if atr and self.recent_candles:
            last_close = self.recent_candles[-1].close
            atr_percent = (atr / last_close) * 100 if last_close > 0 else 0
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
        print(f"║ MA Periods:   Short: {config.MA_SHORT_PERIOD}, Long: {config.MA_LONG_PERIOD:<35} ║")
    if config.CALCULATE_ATR:
        print(f"║ ATR Period:   {config.ATR_PERIOD:<46} ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║ Features:                                                    ║")
    print("║ • Historical backfill on startup                             ║")
    print("║ • Automatic gap detection and filling                        ║")
    print("║ • State persistence and recovery                             ║")
    print("║ • Real-time aggregation to 21/12 minute candles             ║")
    if config.CALCULATE_MA:
        print("║ • Moving average calculations with crossover detection       ║")
    if config.CALCULATE_ATR:
        print("║ • ATR calculation for volatility measurement                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

# ═══════════════════════════════════════════ Main Entry Point ═══════════════════════════════════════

@profile
async def main():
    """Main application entry point"""
    # Print banner
    print_banner()
    
    # Create and configure client - use Config values
    client = BingXCompleteClient(
        symbol=Config.SYMBOL,
        interval=Config.INTERVAL,
        save_data=True,
        backfill_days=Config.HISTORY_DAYS
    )
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, client.stop)
        
    # Run client
    try:
        await client.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        
    log.info("Application terminated")

# ═══════════════════════════════════════════ CLI Interface ═══════════════════════════════════════════

def create_cli():
    """Create command-line interface"""
    parser = argparse.ArgumentParser(
        description="BingX Complete Streamer with Historical Backfill and ATR"
    )
    
    parser.add_argument(
        "--symbol",
        default="BTC-USDT",
        help="Trading symbol (default: BTC-USDT)"
    )
    parser.add_argument(
        "--interval",
        default="3m",
        choices=["1m", "3m", "5m", "15m", "30m", "1h"],
        help="Candle interval (default: 3m)"
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=3,
        help="Days of history to backfill (default: 3)"
    )
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="Skip initial historical backfill"
    )
    parser.add_argument(
        "--no-gap-fill",
        action="store_true",
        help="Disable automatic gap filling on reconnect"
    )
    parser.add_argument(
        "--skip-partial",
        action="store_true",
        help="Skip partial buckets when starting mid-day"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("bingx_data"),
        help="Output directory for data files"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--verify-bucket",
        type=str,
        help="Verify a specific bucket (format: YYYY-MM-DD HH:MM)"
    )
    parser.add_argument(
        "--show-alignment",
        action="store_true",
        help="Show TradingView alignment guide"
    )
    parser.add_argument(
        "--ma-short",
        type=int,
        default=3,  # Changed from 9 to 3
        help="Short moving average period (default: 3)"
    )
    parser.add_argument(
        "--ma-long",
        type=int,
        default=7,  # Changed from 21 to 7
        help="Long moving average period (default: 7)"
    )
    parser.add_argument(
        "--no-ma",
        action="store_true",
        help="Disable moving average calculations"
    )
    parser.add_argument(
        "--atr-period",
        type=int,
        default=14,
        help="ATR period (default: 14)"
    )
    parser.add_argument(
        "--no-atr",
        action="store_true",
        help="Disable ATR calculations"
    )
    
    return parser

def verify_bucket_alignment(symbol: str, bucket_time: str):
    """Verify bucket alignment with TradingView"""
    try:
        dt = datetime.strptime(bucket_time, "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=timezone.utc)
        
        # Calculate bucket boundaries
        minute_of_day = dt.hour * 60 + dt.minute
        if minute_of_day >= Config.FINAL_BUCKET_START_HOUR * 60 + Config.FINAL_BUCKET_START_MINUTE:
            bucket_minutes = Config.FINAL_BUCKET_MINUTES
        else:
            bucket_minutes = Config.STANDARD_BUCKET_MINUTES
            
        # Find bucket start
        bucket_start_minute = (minute_of_day // bucket_minutes) * bucket_minutes
        bucket_start = dt.replace(hour=bucket_start_minute // 60, minute=bucket_start_minute % 60, second=0, microsecond=0)
        bucket_end = bucket_start + timedelta(minutes=bucket_minutes - 1, seconds=59)
        
        print(f"\n📊 Bucket Analysis for {symbol} at {bucket_time}")
        print(f"{'='*60}")
        print(f"Bucket period: {bucket_start:%Y-%m-%d %H:%M:%S} - {bucket_end:%H:%M:%S} UTC")
        print(f"Duration: {bucket_minutes} minutes")
        print(f"Expected 3-minute candles: {bucket_minutes // 3}")
        print(f"\n3-minute candles in this bucket:")
        
        for i in range(bucket_minutes // 3):
            candle_start = bucket_start + timedelta(minutes=i * 3)
            candle_end = candle_start + timedelta(minutes=2, seconds=59)
            print(f"  {i+1}. {candle_start:%H:%M:%S} - {candle_end:%H:%M:%S}")
            
        print(f"\n💡 To verify in TradingView:")
        print(f"1. Set chart to {symbol}")
        print(f"2. Set timeframe to {bucket_minutes} minutes")
        print(f"3. Find candle starting at {bucket_start:%Y-%m-%d %H:%M} UTC")
        print(f"4. Compare OHLC values with aggregated output")
        
    except ValueError as e:
        print(f"❌ Error: Invalid date format. Use YYYY-MM-DD HH:MM")


def verify_tradingview_alignment():
    """Print TradingView alignment information"""
    print("\n📊 TradingView Alignment Guide")
    print("=" * 80)
    print("\n21-Minute Buckets (Standard):")
    print("Each bucket contains 7 consecutive 3-minute candles")
    print("\nBucket | Start Time | End Time   | 3-min Candles Included")
    print("-" * 60)
    
    for hour in range(24):
        for minute in [0, 21, 42]:
            if hour == 23 and minute > 42:
                break
            start_time = f"{hour:02d}:{minute:02d}:00"
            end_minute = minute + 20
            end_hour = hour
            if end_minute >= 60:
                end_minute -= 60
                end_hour += 1
            end_time = f"{end_hour:02d}:{end_minute:02d}:59"
            
            candles = []
            for i in range(7):
                c_min = minute + (i * 3)
                c_hour = hour
                if c_min >= 60:
                    c_min -= 60
                    c_hour += 1
                candles.append(f"{c_hour:02d}:{c_min:02d}")
            
            print(f"  {hour * 3 + minute // 21:<3d}  | {start_time} | {end_time} | {', '.join(candles)}")
    
    print("\n12-Minute Bucket (Final):")
    print("  68   | 23:48:00 | 23:59:59 | 23:48, 23:51, 23:54, 23:57")
    
    print("\n💡 Important Notes:")
    print("1. All times are in UTC")
    print("2. Open price = Open of FIRST 3-min candle (e.g., 14:21:00)")
    print("3. Close price = Close of LAST 3-min candle (e.g., 14:39:59)")
    print("4. High = Highest high of all 7 candles")
    print("5. Low = Lowest low of all 7 candles")
    print("6. ATR = Average True Range of aggregated candles")
    print("\n⚠️  Common Issues:")
    print("• TradingView might show exchange time (not UTC)")
    print("• Ensure you're looking at BingX data on TradingView")
    print("• Check for missing candles in the data feed")
    print("• ATR values may differ slightly due to initialization differences")
    
async def main_cli():
    """CLI entry point"""
    parser = create_cli()
    args = parser.parse_args()
    
    # Handle alignment guide
    if args.show_alignment:
        verify_tradingview_alignment()
        return
        
    # Handle bucket verification
    if args.verify_bucket:
        verify_bucket_alignment(args.symbol, args.verify_bucket)
        return
    
    # Update configuration
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
    
    if args.debug:
        Config.LOG_LEVEL = logging.DEBUG
        
    # Reinitialize logger with new settings
    global log
    log = setup_logging(Config.LOG_LEVEL)
    
    # Run main
    await main()

if __name__ == "__main__":
    # Create output directory
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Check if running with CLI arguments
    if len(sys.argv) > 1:
        import sys
        asyncio.run(main_cli())
    else:
        asyncio.run(main())