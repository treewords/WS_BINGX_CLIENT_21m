import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import Config

log = logging.getLogger("bingx.backfiller")


class NetworkError(Exception):
    """Network-related errors"""

    pass


class HistoryBackfiller:
    """Handles historical data fetching and gap filling"""

    def __init__(self, config: Config) -> None:
        self.config: Config = config
        self.session: requests.Session = requests.Session()

    @property
    def rest_url(self) -> str:
        return f"{self.config.REST_BASE_URL}{self.config.REST_ENDPOINT}"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(NetworkError),
    )
    def fetch_history_chunk(
        self, start_time: datetime, end_time: datetime, symbol: Optional[str] = None
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
            log.debug(
                f"Fetching {symbol} from {start_time:%Y-%m-%d %H:%M} "
                f"to {end_time:%Y-%m-%d %H:%M}"
            )
            log.debug("Making request to REST API")
            response: requests.Response = self.session.get(
                self.rest_url, params=params, timeout=30
            )
            log.debug("Request complete")
            response.raise_for_status()

            data: Dict[str, Any] = response.json()
            if data.get("code") != 0:
                raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

            candles: List[Dict[str, Any]] = data.get("data", [])
            log.info(
                f"Fetched {len(candles)} candles for {symbol} "
                f"({start_time:%H:%M} - {end_time:%H:%M})"
            )
            return sorted(candles, key=lambda x: int(x["time"]))

        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Network error: {e}") from e
        except Exception:
            # Catch any other unexpected errors during API call
            log.error(
                "An unexpected error occurred during history fetch.", exc_info=True
            )
            raise

    def stream_history(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Optional[str] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> None:
        """Stream historical candles chunk by chunk and process them"""
        symbol = symbol or self.config.SYMBOL
        current_start: datetime = start_time
        total_fetched: int = 0
        log.info("In stream_history")
        while current_start < end_time:
            chunk_end: datetime = min(current_start + timedelta(hours=50), end_time)
            log.info(f"Fetching chunk from {current_start} to {chunk_end}")
            candles: List[Dict[str, Any]] = self.fetch_history_chunk(
                current_start, chunk_end, symbol
            )
            log.info(f"Fetched {len(candles)} candles")
            if candles:
                total_fetched += len(candles)
                if process_chunk:
                    process_chunk(candles)

            # Move to next chunk
            current_start = chunk_end

            # Small delay to avoid rate limits
            if current_start < end_time:
                time.sleep(0.5)

        log.info(f"Total fetched: {total_fetched} candles for {symbol}")

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

    def backfill(
        self,
        days: Optional[int] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> None:
        """Perform initial backfill by streaming chunks"""
        days = days or self.config.HISTORY_DAYS
        end_time: datetime = datetime.now(timezone.utc)
        start_time: datetime = end_time - timedelta(days=days)

        log.info(f"Starting backfill for {days} days of {self.config.SYMBOL}")
        log.info("In backfill method")
        try:
            log.info("Calling stream_history")
            self.stream_history(start_time, end_time, process_chunk=process_chunk)
            log.info("stream_history finished")
        except Exception as e:
            log.error(f"Backfill failed: {e}")

    def fill_gap(
        self,
        last_timestamp: int,
        symbol: Optional[str] = None,
        process_chunk: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> None:
        """Fill gap by streaming chunks"""
        symbol = symbol or self.config.SYMBOL
        start_time: datetime = datetime.fromtimestamp(
            last_timestamp / 1000, tz=timezone.utc
        )
        end_time: datetime = datetime.now(timezone.utc)

        gap_minutes: float = (end_time - start_time).total_seconds() / 60

        if gap_minutes < 3:  # Less than one candle
            return

        log.info(
            f"Filling gap for {symbol}: {gap_minutes:.1f} minutes "
            f"from {start_time:%Y-%m-%d %H:%M}"
        )

        try:
            self.stream_history(start_time, end_time, symbol, process_chunk)
        except Exception as e:
            log.error(f"Gap fill failed: {e}")
