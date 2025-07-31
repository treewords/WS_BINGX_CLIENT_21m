import logging
import threading
from collections import deque
from datetime import datetime
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from .config import Config
from .indicators import ATRCalculator, MovingAverageCalculator
from .models import ATRData, Candle

log = logging.getLogger("bingx.aggregator")


class CandleAggregator:
    """Handles candle bucketing and aggregation logic"""

    def __init__(
        self,
        config: Config,
        on_bucket_complete: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.config = config
        self.buckets: Dict[Tuple[str, int], List[Candle]] = {}
        self.on_bucket_complete: Optional[
            Callable[[Dict[str, Any]], None]
        ] = on_bucket_complete
        self.completed_buckets: Deque[Dict[str, Any]] = deque(maxlen=200)
        self.partial_bucket_warned: Set[Tuple[str, int]] = set()

        self.calculate_ma: bool = self.config.CALCULATE_MA
        self.ma_calculator: Optional[MovingAverageCalculator] = (
            MovingAverageCalculator(
                self.config.MA_SHORT_PERIOD, self.config.MA_LONG_PERIOD
            )
            if self.calculate_ma
            else None
        )

        self.calculate_atr: bool = self.config.CALCULATE_ATR
        self.atr_calculator: Optional[ATRCalculator] = (
            ATRCalculator(self.config.ATR_PERIOD) if self.calculate_atr else None
        )
        self._lock: threading.Lock = threading.Lock()

    def get_bucket_info(self, dt: datetime) -> Tuple[int, int]:
        """
        Determine bucket index and expected candle count for a given time.
        Returns: (bucket_index, expected_candle_count)
        """
        minute_of_day: int = dt.hour * 60 + dt.minute
        final_start: int = (
            self.config.FINAL_BUCKET_START_HOUR * 60
            + self.config.FINAL_BUCKET_START_MINUTE
        )

        if minute_of_day >= final_start:
            bucket_idx: int = 68
            expected_count: int = self.config.FINAL_BUCKET_MINUTES // 3
        else:
            bucket_idx = minute_of_day // self.config.STANDARD_BUCKET_MINUTES
            expected_count = self.config.STANDARD_BUCKET_MINUTES // 3

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
                log.warning(
                    f"Duplicate candle detected at {candle.timestamp_utc}, skipping"
                )
                return None

            bucket.append(candle)

            log.debug(f"Added to bucket {key}: {len(bucket)}/{expected_count} candles")

            if len(bucket) in [1, expected_count // 2, expected_count - 1]:
                log.debug(
                    f"Bucket progress {key}: {len(bucket)}/{expected_count} candles"
                )

            if len(bucket) == expected_count:
                aggregated: Dict[str, Any] = self.aggregate_candles(bucket)
                aggregated["bucket_info"] = {
                    "date": date_str,
                    "index": bucket_idx,
                    "candle_count": len(bucket),
                }

                self.completed_buckets.append(aggregated)
                del self.buckets[key]

                # Perform calculations outside the main lock if possible
                if self.calculate_ma and self.ma_calculator:
                    ma_data: Dict[
                        str, Optional[Any]
                    ] = self.ma_calculator.add_price(aggregated["close"])
                    aggregated.update(ma_data)

                if self.calculate_atr and self.atr_calculator:
                    atr_data: Optional[ATRData] = self.atr_calculator.add_candle(
                        aggregated["high"], aggregated["low"], aggregated["close"]
                    )
                    if atr_data:
                        aggregated.update(atr_data.model_dump())

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
            expected_ts: int = sorted_candles[i - 1].open_ts + (3 * 60 * 1000)
            if sorted_candles[i].open_ts != expected_ts:
                gap_minutes: float = (
                    sorted_candles[i].open_ts - sorted_candles[i - 1].open_ts
                ) / (60 * 1000)
                log.warning(
                    f"Gap detected: {gap_minutes:.1f} minutes between candles {i-1} and {i} "
                    f"({sorted_candles[i-1].timestamp_utc:%H:%M:%S} -> {sorted_candles[i].timestamp_utc:%H:%M:%S})"
                )

        first_candle: Candle = sorted_candles[0]
        last_candle: Candle = sorted_candles[-1]

        log.debug(f"Aggregating {len(sorted_candles)} candles:")
        log.debug(
            f"  First: {first_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Open: {first_candle.open:.2f}"
        )
        log.debug(
            f"  Last:  {last_candle.timestamp_utc:%Y-%m-%d %H:%M:%S} - Close: {last_candle.close:.2f}"
        )
        log.debug(f"  High:  {max(c.high for c in sorted_candles):.2f}")
        log.debug(f"  Low:   {min(c.low for c in sorted_candles):.2f}")

        if log.isEnabledFor(logging.DEBUG):
            for i, c in enumerate(sorted_candles):
                log.debug(
                    f"    {i+1}. {c.timestamp_utc:%H:%M:%S} O:{c.open:.2f} H:{c.high:.2f} L:{c.low:.2f} C:{c.close:.2f} V:{c.volume:.2f}"
                )

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
                result[key] = [c.model_dump() for c in candles]
            return result

    def clear(self) -> None:
        """Clear all buckets"""
        with self._lock:
            self.buckets.clear()
            self.completed_buckets.clear()
            self.partial_bucket_warned.clear()
