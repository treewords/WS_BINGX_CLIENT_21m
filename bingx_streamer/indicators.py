import logging
import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from .models import ATRData

log = logging.getLogger("bingx.indicators")


class MovingAverageCalculator:
    """Calculate moving averages for aggregated candles"""

    def __init__(self, short_period: int = 9, long_period: int = 21) -> None:
        self.short_period: int = short_period
        self.long_period: int = long_period
        self.price_history: Deque[float] = deque(
            maxlen=max(short_period, long_period) + 1
        )
        self.prev_short_ma: Optional[float] = None
        self.prev_long_ma: Optional[float] = None
        self._lock: threading.Lock = threading.Lock()

    def add_price(self, close_price: float) -> Dict[str, Optional[Any]]:
        """Add a new close price and calculate MAs"""
        with self._lock:
            # Add new price
            self.price_history.append(close_price)

            # Calculate new MAs
            ma_short: Optional[float] = self._calculate_ma_unsafe(self.short_period)
            ma_long: Optional[float] = self._calculate_ma_unsafe(self.long_period)

            # Detect crossover using stored previous values
            cross_signal: Optional[str] = None
            if (
                self.prev_short_ma is not None
                and self.prev_long_ma is not None
                and ma_short is not None
                and ma_long is not None
            ):

                tolerance: float = 0.01

                # Previous: short below long, Current: short above long = Golden Cross
                if (
                    self.prev_short_ma <= self.prev_long_ma + tolerance
                    and ma_short > ma_long + tolerance
                ):
                    cross_signal = "GOLDEN_CROSS"
                    log.debug(
                        f"📊 MA Crossover detected: GOLDEN CROSS | "
                        f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                        f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}"
                    )

                # Previous: short above long, Current: short below long = Death Cross
                elif (
                    self.prev_short_ma >= self.prev_long_ma - tolerance
                    and ma_short < ma_long - tolerance
                ):
                    cross_signal = "DEATH_CROSS"
                    log.debug(
                        f"📊 MA Crossover detected: DEATH CROSS | "
                        f"Previous: MA{self.short_period}={self.prev_short_ma:.2f} MA{self.long_period}={self.prev_long_ma:.2f} | "
                        f"Current: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f}"
                    )

            # Update previous values for next iteration
            self.prev_short_ma = ma_short
            self.prev_long_ma = ma_long

            # Get trend
            trend: Optional[str] = self._get_trend_unsafe()

            # Log MA values periodically for debugging
            if (
                ma_short is not None
                and ma_long is not None
                and len(self.price_history) % 5 == 0
            ):
                log.debug(
                    f"MA Update: MA{self.short_period}={ma_short:.2f} MA{self.long_period}={ma_long:.2f} "
                    f"Diff={(ma_short - ma_long):.2f} Trend={trend}"
                )

            return {
                "ma_short": ma_short,
                "ma_long": ma_long,
                "ma_cross": cross_signal,
                "trend": trend,
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
                    abs(low - self.prev_close),
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
                return ATRData(tr=tr, atr=atr_value, atr_percent=atr_percent)
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
