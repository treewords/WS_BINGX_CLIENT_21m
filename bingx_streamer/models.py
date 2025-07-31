from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, computed_field


class ConnectionState(Enum):
    """WebSocket connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    SUBSCRIBING = "subscribing"
    STREAMING = "streaming"
    RECONNECTING = "reconnecting"
    BACKFILLING = "backfilling"


class Candle(BaseModel):
    """Individual kline/candle data"""
    open_ts: int
    close_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int = 0

    @computed_field
    @property
    def duration_minutes(self) -> float:
        """Calculate candle duration in minutes"""
        return (self.close_ts - self.open_ts) / (1000 * 60)

    @computed_field
    @property
    def timestamp_utc(self) -> datetime:
        """Get open timestamp as UTC datetime"""
        return datetime.fromtimestamp(self.open_ts / 1000, tz=timezone.utc)

    def __str__(self) -> str:
        ts: datetime = self.timestamp_utc
        return (f"{ts:%Y-%m-%d %H:%M:%S} | "
                f"O: {self.open:.2f} | H: {self.high:.2f} | "
                f"L: {self.low:.2f} | C: {self.close:.2f} | "
                f"V: {self.volume:.2f}")


class StreamStats(BaseModel):
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

    def __str__(self) -> str:
        uptime: timedelta = timedelta(seconds=int(self.uptime_seconds()))
        error_summary = (f"Errors(T/W/A/P/D): {self.total_errors}/{self.ws_errors}/"
                         f"{self.api_errors}/{self.processing_errors}/{self.disconnections}")
        return (f"Uptime: {uptime} | Msgs: {self.messages_received} | "
                f"Candles: {self.candles_processed} (Hist: {self.historical_candles_loaded}) | "
                f"Buckets: {self.completed_buckets} | Gaps: {self.gaps_filled} | "
                f"Reconnects: {self.reconnect_count} | {error_summary}")


class ATRData(BaseModel):
    """ATR calculation result"""
    tr: float
    atr: float
    atr_percent: float
