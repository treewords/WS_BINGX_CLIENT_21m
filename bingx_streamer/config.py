import logging
from pathlib import Path


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
