import logging
from datetime import datetime
from pathlib import Path

from .config import Config


def setup_logging(level: int) -> logging.Logger:
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
        encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter(Config.LOG_FORMAT, Config.LOG_DATE_FORMAT)
    )
    logger.addHandler(file_handler)

    return logger
