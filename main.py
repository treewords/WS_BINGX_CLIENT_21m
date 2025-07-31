import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from bingx_streamer.client import BingXCompleteClient
from bingx_streamer.config import Config
from bingx_streamer.utils import setup_logging

log: Optional[logging.Logger] = None


def print_banner(config: Config) -> None:
    """Print startup banner"""
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║        BingX Complete Streamer with History v3.0             ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║ Symbol:       {config.SYMBOL:<46} ║")
    print(f"║ Interval:     {config.INTERVAL:<46} ║")
    print(f"║ History Days: {config.HISTORY_DAYS:<46} ║")
    print(f"║ Output:       {str(config.OUTPUT_DIR):<46} ║")
    if config.CALCULATE_MA:
        print(
            f"║ MA Periods:   Short: {config.MA_SHORT_PERIOD}, Long: {config.MA_LONG_PERIOD:<29} ║"
        )
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


def create_cli() -> argparse.ArgumentParser:
    """Create command-line interface"""
    parser = argparse.ArgumentParser(
        description="BingX Complete Streamer with Historical Backfill and ATR"
    )

    parser.add_argument(
        "--symbol", default="BTC-USDT", help="Trading symbol (default: BTC-USDT)"
    )
    parser.add_argument(
        "--interval",
        default="3m",
        choices=["1m", "3m", "5m", "15m", "30m", "1h"],
        help="Candle interval (default: 3m)",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=3,
        help="Days of history to backfill (default: 3)",
    )
    parser.add_argument(
        "--no-backfill", action="store_true", help="Skip initial historical backfill"
    )
    parser.add_argument(
        "--no-gap-fill",
        action="store_true",
        help="Disable automatic gap filling on reconnect",
    )
    parser.add_argument(
        "--skip-partial",
        action="store_true",
        help="Skip partial buckets when starting mid-day",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("bingx_data"),
        help="Output directory for data files",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--verify-bucket",
        type=str,
        help="Verify a specific bucket (format: YYYY-MM-DD HH:MM)",
    )
    parser.add_argument(
        "--show-alignment", action="store_true", help="Show TradingView alignment guide"
    )
    parser.add_argument(
        "--ma-short",
        type=int,
        default=3,
        help="Short moving average period (default: 3)",
    )
    parser.add_argument(
        "--ma-long", type=int, default=7, help="Long moving average period (default: 7)"
    )
    parser.add_argument(
        "--no-ma", action="store_true", help="Disable moving average calculations"
    )
    parser.add_argument(
        "--atr-period", type=int, default=14, help="ATR period (default: 14)"
    )
    parser.add_argument(
        "--no-atr", action="store_true", help="Disable ATR calculations"
    )
    parser.add_argument(
        "--memory-mapped",
        action="store_true",
        help="Enable memory-mapped files for large datasets",
    )

    return parser


def verify_bucket_alignment(symbol: str, bucket_time: str) -> None:
    """Verify bucket alignment with TradingView"""
    try:
        dt: datetime = datetime.strptime(bucket_time, "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )

        minute_of_day: int = dt.hour * 60 + dt.minute
        bucket_minutes: int
        if (
            minute_of_day
            >= Config.FINAL_BUCKET_START_HOUR * 60 + Config.FINAL_BUCKET_START_MINUTE
        ):
            bucket_minutes = Config.FINAL_BUCKET_MINUTES
        else:
            bucket_minutes = Config.STANDARD_BUCKET_MINUTES

        bucket_start_minute: int = (minute_of_day // bucket_minutes) * bucket_minutes
        bucket_start: datetime = dt.replace(
            hour=bucket_start_minute // 60,
            minute=bucket_start_minute % 60,
            second=0,
            microsecond=0,
        )
        bucket_end: datetime = bucket_start + timedelta(
            minutes=bucket_minutes - 1, seconds=59
        )

        print(f"\n📊 Bucket Analysis for {symbol} at {bucket_time}")
        print(f"{'='*60}")
        print(
            f"Bucket period: {bucket_start:%Y-%m-%d %H:%M:%S} - {bucket_end:%Y-%m-%d %H:%M:%S} UTC"
        )
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
            if hour == 23 and minute > 42:
                continue
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

            print(
                f"  {hour * 3 + minute // 21:<3d}  | {start_time} | {end_time} | {', '.join(candles)}"
            )

    print("\n12-Minute Bucket (Final):")
    print("  68   | 23:48:00 | 23:59:59 | 23:48, 23:51, 23:54, 23:57")
    print("\n💡 Important Notes:")
    print("1. All times are in UTC")
    print("2. Open price = Open of FIRST 3-min candle")
    print("3. Close price = Close of LAST 3-min candle")
    print("4. High = Highest high of all candles in bucket")
    print("5. Low = Lowest low of all candles in bucket")


async def main(config: Config) -> None:
    """Main application entry point"""
    print_banner(config)
    client: BingXCompleteClient = BingXCompleteClient(config=config, save_data=True)

    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, client.stop)

    try:
        await client.run()
    except KeyboardInterrupt:
        if log:
            log.info("Interrupted by user")
    except Exception as e:
        if log:
            log.error(f"Unexpected error in main: {e}", exc_info=True)
        if "client" in locals() and client:
            client.stats.total_errors += 1
    finally:
        if log and "client" in locals() and client:
            log.info(f"Final Stats: {client.stats}")
        if log:
            log.info("Application terminated")


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

    config = Config()
    config.SYMBOL = args.symbol
    config.INTERVAL = args.interval
    config.HISTORY_DAYS = args.history_days
    config.BACKFILL_ON_START = not args.no_backfill
    config.FILL_GAPS_ON_RECONNECT = not args.no_gap_fill
    config.SKIP_PARTIAL_BUCKETS = args.skip_partial
    config.OUTPUT_DIR = args.output
    config.MA_SHORT_PERIOD = args.ma_short
    config.MA_LONG_PERIOD = args.ma_long
    config.CALCULATE_MA = not args.no_ma
    config.ATR_PERIOD = args.atr_period
    config.CALCULATE_ATR = not args.no_atr
    config.USE_MEMORY_MAPPED_FILES = args.memory_mapped

    if args.debug:
        config.LOG_LEVEL = logging.DEBUG

    global log
    log = setup_logging(config.LOG_LEVEL)

    await main(config)


if __name__ == "__main__":
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    asyncio.run(main_cli())
