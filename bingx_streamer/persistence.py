import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .models import StreamStats

if TYPE_CHECKING:
    from .aggregator import CandleAggregator


log = logging.getLogger("bingx.persistence")


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
            log.info(
                f"Loaded Parquet dataset with {len(self.dataset.fragments)} fragments."
            )
            self.table = self.dataset.read(use_threads=True, use_pandas_metadata=False)
            log.info(
                "Memory-mapping enabled."
                if self.use_memory_map
                else "Dataset loaded into memory."
            )
        except Exception as e:
            log.error(f"Error loading Parquet dataset: {e}", exc_info=True)

    def get_full_data(self) -> Optional[pd.DataFrame]:
        """Return the entire dataset as a Pandas DataFrame"""
        if self.table is None:
            return None
        return self.table.to_pandas()

    def get_date_range(self) -> Optional[Tuple[datetime, datetime]]:
        """Get the min and max date from the dataset partitions"""
        if not self.dataset or not self.dataset.partitions:
            return None

        dates: List[datetime] = [
            datetime.strptime(part.partition_string.split("=")[1], "%Y-%m-%d")
            for part in self.dataset.partitions
        ]
        return min(dates), max(dates)


class DataPersistence:
    """Handles saving candle data to disk using Parquet format"""

    def __init__(self, config: Config, buffer_size: int = 100) -> None:
        log.info("Initializing DataPersistence")
        self.config = config
        self.output_dir: Path = self.config.OUTPUT_DIR
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
            df["date"] = df["bucket_info"].apply(lambda x: x["date"])
            df["bucket_index"] = df["bucket_info"].apply(lambda x: x["index"])

            schema: pa.Schema = pa.schema(
                [
                    pa.field("open_ts", pa.int64()),
                    pa.field("close_ts", pa.int64()),
                    pa.field("open", pa.float64()),
                    pa.field("high", pa.float64()),
                    pa.field("low", pa.float64()),
                    pa.field("close", pa.float64()),
                    pa.field("volume", pa.float64()),
                    pa.field("trades", pa.int64()),
                    pa.field("candle_count", pa.int32()),
                    pa.field("timestamp_utc", pa.string()),
                    pa.field("first_candle_time", pa.string()),
                    pa.field("last_candle_time", pa.string()),
                    pa.field(
                        "bucket_info",
                        pa.struct(
                            [
                                pa.field("date", pa.string()),
                                pa.field("index", pa.int32()),
                                pa.field("candle_count", pa.int32()),
                            ]
                        ),
                    ),
                    pa.field("ma_short", pa.float64()),
                    pa.field("ma_long", pa.float64()),
                    pa.field("ma_cross", pa.string()),
                    pa.field("trend", pa.string()),
                    pa.field("tr", pa.float64()),
                    pa.field("atr", pa.float64()),
                    pa.field("atr_percent", pa.float64()),
                    pa.field("date", pa.string()),
                    pa.field("bucket_index", pa.int32()),
                ]
            )

            table: pa.Table = pa.Table.from_pandas(
                df, schema=schema, preserve_index=False
            )

            pq.write_to_dataset(
                table,
                root_path=self.parquet_dir,
                partition_cols=["date"],
                existing_data_behavior="overwrite_or_ignore",
            )
            log.info(f"Flushed {len(df)} records to Parquet dataset")

        except Exception as e:
            log.error(f"Failed to flush buffer to Parquet: {e}", exc_info=True)
            with self._lock:
                self.buffer.extend(df.to_dict("records"))

    def save_state(
        self,
        aggregator: "CandleAggregator",
        stats: StreamStats,
        last_candle_ts: Optional[int] = None,
    ) -> None:
        """Save current state for recovery"""
        with self._lock:
            state: Dict[str, Any] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "last_candle_timestamp": last_candle_ts,
                "stats": stats.model_dump(),
                "incomplete_buckets": aggregator.get_incomplete_buckets(),
                "completed_buckets_count": len(aggregator.completed_buckets),
                "persistence_buffer": self.buffer,
            }

        state_file: Path = self.output_dir / "state.json"
        try:
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            log.info(f"State saved to {state_file}")
        except (TypeError, OverflowError, IOError) as e:
            log.error(f"Error saving state to JSON: {e}")
            # Fallback for serialization errors
            try:
                with open(state_file.with_suffix(".log"), "w", encoding="utf-8") as f:
                    f.write(str(state))
                log.warning(
                    f"State saved to fallback log file: {state_file.with_suffix('.log')}"
                )
            except Exception as log_e:
                log.error(f"Could not write to fallback log file: {log_e}")

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load saved state"""
        state_file: Path = self.output_dir / "state.json"
        if not state_file.exists():
            return None

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.error(f"Failed to load state: {e}")
            return None
