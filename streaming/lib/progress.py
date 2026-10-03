"""Streaming progress metrics to ops/stream_progress/ (JSON Lines, never Iceberg).

The listener only appends small records to an in-memory buffer. A background thread
drains the buffer once a minute and writes one file, so no write ever happens on Spark's
streaming or listener threads. Records hold counts, timings and offsets only.
"""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener


def progress_record(progress: dict[str, Any], session_id: str) -> dict[str, Any]:
    """Select the fields the Pipeline health page and lag checks need."""
    durations = progress.get("durationMs") or {}
    return {
        "session_id": session_id,
        "query": progress.get("name"),
        "run_id": progress.get("runId"),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "num_input_rows": progress.get("numInputRows"),
        "input_rows_per_second": progress.get("inputRowsPerSecond"),
        "processed_rows_per_second": progress.get("processedRowsPerSecond"),
        "trigger_ms": durations.get("triggerExecution"),
        "add_batch_ms": durations.get("addBatch"),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "sources": [
            {
                "start_offset": source.get("startOffset"),
                "end_offset": source.get("endOffset"),
                "latest_offset": source.get("latestOffset"),
            }
            for source in progress.get("sources") or []
        ],
    }


class ProgressBuffer:
    """Thread-safe list of records."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: list[dict[str, Any]] = []

    def add(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._records.append(record)

    def drain(self) -> list[dict[str, Any]]:
        with self._lock:
            records, self._records = self._records, []
        return records


class ProgressListener(StreamingQueryListener):
    """Buffers every micro-batch's progress; does no I/O."""

    def __init__(self, buffer: ProgressBuffer, session_id: str) -> None:
        self.buffer = buffer
        self.session_id = session_id

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark's API name
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        self.buffer.add(progress_record(json.loads(event.progress.json), self.session_id))

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def object_key(now: datetime) -> str:
    """ops/stream_progress/dt=YYYY-MM-DD/<HHMMSS>-<uuid>.jsonl (UTC)."""
    return f"ops/stream_progress/dt={now:%Y-%m-%d}/{now:%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl"


def flush_once(buffer: ProgressBuffer, write: Callable[[str, str], None], now=None) -> int:
    """Write everything buffered as one JSON Lines file. Returns the record count."""
    records = buffer.drain()
    if records:
        now = now or datetime.now(UTC)
        write(object_key(now), "".join(json.dumps(r) + "\n" for r in records))
    return len(records)


class Flusher(threading.Thread):
    """Daemon thread that flushes the buffer every ``interval`` seconds."""

    def __init__(self, buffer, write, interval: float = 60.0) -> None:
        super().__init__(name="progress-flusher", daemon=True)
        self.buffer, self.write, self.interval = buffer, write, interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            try:
                flush_once(self.buffer, self.write)
            except Exception as exc:  # noqa: BLE001 - metrics must never stop the stream
                print(json.dumps({"msg": "progress_flush_failed", "error": type(exc).__name__}))

    def stop(self) -> None:
        """Stop the loop and flush what is left."""
        self._stop_event.set()
        flush_once(self.buffer, self.write)


def hadoop_writer(spark, bucket: str) -> Callable[[str, str], None]:
    """Return write(key, text) that stores a small object through Hadoop's s3a FileSystem."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    def write(key: str, text: str) -> None:
        path = jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{key}")
        stream = path.getFileSystem(conf).create(path, True)
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()

    return write
