"""Progress listener buffering and flushing (no JVM needed)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime

from streaming.lib.progress import ProgressBuffer, flush_once, object_key, progress_record

SAMPLE = {
    "id": "q-1",
    "runId": "run-1",
    "name": "silver_edits",
    "timestamp": "2026-10-03T21:05:00.000Z",
    "batchId": 7,
    "numInputRows": 2400,
    "inputRowsPerSecond": 40.0,
    "processedRowsPerSecond": 900.5,
    "durationMs": {"triggerExecution": 2660, "addBatch": 2100},
    "eventTime": {},
    "sources": [
        {
            "description": "KafkaV2[Subscribe[wiki_edits]]",
            "startOffset": {"wiki_edits": {"0": 10}},
            "endOffset": {"wiki_edits": {"0": 410}},
            "latestOffset": {"wiki_edits": {"0": 415}},
            "numInputRows": 400,
        }
    ],
}


def test_record_keeps_counts_timings_and_offsets_only():
    record = progress_record(SAMPLE, "redpanda-abc")
    assert record["session_id"] == "redpanda-abc"
    assert (record["query"], record["batch_id"], record["num_input_rows"]) == (
        "silver_edits",
        7,
        2400,
    )
    assert record["trigger_ms"] == 2660
    assert record["sources"][0]["latest_offset"] == {"wiki_edits": {"0": 415}}
    assert "description" not in json.dumps(record)


def test_flush_writes_one_jsonl_file_and_empties_the_buffer():
    buffer, written = ProgressBuffer(), {}
    buffer.add({"batch_id": 1})
    buffer.add({"batch_id": 2})
    now = datetime(2026, 10, 3, 21, 5, 9, tzinfo=UTC)
    assert flush_once(buffer, written.__setitem__, now=now) == 2
    ((key, text),) = written.items()
    assert key.startswith("ops/stream_progress/dt=2026-10-03/210509-")
    assert [json.loads(line)["batch_id"] for line in text.splitlines()] == [1, 2]
    assert flush_once(buffer, written.__setitem__) == 0  # nothing left, nothing written
    assert len(written) == 1


def test_object_keys_are_unique_and_utc_dated():
    now = datetime(2026, 10, 3, 23, 59, 59, tzinfo=UTC)
    assert object_key(now) != object_key(now)
    assert "/dt=2026-10-03/" in object_key(now)


def test_buffer_is_safe_under_concurrent_adds():
    buffer = ProgressBuffer()
    threads = [
        threading.Thread(target=lambda: [buffer.add({"n": i}) for i in range(500)])
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(buffer.drain()) == 2000
