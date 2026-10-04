"""What the lake must contain after replaying the e2e fixture, computed from the fixture.

Pure functions, unit-tested in tests/test_e2e_expectations.py, so the end-to-end checks
are themselves checked.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

COUNTED_TYPES = ("edit", "new")  # same definition as the windows (ADR 0006)


def read_events(path: Path) -> list[dict]:
    """Parse a (possibly gzipped) JSONL fixture. Lines that are not objects are skipped."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        events = [json.loads(line) for line in f if line.strip()]
    return [e for e in events if isinstance(e, dict)]


def unique_valid(events: list[dict]) -> dict[str, dict]:
    """First copy of each meta_id among events the producer accepts (have a title)."""
    unique: dict[str, dict] = {}
    for event in events:
        if "title" not in event or not event.get("meta", {}).get("id"):
            continue  # the malformed event goes to the DLQ
        unique.setdefault(event["meta"]["id"], event)
    return unique


def minute_of(event: dict) -> datetime:
    """UTC minute of the event time, as a naive datetime (how the lake stores it)."""
    ts = datetime.fromisoformat(event["meta"]["dt"].replace("Z", "+00:00"))
    return ts.replace(tzinfo=None, second=0, microsecond=0)


def window_counts(events) -> dict[tuple[datetime, str], tuple[int, int, int]]:
    """(minute, wiki) -> (edits, bot_edits, bytes_changed) for edit and new events."""
    counts: dict[tuple[datetime, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    for event in events:
        if event.get("type") not in COUNTED_TYPES:
            continue
        key = (minute_of(event), event["wiki"])
        length = event.get("length") or {}
        old, new = length.get("old"), length.get("new")
        if old is not None and new is not None:
            delta = abs(new - old)
        elif event["type"] == "new" and new is not None:
            delta = abs(new)
        else:
            delta = 0
        counts[key][0] += 1
        counts[key][1] += int(bool(event.get("bot")))
        counts[key][2] += delta
    return {k: tuple(v) for k, v in counts.items()}


def expected_windows(phase1: list[dict], phase2: list[dict], flush_id: str):
    """Return (final, realtime) window expectations.

    final:    every unique valid event, late ones included (dbt from Silver).
    realtime: the streaming table drops the phase-2 late events (behind the watermark)
              and never emits the flush event's own window, which no later event closes.
    """
    all_unique = unique_valid(phase1 + phase2)
    final = window_counts(all_unique.values())
    phase1_unique = unique_valid(phase1)
    realtime = window_counts(phase1_unique.values())
    flush_minute = minute_of(all_unique[flush_id])
    final_closed = {k: v for k, v in final.items() if k[0] < flush_minute}
    return final, realtime, final_closed
