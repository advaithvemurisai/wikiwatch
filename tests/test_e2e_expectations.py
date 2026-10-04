"""The e2e expectations are computed from the fixture; check that computation itself."""

from __future__ import annotations

import gzip
import json
from datetime import datetime
from pathlib import Path

from tests.e2e.expectations import expected_windows, read_events, unique_valid, window_counts

FIXTURES = Path(__file__).resolve().parent / "e2e" / "fixtures"


def ev(meta_id, dt, wiki="enwiki", typ="edit", bot=False, old=100, new=150, title="T"):
    event = {"meta": {"id": meta_id, "dt": dt}, "type": typ, "wiki": wiki, "bot": bot}
    if title is not None:
        event["title"] = title
    if typ in ("edit", "new"):
        event["length"] = {"old": old, "new": new}
    return event


def test_duplicates_and_malformed_events_are_excluded():
    events = [
        ev("a", "2026-10-03T12:00:01Z"),
        ev("a", "2026-10-03T12:00:01Z"),
        ev("m", "x", title=None),
    ]
    assert list(unique_valid(events)) == ["a"]


def test_window_counts_match_the_streaming_definition():
    events = [
        ev("a", "2026-10-03T12:00:10Z", old=1000, new=700),
        ev("b", "2026-10-03T12:00:50Z", bot=True, typ="new", old=None, new=40),
        ev("c", "2026-10-03T12:00:20Z", typ="log"),
        ev("d", "2026-10-03T12:01:00Z", wiki="dewiki"),
    ]
    assert window_counts(events) == {
        (datetime(2026, 10, 3, 12, 0), "enwiki"): (2, 1, 340),
        (datetime(2026, 10, 3, 12, 1), "dewiki"): (1, 0, 50),
    }


def test_realtime_expectation_drops_late_events_and_the_flush_window():
    phase1 = [ev("a", "2026-10-03T12:05:00Z")]
    phase2 = [ev("late", "2026-10-03T11:50:00Z"), ev("flush", "2026-10-03T12:20:00Z")]
    final, realtime, closed = expected_windows(phase1, phase2, "flush")
    assert (datetime(2026, 10, 3, 11, 50), "enwiki") in final
    assert (datetime(2026, 10, 3, 11, 50), "enwiki") not in realtime
    assert (datetime(2026, 10, 3, 12, 20), "enwiki") not in closed


def test_committed_fixture_matches_its_expected_file():
    expected = json.loads((FIXTURES / "e2e_expected.json").read_text())
    phase1 = read_events(FIXTURES / "e2e_phase1.jsonl.gz")
    phase2 = read_events(FIXTURES / "e2e_phase2.jsonl.gz")
    assert len(phase1) == expected["phase1_lines"]
    assert len(phase2) == expected["phase2_lines"]
    assert len(unique_valid(phase1 + phase2)) == expected["unique_valid_events"]
    assert sum("title" not in e for e in phase1 + phase2) == expected["dlq_rows"]
    assert {e["meta"]["id"] for e in phase2} >= set(expected["late_meta_ids"])


def test_fixture_contains_no_real_user_names():
    with gzip.open(FIXTURES / "e2e_phase1.jsonl.gz", "rt") as f:
        users = {json.loads(line).get("user") for line in f}
    users.discard(None)
    assert all(u.startswith(("Editor-", "Bot-", "~2026-")) for u in users), sorted(users)[:5]
