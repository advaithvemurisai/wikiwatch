from __future__ import annotations

import json

import httpx
import pytest

from conftest import FakePublisher, FakeStore, as_data, make_event
from wikiwatch_producer.pipeline import DeliveryError, Pipeline
from wikiwatch_producer.runner import ResumeError, run_connections, start_position
from wikiwatch_producer.sse import SSEEvent


def sse(event: dict, event_id: str) -> SSEEvent:
    return SSEEvent(data=as_data(event), id=event_id)


# ---------------------------------------------------------------- routing


def test_valid_event_goes_to_wiki_edits_keyed_by_page(parts):
    pipeline, publisher, *_ = parts
    pipeline.handle(sse(make_event(), "pos-1"))
    assert publisher.sent == [("wiki_edits", "enwiki:Acme Corporation", as_data(make_event()))]


def test_invalid_event_goes_to_dlq_with_reason_and_raw(parts):
    pipeline, publisher, *_ = parts
    bad = make_event()
    del bad["title"]
    pipeline.handle(sse(bad, "pos-1"))
    topic, key, value = publisher.sent[0]
    body = json.loads(value)
    assert topic == "wiki_edits_dlq"
    assert key is None  # no title, so no page key
    assert body["error"] == "title: required"
    assert json.loads(body["raw"]) == bad
    assert body["rejected_at"].endswith("+00:00")
    assert pipeline.counters.rejected == 1


def test_dlq_keeps_original_key_when_possible(parts):
    pipeline, publisher, *_ = parts
    pipeline.handle(sse(make_event(namespace="zero"), "pos-1"))
    assert publisher.sent[0][:2] == ("wiki_edits_dlq", "enwiki:Acme Corporation")


def test_unparseable_payload_goes_to_dlq(parts):
    pipeline, publisher, *_ = parts
    pipeline.handle(SSEEvent(data="{broken", id="pos-1"))
    assert publisher.sent[0][0] == "wiki_edits_dlq"
    assert json.loads(publisher.sent[0][2])["error"] == "invalid_json"


def test_canary_events_are_dropped_and_counted(parts):
    pipeline, publisher, *_ = parts
    canary = make_event(meta={"id": "c", "dt": "2026-10-03T12:00:00Z", "domain": "canary"})
    pipeline.handle(sse(canary, "pos-1"))
    assert publisher.sent == []
    assert pipeline.counters.canary == 1
    assert pipeline.last_event_id == "pos-1"  # still advances the position


def test_bare_canary_like_wikimedia_sends_is_dropped_not_dlqd(parts):
    """Live canaries carry only $schema and meta (seen on 2026-10-03)."""
    pipeline, publisher, *_ = parts
    canary = {
        "$schema": "/mediawiki/recentchange/1.0.1",
        "meta": {"id": "c", "dt": "2026-10-03T21:15:00Z", "domain": "canary"},
    }
    pipeline.handle(sse(canary, "pos-1"))
    assert publisher.sent == []
    assert pipeline.counters.canary == 1 and pipeline.counters.rejected == 0


# ---------------------------------------------------------------- checkpointing


def test_checkpoint_happens_every_30_seconds_after_flush(parts):
    pipeline, publisher, store, clock, _ = parts
    pipeline.handle(sse(make_event(), "pos-1"))
    clock.now = 29.9
    pipeline.handle(sse(make_event(meta={"id": "id-2", "dt": "x"}), "pos-2"))
    assert store.saves == []
    clock.now = 30.0
    pipeline.handle(sse(make_event(meta={"id": "id-3", "dt": "x"}), "pos-3"))
    assert store.saves == ["pos-3"]
    assert publisher.flushes == 1


def test_id_is_never_saved_when_delivery_failed(validator):
    publisher, store = FakePublisher(fail=True), FakeStore()
    pipeline = Pipeline(validator, publisher, store)
    pipeline.handle(sse(make_event(), "pos-1"))
    with pytest.raises(DeliveryError):
        pipeline.checkpoint()
    assert store.saves == []


def test_unchanged_position_is_not_rewritten(parts):
    pipeline, _, store, *_ = parts
    pipeline.handle(sse(make_event(), "pos-1"))
    pipeline.checkpoint()
    pipeline.checkpoint()
    assert store.saves == ["pos-1"]


def test_stats_log_has_counts_and_rate_but_no_payload(parts):
    pipeline, _, _, clock, logs = parts
    for i in range(3):
        pipeline.handle(sse(make_event(user="SomeEditor"), f"pos-{i}"))
    clock.now = 30.0
    pipeline.tick()
    record = logs[-1]
    assert record["msg"] == "producer_stats"
    assert record["received"] == 3 and record["published"] == 3
    assert record["events_per_sec"] == pytest.approx(0.1)
    assert "SomeEditor" not in json.dumps(logs)


# ---------------------------------------------------------------- modes and resume


def test_fresh_mode_starts_at_now():
    assert start_position("fresh", FakeStore("old")) is None


def test_resume_mode_uses_stored_id():
    assert start_position("resume", FakeStore("pos-9")) == "pos-9"


def test_resume_without_stored_id_fails_loudly():
    with pytest.raises(ResumeError):
        start_position("resume", FakeStore(None))


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        start_position("latest", FakeStore("x"))


def test_reconnects_resume_from_last_received_id(parts):
    """The server closes every 15 minutes; each reconnect sends Last-Event-ID."""
    pipeline, *_ = parts
    pipeline.start_from("pos-0")
    seen_ids = []
    batches = iter([["pos-1", "pos-2"], ["pos-3"], []])

    def stream(last_event_id):
        seen_ids.append(last_event_id)
        for event_id in next(batches):
            yield sse(make_event(meta={"id": event_id, "dt": "x"}), event_id)

    run_connections(stream, pipeline, sleep=lambda s: None, max_connections=3)
    assert seen_ids == ["pos-0", "pos-2", "pos-3"]
    assert pipeline.counters.reconnects == 3


def test_errors_back_off_and_successful_connection_resets(parts):
    pipeline, *_ = parts
    sleeps = []
    outcomes = iter(["fail", "fail", "ok", "fail"])

    def stream(last_event_id):
        if next(outcomes) == "fail":
            raise httpx.ConnectError("down")
        yield sse(make_event(), "pos-1")

    run_connections(stream, pipeline, sleep=sleeps.append, max_connections=4)
    # two failures back off, the good connection reconnects at once, then backoff restarts
    assert len(sleeps) == 3
    assert all(s >= 0 for s in sleeps)


def test_kill_and_resume_loses_nothing(validator):
    """Simulated crash: events after the last checkpoint are re-read, never skipped."""
    store, clock_value = FakeStore(), [0.0]
    upstream = [f"pos-{i}" for i in range(10)]

    def run_until(stop_after: int, start: str | None) -> list[str]:
        publisher = FakePublisher()
        pipeline = Pipeline(validator, publisher, store, clock=lambda: clock_value[0])
        pipeline.start_from(start)
        begin = 0 if start is None else upstream.index(start) + 1
        for i, event_id in enumerate(upstream[begin:]):
            if i == stop_after:
                break  # crash: no final checkpoint
            clock_value[0] += 10  # 10 s per event, checkpoint every 30 s
            pipeline.handle(sse(make_event(meta={"id": event_id, "dt": "x"}), event_id))
        publisher.flush(1)
        return [json.loads(v)["meta"]["id"] for t, _, v in publisher.sent if t == "wiki_edits"]

    first = run_until(stop_after=5, start=None)
    second = run_until(stop_after=100, start=start_position("resume", store))
    assert set(first) | set(second) == set(upstream)
    assert len(first) + len(second) >= len(upstream)  # overlap allowed, gaps never
