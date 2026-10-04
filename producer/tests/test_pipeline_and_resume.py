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


def test_storage_outage_does_not_stop_the_producer(validator):
    """A failed save keeps the old position and retries at the next checkpoint."""

    class FlakyStore(FakeStore):
        def __init__(self):
            super().__init__()
            self.fail_next = True

        def save(self, last_event_id):
            if self.fail_next:
                self.fail_next = False
                raise ConnectionError("object storage restarting")
            super().save(last_event_id)

    store, logs = FlakyStore(), []
    pipeline = Pipeline(validator, FakePublisher(), store, log=logs.append)
    pipeline.handle(sse(make_event(), "pos-1"))
    pipeline.checkpoint()  # fails: logged, not raised
    assert store.saves == [] and pipeline.saved_event_id is None
    assert pipeline.counters.save_failures == 1
    assert logs[-1] == {"msg": "checkpoint_save_failed", "error": "ConnectionError"}
    pipeline.checkpoint()  # storage is back
    assert store.saves == ["pos-1"]


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


def test_flapping_connection_never_reconnects_in_a_tight_loop(parts):
    """A connection that delivers events and then breaks still pauses before retrying."""
    pipeline, *_ = parts
    sleeps, logs = [], []

    def stream(last_event_id):
        yield sse(make_event(), "pos-1")
        raise httpx.ReadError("connection reset")

    run_connections(stream, pipeline, sleep=sleeps.append, log=logs.append, max_connections=3)
    assert len(sleeps) == 3  # one pause per broken connection
    assert all(0 < s <= 1.0 for s in sleeps)
    assert {r["reason"] for r in logs} == {"ReadError"}


def test_http_errors_are_logged_with_status_code(parts):
    pipeline, *_ = parts
    logs = []
    request = httpx.Request("GET", "https://stream.example/")
    response = httpx.Response(429, request=request)

    def stream(last_event_id):
        raise httpx.HTTPStatusError("too many", request=request, response=response)
        yield  # pragma: no cover - makes this a generator

    run_connections(stream, pipeline, sleep=lambda s: None, log=logs.append, max_connections=1)
    assert logs[0]["reason"] == "HTTP 429"


def simulate_crash_and_resume(validator, first_run_cls=Pipeline) -> tuple[set[str], set[str]]:
    """Run until a crash, then resume; return (upstream IDs, IDs acknowledged by Redpanda).

    The fake publisher acknowledges messages only on flush, like a real client whose
    buffer dies with the process: in a crash, unflushed messages are lost and no final
    checkpoint runs. ``first_run_cls`` lets a test swap in a buggy pipeline for the run
    that crashes.
    """
    store, clock_value = FakeStore(), [0.0]
    upstream = [f"pos-{i}" for i in range(20)]

    def run(pipeline_cls, start: str | None, crash_after: int | None) -> list[str]:
        publisher = FakePublisher()
        pipeline = pipeline_cls(validator, publisher, store, clock=lambda: clock_value[0])
        pipeline.start_from(start)
        begin = 0 if start is None else upstream.index(start) + 1
        for i, event_id in enumerate(upstream[begin:]):
            if i == crash_after:
                break
            clock_value[0] += 7  # checkpoints (every 30 s) land between events
            pipeline.handle(sse(make_event(meta={"id": event_id, "dt": "x"}), event_id))
        if crash_after is None:
            pipeline.checkpoint()  # clean shutdown
        return [json.loads(v)["meta"]["id"] for t, _, v in publisher.acked if t == "wiki_edits"]

    first = run(first_run_cls, start=None, crash_after=11)
    second = run(Pipeline, start=start_position("resume", store), crash_after=None)
    return set(upstream), set(first) | set(second)


def test_kill_and_resume_loses_nothing(validator):
    upstream, reached = simulate_crash_and_resume(validator)
    assert reached == upstream


def test_kill_test_detects_saving_without_flushing(validator):
    """Mutation check: the test above must fail for the classic at-least-once bug."""

    class SavesWithoutFlushing(Pipeline):
        def checkpoint(self):
            if self.last_event_id and self.last_event_id != self.saved_event_id:
                self.store.save(self.last_event_id)
                self.saved_event_id = self.last_event_id

    upstream, reached = simulate_crash_and_resume(validator, SavesWithoutFlushing)
    assert reached != upstream  # events before the early-saved position are lost
