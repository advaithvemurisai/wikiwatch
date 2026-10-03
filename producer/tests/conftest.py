"""Shared fakes: no network, no broker, no S3."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from wikiwatch_producer.pipeline import Pipeline
from wikiwatch_producer.validate import EventValidator


def make_event(**overrides) -> dict:
    """A valid recentchange event; override any top-level field."""
    event = {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {"id": "id-1", "dt": "2026-10-03T12:00:00Z", "domain": "en.wikipedia.org"},
        "type": "edit",
        "namespace": 0,
        "title": "Acme Corporation",
        "wiki": "enwiki",
        "user": "ExampleUser",
        "bot": False,
        "length": {"old": 10_000, "new": 7_000},
        "timestamp": 1_791_000_000,
    }
    event.update(overrides)
    return event


def as_data(event: dict) -> str:
    return json.dumps(event)


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[tuple[str, str | None, str]] = []
        self.pending = 0
        self.delivered: Counter[str] = Counter()
        self.failed = 0
        self.fail = fail
        self.flushes = 0

    def produce(self, topic, key, value):
        self.sent.append((topic, key, value))
        self.pending += 1

    def poll(self):
        pass

    def flush(self, timeout):
        self.flushes += 1
        for topic, _, _ in self.sent[len(self.sent) - self.pending :]:
            if self.fail:
                self.failed += 1
            else:
                self.delivered[topic] += 1
        self.pending = 0
        return 0


class FakeStore:
    def __init__(self, value: str | None = None) -> None:
        self.value = value
        self.saves: list[str] = []

    def load(self):
        return self.value

    def save(self, last_event_id):
        self.value = last_event_id
        self.saves.append(last_event_id)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def validator() -> EventValidator:
    return EventValidator()


@pytest.fixture
def parts(validator):
    publisher, store, clock, logs = FakePublisher(), FakeStore(), FakeClock(), []
    pipeline = Pipeline(
        validator, publisher, store, clock=clock, checkpoint_every=30, log_every=30, log=logs.append
    )
    return pipeline, publisher, store, clock, logs
