"""Per-event routing, delivery tracking and checkpointing.

Delivery guarantee (at-least-once): the stored event ID only moves forward after
``flush()`` confirms Redpanda acknowledged everything sent so far. A crash can therefore
re-send up to one checkpoint interval of events (Silver dedups them on ``meta_id``), but
it can never skip one.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from .keys import message_key
from .sse import SSEEvent
from .state import StateStore
from .topics import WIKI_EDITS, WIKI_EDITS_DLQ
from .validate import EventValidator


class DeliveryError(RuntimeError):
    """Redpanda permanently rejected a message; stop rather than skip it."""


class Publisher(Protocol):
    """The subset of a Kafka producer the pipeline needs."""

    delivered: dict[str, int]
    failed: int

    def produce(self, topic: str, key: str | None, value: str) -> None: ...

    def poll(self) -> None: ...

    def flush(self, timeout: float) -> int: ...


@dataclass
class Counters:
    received: int = 0
    published: int = 0
    rejected: int = 0
    canary: int = 0
    reconnects: int = 0
    checkpoints: int = 0
    save_failures: int = 0


class Pipeline:
    """Validates each SSE event, routes it to wiki_edits or the DLQ, and checkpoints."""

    def __init__(
        self,
        validator: EventValidator,
        publisher: Publisher,
        store: StateStore,
        *,
        clock: Callable[[], float] = time.monotonic,
        checkpoint_every: float = 30.0,
        log_every: float = 30.0,
        log: Callable[[dict[str, Any]], None] = lambda record: None,
    ) -> None:
        self.validator = validator
        self.publisher = publisher
        self.store = store
        self.clock = clock
        self.checkpoint_every = checkpoint_every
        self.log_every = log_every
        self.log = log
        self.counters = Counters()
        self.last_event_id: str | None = None
        self.saved_event_id: str | None = None
        self.last_event_dt: str | None = None
        now = clock()
        self._last_checkpoint = now
        self._last_log = now
        self._received_at_last_log = 0

    def start_from(self, event_id: str | None) -> None:
        """Set the position the stream resumes from (None means start at now)."""
        self.last_event_id = event_id
        self.saved_event_id = event_id

    def handle(self, sse_event: SSEEvent) -> None:
        """Route one SSE event and run any checkpoint or stats log that is due."""
        self.counters.received += 1
        # Canary check comes first: Wikimedia's canaries carry only $schema and meta, so
        # they would otherwise fail validation and pollute the DLQ.
        if is_canary(sse_event.data):
            self.counters.canary += 1
        else:
            self._route(sse_event.data)
        if sse_event.id:
            self.last_event_id = sse_event.id
        self.publisher.poll()
        self.tick()

    def _route(self, raw: str) -> None:
        """Send a valid event to wiki_edits, or the payload and reason to the DLQ."""
        result = self.validator.validate(raw)
        if result.ok:
            self.publisher.produce(WIKI_EDITS.name, message_key(result.event), raw)
            self.last_event_dt = result.event["meta"]["dt"]
        else:
            self.publisher.produce(
                WIKI_EDITS_DLQ.name, _dlq_key(raw), _dlq_value(raw, result.reason)
            )
            self.counters.rejected += 1

    def tick(self) -> None:
        """Checkpoint and log if their intervals have passed."""
        now = self.clock()
        if now - self._last_checkpoint >= self.checkpoint_every:
            self.checkpoint()
            self._last_checkpoint = now
        if now - self._last_log >= self.log_every:
            self.log_stats(now)

    def checkpoint(self) -> None:
        """Wait for every sent message to be acknowledged, then store the last event ID.

        Raises:
            DeliveryError: If any message failed permanently or the flush timed out. The
                stored ID is left unchanged, so a restart in resume mode re-reads from the
                last safe point.
        """
        remaining = self.publisher.flush(30.0)
        if remaining or self.publisher.failed:
            raise DeliveryError(
                f"{self.publisher.failed} failed, {remaining} unacknowledged; not checkpointing"
            )
        if self.last_event_id and self.last_event_id != self.saved_event_id:
            try:
                self.store.save(self.last_event_id)
            except Exception as exc:  # noqa: BLE001 - any storage failure is handled the same
                # Safe to continue: the previously saved position is still valid, so a crash
                # now would only replay more events (absorbed by Silver's MERGE). The next
                # checkpoint retries. Delivery failures above stay fatal; this does not.
                self.counters.save_failures += 1
                self.log({"msg": "checkpoint_save_failed", "error": type(exc).__name__})
                return
            self.saved_event_id = self.last_event_id
            self.counters.checkpoints += 1

    def log_stats(self, now: float | None = None) -> None:
        """Emit one structured stats record (counts and timings only, never payloads)."""
        now = self.clock() if now is None else now
        elapsed = max(now - self._last_log, 1e-9)
        window = self.counters.received - self._received_at_last_log
        self.counters.published = self.publisher.delivered.get(WIKI_EDITS.name, 0)
        self.log(
            {
                "msg": "producer_stats",
                **asdict(self.counters),
                "events_per_sec": round(window / elapsed, 2),
                "last_event_dt": self.last_event_dt,
            }
        )
        self._last_log = now
        self._received_at_last_log = self.counters.received


def is_canary(raw: str) -> bool:
    """Return True for Wikimedia's synthetic monitoring events (meta.domain == "canary")."""
    try:
        meta = json.loads(raw).get("meta")
    except (ValueError, AttributeError):
        return False
    return isinstance(meta, dict) and meta.get("domain") == "canary"


def _dlq_value(raw: str, reason: str | None) -> str:
    return json.dumps(
        {
            "error": reason,
            "raw": raw,
            "rejected_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        }
    )


def _dlq_key(raw: str) -> str | None:
    """Keep the original key when the payload still has wiki and title."""
    try:
        event = json.loads(raw)
        return message_key(event)
    except (ValueError, TypeError, KeyError):
        return None
