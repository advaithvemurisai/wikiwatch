"""Connection loop: fresh or resume start, reconnects with backoff, clean shutdown."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator

import httpx

from .pipeline import Pipeline
from .sse import SSEEvent, backoff_delay
from .state import StateStore

MODES = ("fresh", "resume")


class ResumeError(RuntimeError):
    """Resume mode was requested but no event ID has ever been stored."""


def start_position(mode: str, store: StateStore) -> str | None:
    """Return the event ID to start from: None for fresh (start at now), stored for resume.

    Resume never falls back to fresh silently: starting at now would create a gap that
    nobody notices.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    if mode == "fresh":
        return None
    stored = store.load()
    if stored is None:
        raise ResumeError("No stored event ID yet: start once with MODE=fresh")
    return stored


def run_connections(
    stream: Callable[[str | None], Iterator[SSEEvent]],
    pipeline: Pipeline,
    *,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[dict], None] = lambda record: None,
    max_connections: int | None = None,
) -> None:
    """Read the stream forever, reconnecting from the last received event ID.

    Wikimedia closes each connection after 15 minutes, so a clean close is normal and
    reconnects immediately. Errors back off exponentially with jitter, and the backoff
    resets once a connection delivers events again. ``max_connections`` exists for tests.
    """
    failures = 0
    connections = 0
    while max_connections is None or connections < max_connections:
        connections += 1
        delivered = False
        reason = "closed_by_server"
        try:
            for event in stream(pipeline.last_event_id):
                delivered = True
                pipeline.handle(event)
        except httpx.HTTPError as exc:
            reason = type(exc).__name__
        failures = 0 if delivered else failures + 1
        delay = backoff_delay(failures) if failures else 0.0
        pipeline.counters.reconnects += 1
        log({"msg": "reconnect", "reason": reason, "delay_s": round(delay, 2)})
        pipeline.tick()
        if delay:
            sleep(delay)
