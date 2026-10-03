"""Minimal Server-Sent Events client for Wikimedia EventStreams.

Only the parts of the SSE spec that EventStreams uses: ``id``, ``event`` and ``data``
fields, comments, and blank-line dispatch. Wikimedia closes every connection after
15 minutes, so reconnecting with ``Last-Event-ID`` is the normal path, not an error path.
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class SSEEvent:
    """One dispatched SSE event."""

    data: str
    id: str | None = None
    event: str = "message"


def parse_events(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Turn SSE text lines into events.

    Multiple ``data:`` lines join with ``\\n``. The ``id`` is sticky: an event without
    its own ``id`` field keeps the last one seen, as the spec requires.
    """
    data: list[str] = []
    event_type = "message"
    last_id: str | None = None
    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line:
            if data:
                yield SSEEvent(data="\n".join(data), id=last_id, event=event_type)
            data, event_type = [], "message"
            continue
        if line.startswith(":"):
            continue  # comment / keep-alive
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id" and "\0" not in value:
            last_id = value
        elif field == "event":
            event_type = value or "message"
    if data:
        yield SSEEvent(data="\n".join(data), id=last_id, event=event_type)


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0, rng=random) -> float:
    """Exponential backoff with full jitter: a random delay in [0, min(cap, base * 2^n)]."""
    ceiling = min(cap, base * (2 ** max(attempt, 0)))
    return rng.uniform(0, ceiling)


class SSEClient:
    """Opens the stream and yields events; one call to ``stream`` is one connection."""

    def __init__(self, url: str, user_agent: str, read_timeout: float = 60.0) -> None:
        self.url = url
        self._client = httpx.Client(
            headers={"User-Agent": user_agent, "Accept": "text/event-stream"},
            timeout=httpx.Timeout(connect=10.0, read=read_timeout, write=10.0, pool=10.0),
            follow_redirects=True,
        )

    def stream(self, last_event_id: str | None = None) -> Iterator[SSEEvent]:
        """Yield events from one connection, resuming after ``last_event_id`` if given."""
        headers = {"Last-Event-ID": last_event_id} if last_event_id else {}
        with self._client.stream("GET", self.url, headers=headers) as response:
            response.raise_for_status()
            yield from parse_events(response.iter_lines())

    def close(self) -> None:
        self._client.close()
