"""Replay a recorded JSONL file through the producer's publish path into Redpanda.

Each line goes through the same validation, canary check, keying and DLQ routing as a live
event (Pipeline.handle). Nothing is checkpointed: replays never move the producer's real
stream position. Used to prove Silver dedup (replay the same file twice) and by Task 6.

Usage: python scripts/replay_fixture.py --file tmp/sample.jsonl [--bootstrap localhost:19092]
"""

from __future__ import annotations

import argparse
import gzip
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient

from wikiwatch_producer.pipeline import Pipeline
from wikiwatch_producer.publisher import KafkaPublisher
from wikiwatch_producer.sse import SSEEvent
from wikiwatch_producer.topics import WIKI_EDITS, ensure_topics
from wikiwatch_producer.validate import EventValidator

LOCALHOST_V4 = {"broker.address.family": "v4"}  # skip the IPv6 attempt for localhost


class NoCheckpoint:
    """State store that never saves: a replay must not touch the live stream position."""

    def load(self) -> None:
        return None

    def save(self, last_event_id: str) -> None:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--bootstrap", default="localhost:19092")
    args = parser.parse_args()

    ensure_topics(AdminClient({"bootstrap.servers": args.bootstrap, **LOCALHOST_V4}))
    publisher = KafkaPublisher(args.bootstrap, extra_config=LOCALHOST_V4)
    pipeline = Pipeline(EventValidator(), publisher, NoCheckpoint(), checkpoint_every=1e9)
    opener = gzip.open if args.file.suffix == ".gz" else open
    with opener(args.file, "rt", encoding="utf-8") as f:
        lines = f.read().splitlines()
    for line in lines:
        if line.strip():
            pipeline.handle(SSEEvent(data=line))
    pipeline.checkpoint()  # flushes and raises if anything was not acknowledged
    counters = pipeline.counters
    print(
        f"replayed {counters.received} lines: "
        f"{publisher.delivered.get(WIKI_EDITS.name, 0)} to {WIKI_EDITS.name}, "
        f"{counters.rejected} to DLQ, {counters.canary} canaries dropped"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
