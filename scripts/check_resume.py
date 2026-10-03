"""Prove the producer lost no events across a kill and resume.

Compares the event IDs in Redpanda (wiki_edits plus wiki_edits_dlq) with a reference
recording made by an independent consumer of the same stream (record_reference.py).
Every reference event inside the overlapping time window must be in Redpanda.
Duplicates are allowed (at-least-once) and reported.

Usage: python scripts/check_resume.py [--reference tmp/reference_ids.jsonl]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parent.parent
EDGE = timedelta(seconds=60)  # ignore the ragged first/last minute of each recording


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_topic_ids(bootstrap: str) -> tuple[Counter[str], dict[str, datetime]]:
    """Read every message from both topics; return ID counts and their event times."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "broker.address.family": "v4",  # localhost: skip the IPv6 attempt
            "group.id": "wikiwatch-check-resume",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    counts: Counter[str] = Counter()
    times: dict[str, datetime] = {}
    for topic in ("wiki_edits", "wiki_edits_dlq"):
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high <= low:
                continue
            consumer.assign([TopicPartition(topic, p, low)])
            position = low
            while position < high:
                msg = consumer.poll(5)
                if msg is None:
                    break
                position = msg.offset() + 1
                if msg.error():
                    continue
                value = json.loads(msg.value())
                event = json.loads(value["raw"]) if topic.endswith("_dlq") else value
                try:
                    event_id, dt = event["meta"]["id"], parse_dt(event["meta"]["dt"])
                except (KeyError, TypeError, ValueError):
                    continue
                counts[event_id] += 1
                times[event_id] = dt
    consumer.close()
    return counts, times


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reference", type=Path, default=ROOT / "tmp/reference_ids.jsonl")
    parser.add_argument("--bootstrap", default="localhost:19092")
    args = parser.parse_args()

    reference = [json.loads(line) for line in args.reference.read_text().splitlines() if line]
    ref_times = {r["id"]: parse_dt(r["dt"]) for r in reference if r.get("dt")}
    counts, topic_times = read_topic_ids(args.bootstrap)
    if not counts or not ref_times:
        print("nothing to compare: topic or reference is empty")
        return 1

    start = max(min(ref_times.values()), min(topic_times.values())) + EDGE
    end = min(max(ref_times.values()), max(topic_times.values())) - EDGE
    window = {i for i, t in ref_times.items() if start <= t <= end}
    missing = sorted(window - counts.keys())
    duplicates = sum(c - 1 for c in counts.values() if c > 1)

    print(
        json.dumps(
            {
                "window_utc": [start.isoformat(), end.isoformat()],
                "reference_events_in_window": len(window),
                "missing_from_redpanda": len(missing),
                "redpanda_messages": sum(counts.values()),
                "duplicates_in_redpanda": duplicates,
            }
        )
    )
    if missing:
        print(f"FAILED: {len(missing)} reference events are missing (first: {missing[:3]})")
        return 1
    print("PASSED: no event IDs were lost across the restart")
    return 0


if __name__ == "__main__":
    sys.exit(main())
