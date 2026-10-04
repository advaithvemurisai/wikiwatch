"""Pipeline health: dashboard/v1/health.json and ops/lag_snapshots/ (freshness_monitor DAG).

health.json is rewritten every 5 minutes during a session, so its generated_at is the
pipeline heartbeat the site uses for the offline banner (docs/v1.md).

Consumer lag: Spark's Kafka source does not commit offsets to a consumer group, so there
is no group lag to read. Lag is Redpanda's latest offset per partition minus the end
offset of the Silver query's last micro-batch, from ops/stream_progress/ (ADR 0011).
"Received" in the funnel is everything Redpanda accepted this session: the sum of the
topics' latest offsets, which start at 0 in every session.
"""

from __future__ import annotations

import json
import os
import re
import statistics
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from . import queries as q
from .engines import Engine
from .snapshots import SCHEMA_VERSION, iso, write
from .store import Store

PROGRESS_PREFIX = "ops/stream_progress/"
LAG_PREFIX = "ops/lag_snapshots/"
STATS_WINDOW = timedelta(minutes=60)
LAG_QUERY = "silver_edits"
TOPIC = "wiki_edits"


@dataclass(frozen=True)
class KafkaState:
    """What Redpanda reports: session ID and latest offset per partition per topic."""

    session_id: str
    high_watermarks: dict[str, dict[int, int]]


def kafka_state(bootstrap: str) -> KafkaState:
    """Read the cluster ID (session) and latest offsets from Redpanda."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient

    extra = {"broker.address.family": "v4"} if bootstrap.startswith("localhost") else {}
    admin = AdminClient({"bootstrap.servers": bootstrap, **extra})
    cluster_id = admin.describe_cluster().result().cluster_id
    session_id = re.sub(r"[^A-Za-z0-9-]+", "-", cluster_id).strip("-")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "freshness", **extra})
    highs: dict[str, dict[int, int]] = {}
    try:
        metadata = consumer.list_topics(timeout=10).topics
        for topic in (TOPIC, "wiki_edits_dlq"):
            if topic not in metadata:
                continue
            highs[topic] = {
                p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in metadata[topic].partitions
            }
    finally:
        consumer.close()
    return KafkaState(session_id, highs)


def recent_progress(store: Store, session_id: str, now: datetime) -> list[dict]:
    """Progress records of this session from the last hour (today's and yesterday's files)."""
    since = now - STATS_WINDOW
    records: list[dict] = []
    for day in sorted({since.date(), now.date()}):
        for key in store.list_keys(f"{PROGRESS_PREFIX}dt={day.isoformat()}/"):
            for line in store.get_text(key).splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                stamp = record.get("timestamp")
                if record.get("session_id") != session_id or not stamp:
                    continue
                when = datetime.fromisoformat(stamp.replace("Z", "+00:00")).replace(tzinfo=None)
                if since <= when <= now:
                    records.append(record)
    return sorted(records, key=lambda r: r["timestamp"])


def streaming_stats(records: list[dict]) -> list[dict]:
    by_query: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_query[r.get("query") or "unknown"].append(r)
    stats = []
    for query, rows in sorted(by_query.items()):
        durations = [r["trigger_ms"] for r in rows if r.get("trigger_ms") is not None]
        rates = [r["input_rows_per_second"] for r in rows if r.get("input_rows_per_second")]
        stats.append(
            {
                "query": query,
                "batches": len(rows),
                "input_rows_per_second": round(rates[-1], 2) if rates else None,
                "batch_duration_ms_p50": statistics.median(durations) if durations else None,
                "batch_duration_ms_max": max(durations) if durations else None,
            }
        )
    return stats


def processed_offsets(records: list[dict], query: str = LAG_QUERY) -> dict[int, int]:
    """End offsets of the newest micro-batch of `query`, per wiki_edits partition."""
    for record in reversed(records):
        if record.get("query") != query:
            continue
        for source in record.get("sources") or []:
            end = source.get("end_offset")
            if isinstance(end, str):
                end = json.loads(end)
            if isinstance(end, dict) and TOPIC in end:
                return {int(p): int(o) for p, o in end[TOPIC].items()}
    return {}


def lag_rows(highs: dict[int, int], processed: dict[int, int]) -> list[dict]:
    rows = []
    for partition in sorted(highs):
        done = processed.get(partition)
        rows.append(
            {
                "partition": partition,
                "latest_offset": highs[partition],
                "processed_offset": done,
                "lag": None if done is None else max(highs[partition] - done, 0),
            }
        )
    return rows


def build_health(
    now: datetime,
    kafka: KafkaState,
    freshness_row: dict,
    funnel_row: dict,
    detection_row: dict,
    records: list[dict],
) -> dict:
    received = sum(sum(parts.values()) for parts in kafka.high_watermarks.values())
    samples = int(detection_row.get("samples") or 0)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso(now),
        "freshness": {
            layer: iso(freshness_row.get(layer)) for layer in ("bronze", "silver", "gold")
        },
        "funnel": {
            "received": received,
            "bronze": int(funnel_row.get("bronze") or 0),
            "silver_unique": int(funnel_row.get("silver_unique") or 0),
            "dlq": int(funnel_row.get("dlq") or 0),
        },
        "streaming": streaming_stats(records),
        "detection_seconds": {
            "samples": samples,
            "p50": round(detection_row["p50"], 1)
            if samples and detection_row.get("p50") is not None
            else None,
            "p95": round(detection_row["p95"], 1)
            if samples and detection_row.get("p95") is not None
            else None,
        },
        "lag": lag_rows(kafka.high_watermarks.get(TOPIC, {}), processed_offsets(records)),
    }


def lag_snapshot_key(now: datetime) -> str:
    """One key per logical run time: a re-run of the same interval overwrites it."""
    return f"{LAG_PREFIX}dt={now.date().isoformat()}/{now.strftime('%H%M%S')}.json"


def run_freshness(engine: Engine, store: Store, kafka: KafkaState, now: datetime) -> dict:
    """Write ops/lag_snapshots/<time>.json and dashboard/v1/health.json."""
    session = q.session_param(kafka.session_id)
    freshness_row = engine.run(q.FRESHNESS.render(now))[0]
    funnel_row = engine.run(q.FUNNEL.render(now, session_id=session))[0]
    detection_row = engine.run(q.DETECTION.render(now))[0]
    records = recent_progress(store, kafka.session_id, now)
    health = build_health(now, kafka, freshness_row, funnel_row, detection_row, records)
    store.put_json(
        lag_snapshot_key(now),
        {"generated_at": health["generated_at"], "session_id": session, "lag": health["lag"]},
    )
    write(store, "health", health)
    return health


def bootstrap_from_env(env: Mapping[str, str] | None = None) -> str:
    env = os.environ if env is None else env
    return env.get("KAFKA_BOOTSTRAP", "localhost:19092")
