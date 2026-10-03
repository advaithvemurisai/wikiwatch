"""Correctness checks on the lake through Trino (needs `make up-dbt`).

1. Silver has zero duplicate meta_id (invariant 7).
2. Bronze for the current session has no offset gaps or repeats per Kafka partition, and
   reaches Redpanda's high watermark (no lost or repeated records across Spark restarts).
3. Every Bronze event of the current session is in Silver.

Usage: python scripts/check_lake.py [--bootstrap localhost:19092] [--allow-lag N]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

import trino
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

LOCALHOST_V4 = {"broker.address.family": "v4"}


def query(cur, sql: str) -> list[tuple]:
    cur.execute(sql)
    return cur.fetchall()


def current_session(bootstrap: str) -> tuple[str, dict[int, int]]:
    """Session ID (as the Spark app derives it) and wiki_edits high watermarks."""
    admin = AdminClient({"bootstrap.servers": bootstrap, **LOCALHOST_V4})
    cluster_id = admin.describe_cluster().result().cluster_id
    session_id = re.sub(r"[^A-Za-z0-9-]+", "-", cluster_id).strip("-")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "check-lake", **LOCALHOST_V4})
    partitions = consumer.list_topics("wiki_edits", timeout=10).topics["wiki_edits"].partitions
    highs = {
        p: consumer.get_watermark_offsets(TopicPartition("wiki_edits", p), timeout=10)[1]
        for p in partitions
    }
    consumer.close()
    return session_id, highs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument(
        "--allow-lag", type=int, default=0, help="records Bronze may still be behind"
    )
    args = parser.parse_args()

    session_id, highs = current_session(args.bootstrap)
    cur = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8085")),
        user="check-lake",
        catalog="iceberg",
    ).cursor()
    session = session_id.replace("'", "")
    results: dict[str, object] = {"session_id": session_id}
    failures: list[str] = []

    rows, unique = query(cur, "SELECT count(*), count(DISTINCT meta_id) FROM silver.wiki_edits")[0]
    results["silver_rows"], results["silver_duplicates"] = rows, rows - unique
    if rows != unique:
        failures.append(f"Silver has {rows - unique} duplicate meta_id rows")

    per_partition = query(
        cur,
        f"""
        SELECT kafka_partition, min(kafka_offset), max(kafka_offset), count(*),
               count(DISTINCT kafka_offset)
        FROM bronze.wiki_edits_raw WHERE session_id = '{session}'
        GROUP BY kafka_partition ORDER BY kafka_partition
        """,  # noqa: S608 - session comes from the broker, quotes stripped
    )
    bronze = {}
    for partition, low, high, count, distinct in per_partition:
        bronze[partition] = {"min": low, "max": high, "rows": count}
        if count != distinct:
            failures.append(f"partition {partition}: {count - distinct} repeated offsets")
        if low != 0 or distinct != high - low + 1:
            failures.append(f"partition {partition}: offset gap (min {low}, max {high})")
        behind = highs.get(partition, 0) - (high + 1)
        if behind > args.allow_lag:
            failures.append(f"partition {partition}: Bronze is {behind} records behind")
    missing_partitions = [p for p, h in highs.items() if h > 0 and p not in bronze]
    if missing_partitions:
        failures.append(f"no Bronze rows for partitions {missing_partitions}")
    results["bronze_partitions"] = bronze
    results["redpanda_high_watermarks"] = highs

    not_in_silver = query(
        cur,
        f"""
        SELECT count(DISTINCT b.meta_id) FROM bronze.wiki_edits_raw b
        LEFT JOIN silver.wiki_edits s ON s.meta_id = b.meta_id
        WHERE b.session_id = '{session}' AND b.meta_id IS NOT NULL AND s.meta_id IS NULL
        """,  # noqa: S608
    )[0][0]
    results["bronze_events_missing_from_silver"] = not_in_silver
    if not_in_silver > args.allow_lag:
        failures.append(f"{not_in_silver} Bronze events are not in Silver")

    print(json.dumps(results, indent=2))
    if failures:
        print("FAILED:\n- " + "\n- ".join(failures))
        return 1
    print("PASSED: Silver unique, Bronze complete and gap-free, every event in Silver")
    return 0


if __name__ == "__main__":
    sys.exit(main())
