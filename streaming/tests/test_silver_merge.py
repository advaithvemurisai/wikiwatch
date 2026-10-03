"""Silver dedup on a real local Iceberg table."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from streaming.lib.silver import hour_filter, merge_into_silver
from streaming.lib.tables import SILVER_EDITS, create_tables
from streaming.lib.transform import to_silver


def batch(spark, *ids_and_times):
    """Kafka-shaped rows for (meta_id, meta.dt) pairs, run through the Silver transform."""
    rows = [
        (
            None,
            json.dumps(
                {
                    "meta": {"id": meta_id, "dt": dt},
                    "type": "edit",
                    "namespace": 0,
                    "title": "Acme",
                    "wiki": "enwiki",
                    "user": "U",
                    "bot": False,
                }
            ),
            0,
            i,
            datetime(2026, 10, 3, 13, 0, 0),
        )
        for i, (meta_id, dt) in enumerate(ids_and_times)
    ]
    schema = (
        "kafka_key string, raw string, kafka_partition int, kafka_offset long, "
        "ingested_at timestamp_ntz"
    )
    return to_silver(spark.createDataFrame(rows, schema))


@pytest.fixture
def silver_table(spark):
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_EDITS}")
    create_tables(spark, (SILVER_EDITS,))
    return SILVER_EDITS


def ids(spark, table):
    return sorted(r.meta_id for r in spark.table(table).collect())


def test_duplicates_across_two_batches_leave_one_row(spark, silver_table):
    merge_into_silver(batch(spark, ("a", "2026-10-03T12:00:00Z"), ("b", "2026-10-03T12:30:00Z")))
    merge_into_silver(batch(spark, ("b", "2026-10-03T12:30:00Z"), ("c", "2026-10-03T12:45:00Z")))
    assert ids(spark, silver_table) == ["a", "b", "c"]


def test_duplicates_inside_one_batch_leave_one_row(spark, silver_table):
    merge_into_silver(batch(spark, ("a", "2026-10-03T12:00:00Z"), ("a", "2026-10-03T12:00:00Z")))
    assert ids(spark, silver_table) == ["a"]


def test_replaying_old_events_still_dedups(spark, silver_table):
    """A fixture recorded days ago, replayed twice (Task 3 and the e2e test)."""
    old = [("x", "2026-09-01T08:15:00Z"), ("y", "2026-09-01T09:45:00Z")]
    merge_into_silver(batch(spark, *old))
    merge_into_silver(batch(spark, *old))
    assert ids(spark, silver_table) == ["x", "y"]


def test_rerunning_the_same_batch_is_idempotent(spark, silver_table):
    rows = batch(spark, ("a", "2026-10-03T12:00:00Z"))
    merge_into_silver(rows)
    merge_into_silver(rows)  # Spark retries a failed foreachBatch with the same data
    assert ids(spark, silver_table) == ["a"]


def test_rows_carry_processed_at(spark, silver_table):
    merge_into_silver(batch(spark, ("a", "2026-10-03T12:00:00Z")))
    assert spark.table(silver_table).collect()[0].processed_at is not None


def test_empty_batch_is_a_no_op(spark, silver_table):
    assert merge_into_silver(batch(spark)) == 0


def test_hour_filter_covers_each_hour_exactly():
    sql = hour_filter("t", ["2026-10-03 12:00:00"])
    assert sql == (
        "((t.event_ts >= TIMESTAMP_NTZ '2026-10-03 12:00:00' "
        "AND t.event_ts < TIMESTAMP_NTZ '2026-10-03 12:00:00' + INTERVAL 1 HOUR))"
    )


def test_hour_filter_rejects_empty_list():
    with pytest.raises(ValueError):
        hour_filter("t", [])
