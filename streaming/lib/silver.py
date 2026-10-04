"""Silver dedup: insert-only MERGE on meta_id (invariant 7).

Why insert-only MERGE: with only ``WHEN NOT MATCHED THEN INSERT``, Iceberg runs the MERGE
as an anti-join plus an append. No existing data file is rewritten, so the cost stays
proportional to the micro-batch, not to the table.

Why the MERGE only scans the batch's own event hours: a duplicate is the same Wikimedia
event read twice, so it carries the same meta.dt as the original and sits in the same
event hour. Matching only those hour partitions is exact for live traffic, for resumes
after long gaps and for replays of old fixtures, and it scans as little as possible
(docs/adr/0005-silver-dedup-and-session-id.md).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .tables import SILVER_EDITS

HOUR_FORMAT = "yyyy-MM-dd HH:mm:ss"


def batch_hours(rows: DataFrame) -> list[str]:
    """Distinct UTC event hours in the batch, as 'yyyy-MM-dd HH:00:00' strings."""
    hours = rows.select(F.date_format(F.date_trunc("hour", "event_ts"), HOUR_FORMAT).alias("h"))
    return sorted(r.h for r in hours.distinct().collect())


def hour_filter(alias: str, hours: list[str]) -> str:
    """SQL predicate limiting ``alias.event_ts`` to the given hours (partition pruning)."""
    if not hours:
        raise ValueError("no hours to filter on")
    ranges = [
        f"({alias}.event_ts >= TIMESTAMP_NTZ '{h}' "
        f"AND {alias}.event_ts < TIMESTAMP_NTZ '{h}' + INTERVAL 1 HOUR)"
        for h in hours
    ]
    return "(" + " OR ".join(ranges) + ")"


def insert_new_rows(rows: DataFrame, table: str, key: str, stamp_column: str) -> int:
    """Insert-only MERGE of rows whose ``key`` is not already in ``table``.

    Rows are deduplicated on ``key`` within the batch first: an insert-only MERGE would
    otherwise insert both copies of a duplicate that arrives twice in one batch. The
    target is matched only in the batch's event hours (see module docstring). Re-running
    the same batch (Spark retries a failed foreachBatch) inserts nothing new. Returns the
    number of unique source rows.
    """
    unique = (
        rows.dropDuplicates([key])
        .withColumn(stamp_column, F.current_timestamp().cast("timestamp_ntz"))
        .persist()  # used three times: hours, MERGE, count
    )
    try:
        hours = batch_hours(unique)
        if not hours:
            return 0
        view = f"merge_source_{table.replace('.', '_')}"
        unique.createOrReplaceTempView(view)
        rows.sparkSession.sql(
            f"""
            MERGE INTO {table} t
            USING {view} s
            ON t.{key} = s.{key} AND {hour_filter("t", hours)}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return unique.count()
    finally:
        unique.unpersist()


def merge_into_silver(rows: DataFrame, table: str = SILVER_EDITS) -> int:
    """Insert Silver rows whose meta_id is not already in Silver (invariant 7)."""
    return insert_new_rows(rows, table, key="meta_id", stamp_column="processed_at")
