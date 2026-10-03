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


def merge_into_silver(rows: DataFrame, table: str = SILVER_EDITS) -> int:
    """Insert rows whose meta_id is not already in Silver. Returns the source row count.

    Rows are deduplicated within the batch first: an insert-only MERGE would otherwise
    insert both copies of a duplicate that arrives twice in the same micro-batch.
    Re-running the same batch (Spark retries a failed foreachBatch) inserts nothing new.
    """
    unique = (
        rows.dropDuplicates(["meta_id"])
        .withColumn("processed_at", F.current_timestamp().cast("timestamp_ntz"))
        .persist()  # used three times: hours, MERGE, count
    )
    try:
        hours = batch_hours(unique)
        if not hours:
            return 0
        view = "silver_merge_source"
        unique.createOrReplaceTempView(view)
        rows.sparkSession.sql(
            f"""
            MERGE INTO {table} t
            USING {view} s
            ON t.meta_id = s.meta_id AND {hour_filter("t", hours)}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        return unique.count()
    finally:
        unique.unpersist()
