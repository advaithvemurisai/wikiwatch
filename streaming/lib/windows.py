"""Real-time edits per minute per wiki (invariant 8: 1-minute tumbling, 2-minute watermark).

Append mode writes each window once, after the watermark passes its end, so the table
never needs updates (no MERGE, no delete files on the hottest Gold table). Events later
than the watermark are dropped here on purpose; dbt's edits_per_min_final (Task 5) counts
them from Silver, and the gap between the two is the measured cost of low latency.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

WINDOW = "1 minute"
WATERMARK = "2 minutes"
COUNTED_TYPES = ("edit", "new")  # page changes; log and categorize events are not edits


def edits_per_minute(silver_rows: DataFrame) -> DataFrame:
    """Aggregate Silver-shaped rows into 1-minute windows per wiki.

    Duplicates from producer resumes are removed with dropDuplicatesWithinWatermark on
    meta_id: a duplicate always carries the same event time, so it arrives within the
    watermark of the original and would otherwise be counted twice.
    """
    events = (
        silver_rows.where(F.col("edit_type").isin(*COUNTED_TYPES))
        .withColumn("event_time", F.col("event_ts").cast("timestamp"))
        .withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["meta_id"])
    )
    counts = events.groupBy(F.window("event_time", WINDOW), "wiki").agg(
        F.count(F.lit(1)).alias("edits"),
        F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("bot_edits"),
        F.coalesce(F.sum(F.abs("byte_delta")), F.lit(0)).alias("bytes_changed"),
    )
    return counts.select(
        F.col("window.start").cast("timestamp_ntz").alias("window_start"),
        F.col("window.end").cast("timestamp_ntz").alias("window_end"),
        "wiki",
        F.col("edits").cast("long"),
        F.col("bot_edits").cast("long"),
        F.col("bytes_changed").cast("long"),
    )
