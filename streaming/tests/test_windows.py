"""edits_per_min as a real streaming query: watermark, append mode and dedup."""

from __future__ import annotations

import json

from streaming.lib.tables import GOLD_EDITS_PER_MIN, create_tables
from streaming.lib.windows import edits_per_minute

SILVER_SCHEMA = (
    "meta_id STRING, event_ts TIMESTAMP_NTZ, wiki STRING, edit_type STRING, "
    "is_bot BOOLEAN, byte_delta BIGINT"
)


def row(meta_id, ts, wiki="enwiki", edit_type="edit", bot=False, delta=10):
    return {
        "meta_id": meta_id,
        "event_ts": f"2026-10-03T{ts}",
        "wiki": wiki,
        "edit_type": edit_type,
        "is_bot": bot,
        "byte_delta": delta,
    }


def test_windows_dedup_drop_late_events_and_append_once(spark, tmp_path):
    source = tmp_path / "in"
    source.mkdir()
    stream = spark.readStream.schema(SILVER_SCHEMA).json(str(source))
    # Write to the real Iceberg table definition, exactly like the app does, so schema
    # and nullability mismatches fail here and not in the running stream.
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_EDITS_PER_MIN}")
    create_tables(spark, (GOLD_EDITS_PER_MIN,))
    query = (
        edits_per_minute(stream)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .option("fanout-enabled", "true")
        .toTable(GOLD_EDITS_PER_MIN)
    )

    def feed(name, rows):
        (source / f"{name}.json").write_text("".join(json.dumps(r) + "\n" for r in rows))
        query.processAllAvailable()

    try:
        feed(
            "1",
            [
                row("a", "12:00:10", delta=-300),
                row("a", "12:00:10", delta=-300),  # producer-resume duplicate
                row("b", "12:00:50", bot=True, delta=40),
                row("c", "12:00:20", edit_type="log", delta=None),  # not an edit
                row("d", "12:01:30", wiki="dewiki"),
            ],
        )
        feed("2", [row("e", "12:05:00")])  # watermark moves to 12:03:00
        feed("3", [row("late", "12:00:40")])  # behind the watermark: dropped
        feed("4", [row("f", "12:10:00")])  # moves the watermark past 12:05
        result = {
            (r.window_start.strftime("%H:%M"), r.wiki): (r.edits, r.bot_edits, r.bytes_changed)
            for r in spark.table(GOLD_EDITS_PER_MIN).collect()
        }
    finally:
        query.stop()

    assert result == {
        ("12:00", "enwiki"): (2, 1, 340),  # a once, b; c (log) and the late event excluded
        ("12:01", "dewiki"): (1, 0, 10),
        ("12:05", "enwiki"): (1, 0, 10),
    }
