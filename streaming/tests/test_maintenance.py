"""Lake maintenance: compacts closed partitions only, keeps every row, expires snapshots."""

from __future__ import annotations

from datetime import datetime, timedelta

from streaming.lib.maintenance import STREAM_TABLES, plan, run

NOW = datetime(2026, 10, 6, 12, 0)
# The Spark test commits in real time, and expire_snapshots compares against those real
# commit times, so its clock must stay ahead of the wall clock or nothing would expire.
FUTURE = datetime(2100, 1, 2, 12, 0)
TABLE = "lake.maint.events"


def test_plan_only_compacts_partitions_before_today():
    statements = plan("lake.bronze.wiki_edits_raw", "ingested_at", NOW).statements
    rewrite, manifests, expire = statements
    assert "rewrite_data_files(table => 'lake.bronze.wiki_edits_raw'" in rewrite
    assert "ingested_at < TIMESTAMP_NTZ '2026-10-06 00:00:00'" in rewrite
    assert "rewrite_manifests(table => 'lake.bronze.wiki_edits_raw')" in manifests
    assert "older_than => TIMESTAMP '2026-10-03 12:00:00', retain_last => 10" in expire


def test_only_stream_tables_are_maintained():
    """dbt's Gold tables are left alone: maintaining them could race a dbt run."""
    assert all("burst" not in t and "digest" not in t for t in STREAM_TABLES)
    assert len(STREAM_TABLES) == 5


def files_per_day(spark) -> dict[str, int]:
    rows = spark.sql(
        f"select partition.ts_day as day, count(*) as n from {TABLE}.files group by 1"
    ).collect()
    return {str(r["day"]): r["n"] for r in rows}


def test_compacts_yesterday_leaves_today_and_expires_snapshots(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.maint")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(
        f"CREATE TABLE {TABLE} (id INT, ts TIMESTAMP_NTZ) USING iceberg "
        "PARTITIONED BY (days(ts)) TBLPROPERTIES ('format-version' = '2')"
    )
    # One commit per insert, like the stream's micro-batches: many small files.
    for i in range(6):
        spark.sql(f"INSERT INTO {TABLE} VALUES ({i}, TIMESTAMP_NTZ '2100-01-01 0{i}:00:00')")
    # Today gets enough small files to qualify too, so only the date guard can spare them.
    for i in range(6, 12):
        spark.sql(f"INSERT INTO {TABLE} VALUES ({i}, TIMESTAMP_NTZ '2100-01-02 0{i - 6}:00:00')")
    assert files_per_day(spark) == {"2100-01-01": 6, "2100-01-02": 6}

    [summary] = run(spark, FUTURE, tables={TABLE: "ts"}, retention=timedelta(0), retain_last=1)

    assert files_per_day(spark) == {"2100-01-01": 1, "2100-01-02": 6}  # today untouched
    assert summary["rewrite_data_files"]["rewritten_data_files_count"] == 6
    assert spark.sql(f"select count(*) from {TABLE}").first()[0] == 12  # no row lost
    assert spark.sql(f"select count(*) from {TABLE}.snapshots").first()[0] == 1


def test_missing_tables_are_skipped(spark):
    [summary] = run(spark, NOW, tables={"lake.maint.nope": "ts"})
    assert summary == {"table": "lake.maint.nope", "skipped": "does not exist"}
