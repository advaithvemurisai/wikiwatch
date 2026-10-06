"""Iceberg table maintenance for the tables the stream writes (docs/v1.md: "a one-off
compaction script is enough").

The stream commits every minute, so each table gains about 60 small files and 60
snapshots an hour per query. Left alone, every Athena query (including the 5-minute
health check) plans over more files and manifests each session. Per table this:

1. compacts small data files, only in partitions from before today (UTC), so it never
   rewrites files the live stream may still be appending to or merging into;
2. rewrites manifests into fewer, larger ones;
3. expires snapshots older than the retention, keeping the newest few, which also deletes
   data files no snapshot references any more.

dbt's Gold tables are not touched: dbt commits only every 30 minutes, and maintaining
them could race a dbt run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from .tables import BRONZE_DLQ, BRONZE_EDITS, GOLD_ALERTS, GOLD_EDITS_PER_MIN, SILVER_EDITS

CATALOG = "lake"
RETENTION = timedelta(days=3)
RETAIN_LAST = 10

# Table -> its partition source column (see tables._PARTITIONING).
STREAM_TABLES = {
    BRONZE_EDITS: "ingested_at",
    BRONZE_DLQ: "ingested_at",
    SILVER_EDITS: "event_ts",
    GOLD_EDITS_PER_MIN: "window_start",
    GOLD_ALERTS: "event_ts",
}


@dataclass(frozen=True)
class Plan:
    table: str
    statements: tuple[str, ...]


def _ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def plan(
    table: str,
    column: str,
    now: datetime,
    retention: timedelta = RETENTION,
    retain_last: int = RETAIN_LAST,
) -> Plan:
    """The CALL statements for one table. `now` is naive UTC."""
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    name = table  # fully qualified: a short name makes Iceberg log a "catalog not found" warning
    closed = f"{column} < TIMESTAMP_NTZ '{_ts(today)}'"
    return Plan(
        table,
        (
            f"CALL {CATALOG}.system.rewrite_data_files(table => '{name}', "
            f'where => "{closed}", '
            "options => map('partial-progress.enabled', 'true', 'min-input-files', '5'))",
            f"CALL {CATALOG}.system.rewrite_manifests(table => '{name}')",
            f"CALL {CATALOG}.system.expire_snapshots(table => '{name}', "
            f"older_than => TIMESTAMP '{_ts(now - retention)}', retain_last => {retain_last})",
        ),
    )


def run(spark, now: datetime, tables: dict[str, str] | None = None, **plan_args) -> list[dict]:
    """Run maintenance on each table that exists. Returns one summary dict per table."""
    summaries = []
    for table, column in (tables or STREAM_TABLES).items():
        if not spark.catalog.tableExists(table):
            summaries.append({"table": table, "skipped": "does not exist"})
            continue
        summary: dict = {"table": table}
        for statement in plan(table, column, now, **plan_args).statements:
            row = spark.sql(statement).collect()
            procedure = statement.split("system.", 1)[1].split("(", 1)[0]
            summary[procedure] = row[0].asDict() if row else {}
        summaries.append(summary)
    return summaries
