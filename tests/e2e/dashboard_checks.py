"""E2E part for Task 8: the real dashboard export and health check on the e2e stack.

Runs the same functions the Airflow tasks call, against Trino and SeaweedFS:
- export_dashboard writes alerts/baseline/meta (each validated against its schema),
- run_freshness writes health.json and a lag snapshot,
- every export and health query is EXPLAINed on Trino to prove each partitioned scan
  carries a pushed-down constraint on its partition column (not just a literal in SQL),
- optionally the same snapshots are written to web/fixtures/ for the site.
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from orchestration import queries as q
from orchestration.engines import TrinoEngine
from orchestration.freshness import (
    kafka_state,
    lag_rows,
    processed_offsets,
    recent_progress,
    run_freshness,
)
from orchestration.snapshots import export_dashboard
from orchestration.store import LocalStore, S3Store

ROOT = Path(__file__).resolve().parents[2]
WEB_FIXTURES = ROOT / "web" / "fixtures"


def _env_file_values(path: Path, *names: str) -> dict[str, str]:
    values = {}
    for line in path.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key in names:
            values[key] = value
    return values


def pruned_tables(engine: TrinoEngine, sql: str) -> dict[str, set[str]]:
    """table -> columns with a pushed-down constraint, from EXPLAIN (TYPE IO)."""
    plan = json.loads(engine.run(f"EXPLAIN (TYPE IO, FORMAT JSON) {sql}")[0]["Query Plan"])
    result: dict[str, set[str]] = {}
    for info in plan.get("inputTableColumnInfos", []):
        table = info["table"]["schemaTable"]
        name = f"{table['schema']}.{table['table']}"
        constraint = info.get("constraint") or {}
        columns = {c["columnName"] for c in constraint.get("columnConstraints", [])}
        result.setdefault(name, set()).update(columns)
    return result


class DashboardOnly(LocalStore):
    """web/fixtures/ holds only the dashboard snapshots, not ops files."""

    def put_json(self, key: str, document: dict) -> None:
        if key.startswith("dashboard/"):
            super().put_json(key, document)


def check_pruning(engine: TrinoEngine, now: datetime, session_id: str) -> tuple[int, list[str]]:
    """Return (partitioned scans verified, failures)."""
    failures, verified = [], 0
    for query in q.ALL_QUERIES:
        sql = query.render(now, session_id=session_id)
        for table, columns in pruned_tables(engine, sql).items():
            column = q.PARTITION_COLUMNS.get(table)
            if not column:
                continue
            verified += 1
            if column not in columns:
                failures.append(f"{query.name}: Trino does not prune {table} on {column}")
    if verified == 0:
        failures.append("EXPLAIN returned no partitioned scans: the pruning check checked nothing")
    return verified, failures


def run_dashboard_checks(env_file: Path, expected: dict, write_fixtures: bool) -> list[str]:
    creds = _env_file_values(env_file, "S3_ACCESS_KEY", "S3_SECRET_KEY")
    os.environ["AWS_ACCESS_KEY_ID"] = creds["S3_ACCESS_KEY"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["S3_SECRET_KEY"]
    engine = TrinoEngine("localhost", 8085)
    lake = S3Store("warehouse", "http://localhost:8333", "us-east-1")
    kafka = kafka_state("localhost:19092")
    failures: list[str] = []

    # The fixture's events are from its recording day: anchor the export there.
    flush = engine.run(
        "select max(event_ts) as latest from silver.wiki_edits "
        "where event_ts >= timestamp '2000-01-01 00:00:00'"
    )[0]["latest"]
    export_now = (flush + timedelta(minutes=1)).replace(microsecond=0)

    # Spark flushes progress once a minute: wait until it shows Silver caught up with
    # Redpanda, so health.json has real batch stats and lag (and lag must then be zero).
    deadline = time.monotonic() + 150
    caught_up = False
    while time.monotonic() < deadline and not caught_up:
        now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
        rows = lag_rows(
            kafka.high_watermarks.get("wiki_edits", {}),
            processed_offsets(recent_progress(lake, kafka.session_id, now)),
        )
        caught_up = bool(rows) and all(r["lag"] == 0 for r in rows)
        if not caught_up:
            time.sleep(10)
    real_now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)

    run_results = ROOT / "dbt" / "target" / "run_results.json"  # from the e2e dbt build
    export_dashboard(engine, lake, export_now, run_results)  # validates before writing
    run_freshness(engine, lake, kafka, real_now)
    if write_fixtures:
        # web/fixtures/ is exactly what the lake received (already validated).
        fixtures = DashboardOnly(WEB_FIXTURES)
        for name in ("alerts", "baseline", "health", "meta"):
            key = f"dashboard/v1/{name}.json"
            fixtures.put_json(key, json.loads(lake.get_text(key)))

    alerts = json.loads(lake.get_text("dashboard/v1/alerts.json"))
    got = {a["alert_id"] for a in alerts["alerts"]}
    want = {a["alert_id"] for a in expected["alerts"]}
    if got != want:
        failures.append(
            f"alerts.json differs: missing {sorted(want - got)}, extra {sorted(got - want)}"
        )

    health = json.loads(lake.get_text("dashboard/v1/health.json"))
    funnel = health["funnel"]
    want_bronze = expected["phase1_lines"] - expected["dlq_rows"] + expected["phase2_lines"]
    if funnel["bronze"] != want_bronze or funnel["dlq"] != expected["dlq_rows"]:
        failures.append(f"health funnel {funnel} does not match Bronze/DLQ")
    if funnel["received"] != funnel["bronze"] + funnel["dlq"]:
        failures.append(f"received {funnel['received']} != Bronze + DLQ")
    if not health["streaming"]:
        failures.append("health.json has no streaming stats (no Spark progress found)")
    if any(row["lag"] != 0 for row in health["lag"]):
        failures.append(f"consumer lag should be 0 once Spark caught up: {health['lag']}")
    if funnel["silver_unique"] != expected["unique_valid_events"]:
        failures.append(
            f"silver_unique {funnel['silver_unique']} != {expected['unique_valid_events']}"
        )

    verified, pruning_failures = check_pruning(engine, export_now, kafka.session_id)
    print(f"[e2e] partition pruning confirmed by EXPLAIN for {verified} partitioned scans")
    failures.extend(pruning_failures)
    return failures
