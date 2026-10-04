"""dbt_gold: every 30 minutes, build the Gold models with dbt, then export the dashboard.

1. dbt_build: `dbt build` (models + tests) on the target for this environment
   (local = Trino, aws = Athena). Incremental and partition-pruned (ADR 0008).
2. export_snapshots: alerts.json, baseline.json and meta.json to dashboard/v1/. Runs even
   when dbt fails, so meta.json reports the failure; the DAG run still shows as failed.

Idempotent: dbt merges, and the export overwrites the same three keys from queries bounded
by the run's logical time, so a re-run of the same interval writes the same snapshots.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DBT = os.environ.get("DBT_BIN", "dbt")
PROJECT = "/opt/wikiwatch/dbt"
# Container-local scratch; max_active_runs=1, so one run owns it at a time.
TARGET_PATH = "/tmp/dbt/target"  # noqa: S108


@dag(
    dag_id="dbt_gold",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["wikiwatch"],
    doc_md=__doc__,
)
def dbt_gold():
    build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT} --no-use-colors build --project-dir {PROJECT} --profiles-dir {PROJECT} "
            f'--target "$DBT_TARGET" --target-path {TARGET_PATH} --log-path {TARGET_PATH}/logs'
        ),
        append_env=True,
    )

    @task(trigger_rule="all_done")
    def export_snapshots(logical_date=None):
        from orchestration.engines import engine_from_env
        from orchestration.snapshots import export_dashboard
        from orchestration.store import store_from_env

        now = logical_date.astimezone(UTC).replace(tzinfo=None, microsecond=0)
        return export_dashboard(
            engine_from_env(),
            store_from_env(),
            now,
            run_results=Path(TARGET_PATH) / "run_results.json",
        )

    build >> export_snapshots()


dbt_gold()
