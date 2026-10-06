"""freshness_monitor: every 5 minutes, write dashboard/v1/health.json and a lag snapshot.

health.json holds layer freshness, the record funnel for the current session, streaming
batch stats, detection latency and consumer lag per partition. Its generated_at is the
pipeline heartbeat behind the site's offline banner.

Idempotent: everything is computed for the run's logical time; the lag snapshot key is
that time, so a re-run overwrites the same object.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="freshness_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["wikiwatch"],
    doc_md=__doc__,
)
def freshness_monitor():
    @task
    def write_health(logical_date=None, dag_run=None):
        from orchestration.engines import engine_from_env
        from orchestration.freshness import bootstrap_from_env, kafka_state, run_freshness
        from orchestration.runtime import run_time
        from orchestration.store import store_from_env

        now = run_time(logical_date, dag_run.run_after if dag_run else None)
        health = run_freshness(
            engine_from_env(), store_from_env(), kafka_state(bootstrap_from_env()), now
        )
        return health["funnel"]

    write_health()


freshness_monitor()
