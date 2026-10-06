"""DAG import test, run inside the real Airflow image (make test-dags, CI).

Parses airflow/dags/ with Airflow's own DagBag and checks the contract of each DAG:
no import errors, the schedule, no catchup, one run at a time, and the task layout.
"""

from __future__ import annotations

import sys

from airflow.dag_processing.dagbag import DagBag  # Airflow 3.3 location

EXPECTED = {
    "dbt_gold": ("*/30 * * * *", {"dbt_build", "export_snapshots"}),
    "freshness_monitor": ("*/5 * * * *", {"write_health"}),
}


def main() -> int:
    bag = DagBag(dag_folder="/opt/airflow/dags")  # examples off via AIRFLOW__CORE__LOAD_EXAMPLES
    problems = [
        f"import error in {path}: {err.splitlines()[-1]}" for path, err in bag.import_errors.items()
    ]
    if set(bag.dag_ids) != set(EXPECTED):
        problems.append(f"DAG ids {sorted(bag.dag_ids)}, expected {sorted(EXPECTED)}")
    for dag_id, (schedule, tasks) in EXPECTED.items():
        dag = bag.dags.get(dag_id)
        if dag is None:
            continue
        timetable = getattr(dag.timetable, "summary", str(dag.timetable))
        if schedule not in str(timetable):
            problems.append(f"{dag_id}: schedule {timetable!r}, expected {schedule!r}")
        if dag.catchup:
            problems.append(f"{dag_id}: catchup must be off")
        if dag.max_active_runs != 1:
            problems.append(f"{dag_id}: max_active_runs must be 1")
        if set(dag.task_ids) != tasks:
            problems.append(f"{dag_id}: tasks {sorted(dag.task_ids)}, expected {sorted(tasks)}")
    gold = bag.dags.get("dbt_gold")
    if gold is not None:
        export = gold.get_task("export_snapshots")
        if str(export.trigger_rule) not in ("all_done", "TriggerRule.ALL_DONE"):
            problems.append("dbt_gold.export_snapshots must run even when dbt fails (all_done)")
        if "dbt_build" not in export.upstream_task_ids:
            problems.append("dbt_gold.export_snapshots must run after dbt_build")
        build_cmd = gold.get_task("dbt_build").bash_command
        if "--exclude-resource-type unit_test" not in build_cmd:
            problems.append("dbt_gold.dbt_build must exclude unit tests (they run in CI)")
    for problem in problems:
        print("FAIL:", problem)
    if not problems:
        print(f"OK: {len(bag.dag_ids)} DAGs import cleanly and match their contract")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
