"""Boot-time check: prove the cloud path works within minutes of a session starting.

Runs inside the Airflow container after `make up-airflow` (the boot script calls
`make boot-check`). It checks that both DAGs are unpaused, then runs each once with
`airflow dags test` and reads the run's final state:

- freshness_monitor: Athena (or Trino) answers, health.json is written to S3;
- dbt_gold: dbt builds Gold on Athena, and the snapshots are exported.

Each problem in the first cloud session (paused DAGs, a dbt test failing on Athena, manual
runs without a logical date) would have shown up here instead of 30 minutes in.
Prints one line per check and exits non-zero if any failed.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

DAGS = ("freshness_monitor", "dbt_gold")
TIMEOUT_S = 15 * 60


def airflow(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    # Fixed arguments from this file only; `airflow` is the image's own CLI on PATH.
    return subprocess.run(  # noqa: S603
        ["airflow", *args],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def json_rows(text: str) -> list[dict]:
    """The JSON array in Airflow CLI output (it may print log lines before it)."""
    match = re.search(r"^\[", text, re.M)  # log lines may contain "[info]" mid-line
    return json.loads(text[match.start() :]) if match else []


def paused(rows: list[dict]) -> set[str]:
    """Names of our DAGs that are paused (or missing, which is just as bad)."""
    found = {r.get("dag_id"): str(r.get("is_paused")).lower() for r in rows}
    return {d for d in DAGS if found.get(d, "missing") != "false"}


def latest_state(rows: list[dict]) -> str:
    """State of the newest run in `airflow dags list-runs -o json` output."""
    runs = sorted(rows, key=lambda r: r.get("start_date") or r.get("run_after") or "")
    return runs[-1].get("state", "unknown") if runs else "none"


def main() -> int:
    failures = []
    bad = paused(json_rows(airflow("dags", "list", "-o", "json").stdout))
    if bad:
        failures.append(f"paused or missing DAGs: {', '.join(sorted(bad))}")
    print(f"boot check: DAGs unpaused ... {'FAIL' if bad else 'ok'}", flush=True)

    for dag_id in DAGS:
        try:
            airflow("dags", "test", dag_id, timeout=TIMEOUT_S)
        except subprocess.TimeoutExpired:
            failures.append(f"{dag_id}: no result within {TIMEOUT_S // 60} minutes")
            print(f"boot check: {dag_id} ... FAIL (timeout)", flush=True)
            continue
        state = latest_state(json_rows(airflow("dags", "list-runs", dag_id, "-o", "json").stdout))
        if state != "success":
            failures.append(f"{dag_id}: run ended {state}")
        print(f"boot check: {dag_id} ... {'ok' if state == 'success' else 'FAIL'} ({state})",
              flush=True)  # fmt: skip

    if failures:
        print("BOOT CHECK FAILED: " + "; ".join(failures))
        print("Task logs: /opt/airflow/logs/dag_id=<dag>/ in the airflow container.")
        return 1
    print("BOOT CHECK PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
