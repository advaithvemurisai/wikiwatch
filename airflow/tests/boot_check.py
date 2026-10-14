"""Boot-time check: prove the cloud path works within minutes of a session starting.

Runs inside the Airflow container after `make up-airflow` (the boot script calls
`make boot-check`). It checks that both DAGs are unpaused, then triggers one run of each
through the scheduler and waits for that run's final state:

- freshness_monitor: Athena (or Trino) answers, health.json is written to S3;
- dbt_gold: dbt builds Gold on Athena, and the snapshots are exported;
- the dbt unit tests pass on this engine too (they run on Trino in CI; the scheduled
  build skips them, so this is where Athena-only differences show up).

The runs go through the scheduler, not `airflow dags test`, so max_active_runs=1 holds:
unpausing a DAG starts the run for its latest interval at once, and a test run beside it
would race it for dbt's target folder and the same tables. The boot check's run queues
behind that one instead.

Each problem in the first cloud session (paused DAGs, a dbt test failing on Athena, manual
runs without a logical date) would have shown up here instead of 30 minutes in.
Prints one line per check and exits non-zero if any failed.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime

DAGS = ("freshness_monitor", "dbt_gold")
UNIT_TARGET = "/tmp/dbt/unit"  # noqa: S108 - container-local scratch, like the DAG's target
TIMEOUT_S = 15 * 60
# A triggered run may queue behind the run that unpausing started, so allow for two runs.
RUN_TIMEOUT_S = 30 * 60
POLL_S = 15


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


def run_state(rows: list[dict], run_id: str) -> str:
    """State of one run in `airflow dags list-runs -o json` output."""
    states = [r.get("state", "unknown") for r in rows if r.get("run_id") == run_id]
    return states[0] if states else "none"


def run_dag(dag_id: str) -> str:
    """Trigger one run through the scheduler and wait for it; returns its final state."""
    now = datetime.now(UTC).replace(microsecond=0)
    run_id = f"boot_check__{now:%Y%m%dT%H%M%SZ}"
    # A logical date, because the export bounds its queries by it.
    if airflow("dags", "trigger", dag_id, "-r", run_id, "-l", now.isoformat()).returncode:
        return "not triggered"
    deadline = time.monotonic() + RUN_TIMEOUT_S
    while time.monotonic() < deadline:
        rows = json_rows(airflow("dags", "list-runs", dag_id, "-o", "json").stdout)
        state = run_state(rows, run_id)
        if state in ("success", "failed"):
            return state
        time.sleep(POLL_S)
    return "timeout"


def dbt_unit_tests() -> subprocess.CompletedProcess:
    """dbt's unit tests on the session's engine, in their own target folder."""
    dbt, project = os.environ.get("DBT_BIN", "dbt"), "/opt/wikiwatch/dbt"
    target = os.environ.get("DBT_TARGET", "local")
    return subprocess.run(  # noqa: S603 - fixed arguments; dbt is the image's own binary
        [dbt, "--no-use-colors", "test", "--select", "test_type:unit",
         "--project-dir", project, "--profiles-dir", project, "--target", target,
         "--target-path", UNIT_TARGET, "--log-path", f"{UNIT_TARGET}/logs"],
        capture_output=True, text=True, timeout=TIMEOUT_S, check=False,
    )  # fmt: skip


def dbt_summary(output: str) -> str:
    """dbt's 'Done. PASS=... ERROR=...' line, or a note that it is missing."""
    lines = [line for line in output.splitlines() if "Done. PASS=" in line]
    return lines[-1].split("Done. ", 1)[1].strip() if lines else "no summary (dbt did not finish)"


def main() -> int:
    failures = []
    bad = paused(json_rows(airflow("dags", "list", "-o", "json").stdout))
    if bad:
        failures.append(f"paused or missing DAGs: {', '.join(sorted(bad))}")
    print(f"boot check: DAGs unpaused ... {'FAIL' if bad else 'ok'}", flush=True)

    for dag_id in DAGS:
        # A paused DAG's run would stay queued until the timeout; it is already reported.
        state = "paused" if dag_id in bad else run_dag(dag_id)
        if state != "success":
            failures.append(f"{dag_id}: run ended {state}")
        print(f"boot check: {dag_id} ... {'ok' if state == 'success' else 'FAIL'} ({state})",
              flush=True)  # fmt: skip

    try:
        unit = dbt_unit_tests()
        summary = dbt_summary(unit.stdout)
        ok = unit.returncode == 0
    except subprocess.TimeoutExpired:
        summary, ok = "timeout", False
    if not ok:
        failures.append(f"dbt unit tests: {summary}")
    print(f"boot check: dbt unit tests ... {'ok' if ok else 'FAIL'} ({summary})", flush=True)

    if failures:
        print("BOOT CHECK FAILED: " + "; ".join(failures))
        print("Task logs: /opt/airflow/logs/dag_id=<dag>/ in the airflow container.")
        return 1
    print("BOOT CHECK PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
