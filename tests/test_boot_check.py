"""airflow/tests/boot_check.py: parsing Airflow's CLI output (runs without Airflow)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("boot_check", ROOT / "airflow/tests/boot_check.py")
boot_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(boot_check)


def test_finds_the_json_after_log_lines():
    text = "2026-10-06 [info] loading dags\n" + json.dumps([{"dag_id": "dbt_gold"}])
    assert boot_check.json_rows(text) == [{"dag_id": "dbt_gold"}]
    assert boot_check.json_rows("no json here") == []


def test_paused_and_missing_dags_are_reported():
    rows = [{"dag_id": "dbt_gold", "is_paused": "False"},
            {"dag_id": "freshness_monitor", "is_paused": True}]  # fmt: skip
    assert boot_check.paused(rows) == {"freshness_monitor"}
    assert boot_check.paused(rows[:1]) == {"freshness_monitor"}  # missing counts as bad
    rows[1]["is_paused"] = False
    assert boot_check.paused(rows) == set()


def test_run_state_reads_only_the_boot_check_run():
    # The run unpausing started can fail beside the boot check's run (or the other way
    # round); only the triggered run counts.
    rows = [
        {"run_id": "scheduled__2026-10-14T03:30:00+00:00", "state": "failed"},
        {"run_id": "boot_check__20261014T033845Z", "state": "queued"},
    ]
    assert boot_check.run_state(rows, "boot_check__20261014T033845Z") == "queued"
    rows[1]["state"] = "success"
    assert boot_check.run_state(rows, "boot_check__20261014T033845Z") == "success"
    assert boot_check.run_state(rows, "boot_check__other") == "none"


def test_runs_go_through_the_scheduler(monkeypatch):
    calls, states = [], iter(["queued", "running", "success"])

    def fake_airflow(*args, timeout=120):
        calls.append(args)
        out = ""
        if args[:2] == ("dags", "list-runs"):
            out = json.dumps([{"run_id": calls[0][4], "state": next(states)}])
        return boot_check.subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

    monkeypatch.setattr(boot_check, "airflow", fake_airflow)
    monkeypatch.setattr(boot_check.time, "sleep", lambda _: None)
    assert boot_check.run_dag("dbt_gold") == "success"
    assert calls[0][:3] == ("dags", "trigger", "dbt_gold")
    assert calls[0][5] == "-l", "the export needs a logical date"
    assert not any(c[:2] == ("dags", "test") for c in calls), "dags test skips max_active_runs"


def test_a_failed_trigger_is_reported(monkeypatch):
    failed = boot_check.subprocess.CompletedProcess((), 1, stdout="", stderr="")
    monkeypatch.setattr(boot_check, "airflow", lambda *a, timeout=120: failed)
    assert boot_check.run_dag("dbt_gold") == "not triggered"


def test_dbt_summary_reads_the_done_line():
    out = "04:31:26  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=5\n"
    assert boot_check.dbt_summary(out) == "PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=5"
    assert boot_check.dbt_summary("crashed") == "no summary (dbt did not finish)"
