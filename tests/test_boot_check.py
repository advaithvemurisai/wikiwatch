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


def test_latest_state_is_the_newest_run():
    rows = [
        {"state": "failed", "start_date": "2026-10-06T03:00:00+00:00"},
        {"state": "success", "start_date": "2026-10-06T03:30:00+00:00"},
    ]
    assert boot_check.latest_state(rows) == "success"
    assert boot_check.latest_state([]) == "none"


def test_dbt_summary_reads_the_done_line():
    out = "04:31:26  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=5\n"
    assert boot_check.dbt_summary(out) == "PASS=5 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=5"
    assert boot_check.dbt_summary("crashed") == "no summary (dbt did not finish)"
