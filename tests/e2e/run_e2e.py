"""End-to-end replay test: the real pipeline on a recorded fixture (make e2e, CI).

1. Start core + dbt as a separate Compose project (wikiwatch-e2e) with throwaway secrets
   and volumes, and a 10-second trigger instead of 1 minute.
2. Replay phase 1 through the producer's publish path; wait for Spark to process it.
3. Replay phase 2 (late events + a watermark flush); wait again.
4. Run dbt build (models + every dbt test) over the fixture's dates.
5. Assert: Silver unique, exactly the expected alert IDs, 1 DLQ row, Bronze complete,
   final windows = true counts, real-time windows = true counts minus the late events.
6. Tear everything down (unless --keep).

Usage: python tests/e2e/run_e2e.py [--keep]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import trino

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from init_env_local import render  # noqa: E402

from tests.e2e.expectations import expected_windows, minute_of, read_events  # noqa: E402

FIXTURES = ROOT / "tests/e2e/fixtures"
PROJECT = "wikiwatch-e2e"
ENV_FILE = ROOT / "tmp/.env.e2e"
PY = sys.executable
TIMEOUT_S = 600


def log(message: str) -> None:
    print(f"[e2e {time.strftime('%H:%M:%S')}] {message}", flush=True)


def run(*cmd: str, env: dict | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, env=env, check=check, text=True)


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run(
        "docker", "compose", "-p", PROJECT, "--env-file", str(ENV_FILE),
        "--profile", "core", "--profile", "dbt", *args, check=check,
    )  # fmt: skip


def dev_stack_running() -> bool:
    out = subprocess.run(
        ["docker", "ps", "-q", "--filter", "label=com.docker.compose.project=wikiwatch"],
        capture_output=True, text=True, check=True,
    )  # fmt: skip
    return bool(out.stdout.strip())


def write_env_file() -> None:
    """Throwaway secrets for this run only; deleted at teardown, never printed."""
    ENV_FILE.parent.mkdir(exist_ok=True)
    text = render((ROOT / ".env.example").read_text())
    text = text.replace("STREAM_TRIGGER=1 minute", "STREAM_TRIGGER=10 seconds")
    ENV_FILE.write_text(text)
    ENV_FILE.chmod(0o600)


def trino_cursor():
    return trino.dbapi.connect(host="localhost", port=8085, user="e2e", catalog="iceberg").cursor()


def scalar(sql: str):
    cur = trino_cursor()
    cur.execute(sql)
    return cur.fetchall()[0][0]


def wait_for(description: str, condition, timeout: float = TIMEOUT_S, hard: bool = True) -> None:
    """Poll until ``condition`` holds. A soft wait moves on at the timeout, so the
    assertions can say exactly what is missing instead of a generic timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition():
                log(f"ready: {description}")
                return
        except Exception as exc:  # noqa: BLE001 - tables may not exist yet
            last = type(exc).__name__
        else:
            last = "not yet"
        time.sleep(5)
    if hard:
        raise TimeoutError(f"timed out waiting for {description} ({last})")
    log(f"gave up waiting for {description}; the assertions will say what is missing")


def replay(path: Path) -> None:
    run(PY, "scripts/replay_fixture.py", "--file", str(path))


def assertions(expected: dict, phase1: list[dict], phase2: list[dict]) -> list[str]:
    failures: list[str] = []
    cur = trino_cursor()

    def rows(sql: str) -> list:
        cur.execute(sql)
        return cur.fetchall()

    def table_rows(table: str, columns: str) -> list:
        """Rows of a table, or [] plus a failure if the table was never built."""
        try:
            return rows(f"select {columns} from {table}")  # noqa: S608 - fixed names
        except trino.exceptions.TrinoUserError:
            failures.append(f"{table} does not exist (an upstream step failed to build it)")
            return []

    silver, distinct = rows("select count(*), count(distinct meta_id) from silver.wiki_edits")[0]
    if silver != distinct:
        failures.append(f"Silver has {silver - distinct} duplicate meta_id rows")
    if distinct != expected["unique_valid_events"]:
        failures.append(f"Silver has {distinct} events, expected {expected['unique_valid_events']}")

    dlq = rows("select count(*) from bronze.wiki_edits_dlq")[0][0]
    if dlq != expected["dlq_rows"]:
        failures.append(f"DLQ has {dlq} rows, expected {expected['dlq_rows']}")

    bronze = rows("select count(*) from bronze.wiki_edits_raw")[0][0]
    want_bronze = expected["phase1_lines"] - expected["dlq_rows"] + expected["phase2_lines"]
    if bronze != want_bronze:
        failures.append(f"Bronze has {bronze} rows, expected {want_bronze} (duplicates kept)")

    alert_columns = "alert_id, rule_id, severity"
    actual_alerts = {
        tuple(row)
        for table in ("gold.watched_page_alerts", "gold.burst_alerts")
        for row in table_rows(table, alert_columns)
    }
    want_alerts = {(a["alert_id"], a["rule_id"], a["severity"]) for a in expected["alerts"]}
    if actual_alerts != want_alerts:
        failures.append(
            f"alerts differ: missing {sorted(want_alerts - actual_alerts)}, "
            f"unexpected {sorted(actual_alerts - want_alerts)}"
        )

    final, realtime, final_closed = expected_windows(phase1, phase2, expected["flush_meta_id"])
    got_final = {
        (w, wiki): (e, b, c)
        for w, wiki, e, b, c in table_rows(
            "gold.edits_per_min_final", "window_start, wiki, edits, bot_edits, bytes_changed"
        )
    }
    if got_final != final:
        failures.append(
            f"final windows differ in {len(set(got_final.items()) ^ set(final.items()))} entries"
        )
    got_realtime = {
        (w, wiki): (e, b, c)
        for w, wiki, e, b, c in rows(
            "select window_start, wiki, edits, bot_edits, bytes_changed from gold.edits_per_min"
        )
    }
    if got_realtime != realtime:
        failures.append(
            f"real-time windows differ in {len(set(got_realtime.items()) ^ set(realtime.items()))}"
            " entries (expected: true counts minus the late events)"
        )
    late_dropped = sum(v[0] for v in final_closed.values()) - sum(
        v[0] for v in got_realtime.values()
    )
    log(
        f"windows: {len(got_final)} final, {len(got_realtime)} real-time, "
        f"{late_dropped} late edits dropped by the watermark"
    )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", action="store_true", help="leave the e2e stack running")
    parser.add_argument(
        "--no-build", action="store_true", help="use images already built (CI builds with cache)"
    )
    parser.add_argument(
        "--write-web-fixtures",
        action="store_true",
        help="also write the exported snapshots to web/fixtures/ (to refresh them)",
    )
    args = parser.parse_args()

    if dev_stack_running():
        print("The dev stack (project 'wikiwatch') is running and uses the same ports.")
        print("Stop it first with: make down")
        return 2

    expected = json.loads((FIXTURES / "e2e_expected.json").read_text())
    phase1 = read_events(FIXTURES / "e2e_phase1.jsonl.gz")
    phase2 = read_events(FIXTURES / "e2e_phase2.jsonl.gz")
    phase1_unique = len({e["meta"]["id"] for e in phase1 if "title" in e})
    started = time.monotonic()
    write_env_file()
    try:
        log("starting core + dbt (project wikiwatch-e2e)")
        compose("down", "-v", "--remove-orphans", check=False)
        compose("up", "-d", "--wait", *([] if args.no_build else ["--build"]))

        log("phase 1: replaying the fixture through the producer's publish path")
        replay(FIXTURES / "e2e_phase1.jsonl.gz")
        wait_for(
            "Silver has every phase-1 event",
            lambda: (
                scalar("select count(distinct meta_id) from silver.wiki_edits") >= phase1_unique
            ),
        )
        stream_alerts = sum(a["rule_id"] != "R5" for a in expected["alerts"])
        wait_for(
            f"the {stream_alerts} stream alerts",
            lambda: scalar("select count(*) from gold.watched_page_alerts") >= stream_alerts,
            timeout=120,
            hard=False,
        )
        # The windows query runs on its own trigger: wait until its watermark has moved
        # past phase 1 (latest minute - 2 min watermark), so the late events in phase 2
        # really arrive behind it.
        latest = max(minute_of(e) for e in phase1 if "title" in e)
        wait_for(
            "the real-time windows have caught up with phase 1",
            lambda: (
                (scalar("select max(window_start) from gold.edits_per_min") or latest)
                >= latest - timedelta(minutes=3)
            ),
        )
        log("phase 2: late events + watermark flush")
        replay(FIXTURES / "e2e_phase2.jsonl.gz")
        wait_for(
            "Silver has every event and the real-time windows are closed",
            lambda: (
                scalar("select count(distinct meta_id) from silver.wiki_edits")
                == expected["unique_valid_events"]
                and scalar("select count(*) from gold.edits_per_min")
                >= len(expected_windows(phase1, phase2, expected["flush_meta_id"])[1])
            ),
        )

        log("dbt build over the fixture's dates")
        dbt_vars = json.dumps(
            {
                "event_time_floor": "2000-01-01 00:00:00",
                "test_window_days": 36500,
                "in_flight_minutes": 0,
            }
        )
        env = {**os.environ, "DBT_PROFILES_DIR": str(ROOT / "dbt")}
        dbt = str(Path(PY).parent / "dbt")
        dbt_run = run(
            dbt,
            "--no-use-colors",
            "build",
            "--project-dir",
            "dbt",
            "--target",
            "local",
            "--vars",
            dbt_vars,
            env=env,
            check=False,  # keep going: the assertions below explain what is wrong
        )

        log("assertions")
        failures = assertions(expected, phase1, phase2)
        if dbt_run.returncode:
            failures.append("dbt build failed (see the dbt output above for the failing test)")
        log("dashboard export, health and partition pruning (Task 8)")
        from tests.e2e.dashboard_checks import run_dashboard_checks

        failures.extend(run_dashboard_checks(ENV_FILE, expected, args.write_web_fixtures))
        lake = run(PY, "scripts/check_lake.py", check=False)
        if lake.returncode:
            failures.append("check_lake.py failed (Bronze gaps or Silver completeness)")
    finally:
        if not args.keep:
            log("tearing down")
            compose("down", "-v", "--remove-orphans", check=False)
            ENV_FILE.unlink(missing_ok=True)

    minutes = (time.monotonic() - started) / 60
    if failures:
        print("\nE2E FAILED:\n- " + "\n- ".join(failures))
        return 1
    print(
        f"\nE2E PASSED in {minutes:.1f} min: Silver unique, exact alerts, 1 DLQ row, windows exact"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
