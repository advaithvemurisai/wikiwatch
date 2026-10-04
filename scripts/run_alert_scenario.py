"""Replay the scripted alert edits, check the exact alerts, and measure detection latency.

Each run gives every scripted event a fresh meta.id (so earlier runs never mask this one)
and stamps meta.dt with the current time, then publishes through the producer's publish
path. It waits for the streaming app to commit the alerts and compares them with
tests/e2e/fixtures/scripted_alert_edits.expected.json: same alerts, nothing extra.

Detection latency = detected_at (alert committed) - event_ts (edit time). Here edit time
is the publish time, so this measures our pipeline; Wikimedia's own delivery delay
(seconds) comes on top in production.

Usage: python scripts/run_alert_scenario.py [--timeout 300]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import trino

from wikiwatch_producer.pipeline import Pipeline
from wikiwatch_producer.publisher import KafkaPublisher
from wikiwatch_producer.sse import SSEEvent
from wikiwatch_producer.validate import EventValidator

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "tests/e2e/fixtures/scripted_alert_edits.jsonl"
EXPECTED = ROOT / "tests/e2e/fixtures/scripted_alert_edits.expected.json"
LOCALHOST_V4 = {"broker.address.family": "v4"}
TARGET_P95_SECONDS = 120


class NoCheckpoint:
    def load(self) -> None:
        return None

    def save(self, last_event_id: str) -> None:
        pass


def publish(run_id: str, bootstrap: str) -> None:
    """Send every scripted line with run-specific IDs and a current timestamp."""
    now = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    publisher = KafkaPublisher(bootstrap, extra_config=LOCALHOST_V4)
    pipeline = Pipeline(EventValidator(), publisher, NoCheckpoint(), checkpoint_every=1e9)
    for line in FIXTURE.read_text().splitlines():
        event = json.loads(line)
        event["meta"]["id"] = f"{event['meta']['id']}-{run_id}"
        event["meta"]["dt"] = now
        pipeline.handle(SSEEvent(data=json.dumps(event, ensure_ascii=False)))
    pipeline.checkpoint()


def fetch_alerts(cur, run_id: str) -> list[tuple]:
    cur.execute(
        "SELECT meta_id, rule_id, severity, "
        "date_diff('millisecond', event_ts, detected_at) / 1000.0 "
        "FROM gold.watched_page_alerts WHERE meta_id LIKE ?",
        (f"%-{run_id}",),
    )
    return cur.fetchall()


def run_once(cur, expected: set, bootstrap: str, timeout: float) -> tuple[dict, list[float]]:
    """Publish the scenario once and return (report, latencies in seconds)."""
    run_id = uuid.uuid4().hex[:8]
    publish(run_id, bootstrap)
    started = time.monotonic()
    rows: list[tuple] = []
    while time.monotonic() - started < timeout:
        rows = fetch_alerts(cur, run_id)
        if len(rows) >= len(expected):
            time.sleep(70)  # one more trigger: anything extra would show up now
            rows = fetch_alerts(cur, run_id)
            break
        time.sleep(5)
    actual = {(m.removesuffix(f"-{run_id}"), rule, sev) for m, rule, sev, _ in rows}
    report = {
        "run_id": run_id,
        "alerts": len(rows),
        "missing": sorted(expected - actual),
        "unexpected": sorted(actual - expected),
        "exact": actual == expected and len(rows) == len(expected),
    }
    return report, [float(r[3]) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument(
        "--runs", type=int, default=3, help="runs land at different points of the 1 min trigger"
    )
    args = parser.parse_args()

    expected = {
        (meta_id, rule, severity)
        for meta_id, case in json.loads(EXPECTED.read_text()).items()
        for rule, severity in case["alerts"]
    }
    cur = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8085")),
        user="alert-scenario",
        catalog="iceberg",
    ).cursor()

    reports, latencies = [], []
    for run in range(args.runs):
        if run:
            time.sleep(23)  # shift the next run's phase against the trigger
        report, run_latencies = run_once(cur, expected, args.bootstrap, args.timeout)
        reports.append(report)
        latencies.extend(run_latencies)
        print(json.dumps(report))

    latencies.sort()
    summary = {
        "runs": len(reports),
        "all_exact": all(r["exact"] for r in reports),
        "expected_alerts_per_run": len(expected),
    }
    if latencies:
        summary["latency_s"] = {
            "samples": len(latencies),
            "p50": round(statistics.median(latencies), 1),
            "p95": round(latencies[max(0, round(0.95 * len(latencies)) - 1)], 1),
            "max": round(latencies[-1], 1),
        }
    print(json.dumps(summary, indent=2))
    if summary["all_exact"] and latencies and summary["latency_s"]["p95"] < TARGET_P95_SECONDS:
        print("PASSED: exactly the expected alerts in every run, p95 detection under 2 minutes")
        return 0
    print("FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
