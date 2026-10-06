"""Build, validate and write the dashboard snapshots in dashboard/v1/ (docs/v1.md).

The snapshot JSON is a data contract like the event schema: every document carries
schema_version and is validated against schemas/dashboard/ before it is written. An
invalid or oversized document is never written, so the site keeps serving the last good
one instead of breaking.

alerts.json and baseline.json are exported independently: a failing query (for example
a table dbt could not build) affects only its own snapshot. meta.json is always written
and records which exports failed, so the site can say why a section is stale.
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from . import queries as q
from .engines import Engine
from .store import Store, to_json

SCHEMA_VERSION = 1
PREFIX = "dashboard/v1/"
MAX_BYTES = 1_000_000
SCHEMA_DIR = Path(
    os.environ.get(
        "DASHBOARD_SCHEMA_DIR", Path(__file__).resolve().parents[1] / "schemas" / "dashboard"
    )
)


log = logging.getLogger(__name__)


class SnapshotError(ValueError):
    """A snapshot failed its schema or size limit; nothing was written."""


class ExportError(RuntimeError):
    """One or more snapshots could not be exported (meta.json says which)."""


def iso(value: datetime | date | None) -> str | None:
    """UTC ISO-8601 with a Z suffix for timestamps, plain ISO for dates."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value.isoformat(timespec="seconds") + "Z"
    return value.isoformat()


def _validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_DIR / f"{name}.schema.json").read_text())
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def validate(name: str, document: dict) -> None:
    """Raise SnapshotError if the document breaks its schema or the 1 MB cap."""
    errors = sorted(_validator(name).iter_errors(document), key=lambda e: list(e.path))
    if errors:
        first = errors[0]
        where = "/".join(str(p) for p in first.absolute_path) or "<root>"
        raise SnapshotError(f"{name}.json invalid at {where}: {first.validator}")
    size = len(to_json(document))
    if size > MAX_BYTES:
        raise SnapshotError(f"{name}.json is {size} bytes, over the {MAX_BYTES} byte cap")


def write(store: Store, name: str, document: dict) -> None:
    validate(name, document)
    store.put_json(f"{PREFIX}{name}.json", document)


# ---------------------------------------------------------------- builders


def build_alerts(now: datetime, alert_rows: list[dict], digest_rows: list[dict]) -> dict:
    alerts = [
        {
            "alert_id": r["alert_id"],
            "rule_id": r["rule_id"],
            "rule_name": r["rule_name"],
            "severity": r["severity"],
            "wiki": r["wiki"],
            "title": r["title"],
            "category": r["category"],
            "owner_team": r["owner_team"],
            "edit_type": r["edit_type"],
            "log_action": r["log_action"],
            "editor_type": r["editor_type"],
            "byte_delta": r["byte_delta"],
            "edits_in_window": r["edits_in_window"],
            "event_ts": iso(r["event_ts"]),
            "detected_at": iso(r["detected_at"]),
        }
        for r in alert_rows[:500]
    ]
    digest = [
        {
            **{k: int(r[k]) for k in DIGEST_COUNTS},
            **{k: r[k] for k in ("wiki", "title", "category")},
            "digest_date": iso(r["digest_date"]),
        }
        for r in digest_rows[:500]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso(now),
        "window_days": 7,
        "alerts": alerts,
        "digest": digest,
    }


DIGEST_COUNTS = (
    "edits",
    "log_events",
    "net_bytes",
    "registered_edits",
    "unregistered_edits",
    "bot_edits",
    "r1_alerts",
    "r2_alerts",
    "r3_alerts",
    "r4_alerts",
    "r5_alerts",
)


def build_baseline(
    now: datetime,
    final_rows: list[dict],
    realtime_rows: list[dict],
    bot_rows: list[dict],
    hour_rows: list[dict],
    page_rows: list[dict],
    watchlist_rows: list[dict],
) -> dict:
    final = {r["minute"]: int(r["edits"]) for r in final_rows}
    realtime = {r["minute"]: int(r["edits"]) for r in realtime_rows}
    minutes = sorted(set(final) | set(realtime))[-400:]
    recorded = [r["hour"] for r in hour_rows][-200:]

    totals: dict[str, int] = defaultdict(int)
    for r in page_rows:
        totals[r["title"]] += int(r["edits"])
    hours = max(len(recorded), 1)
    averages = [
        {
            "title": w["title"],
            "category": w["category"],
            "total_edits": totals.get(w["title"], 0),
            "avg_edits_per_recorded_hour": round(totals.get(w["title"], 0) / hours, 3),
        }
        for w in watchlist_rows[:100]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso(now),
        "edits_per_min": [
            {"minute": iso(m), "realtime_edits": realtime.get(m), "final_edits": final.get(m)}
            for m in minutes
        ],
        "bot_share_hourly": [
            {
                "hour": iso(r["hour"]),
                "edits": int(r["edits"]),
                "bot_edits": int(r["bot_edits"]),
                "bot_share": round(int(r["bot_edits"]) / int(r["edits"]), 4) if r["edits"] else 0,
            }
            for r in bot_rows[-200:]
        ],
        "recorded_hours": [iso(h) for h in recorded],
        "page_activity": [
            {
                "title": r["title"],
                "category": r["category"],
                "hour": iso(r["hour"]),
                "edits": int(r["edits"]),
            }
            for r in page_rows[:3000]
        ],
        "page_averages": averages,
    }


def dbt_status(run_results: Path | None) -> dict[str, Any]:
    """Summarise dbt's run_results.json (status, finish time, counts)."""
    if run_results is None or not run_results.exists():
        return {"status": "unknown", "finished_at": None, "models_ok": 0, "tests_ok": 0,
                "failures": 0}  # fmt: skip
    data = json.loads(run_results.read_text())
    models_ok = tests_ok = failures = 0
    for result in data.get("results", []):
        status = result.get("status")
        is_model = result.get("unique_id", "").startswith("model.")
        if status in ("error", "fail", "runtime error"):
            failures += 1
        elif is_model and status == "success":
            models_ok += 1
        elif not is_model and status == "pass":
            tests_ok += 1
    finished = data.get("metadata", {}).get("generated_at")
    finished_at = iso(datetime.fromisoformat(finished.replace("Z", "+00:00"))) if finished else None
    return {
        "status": "failed" if failures else "success",
        "finished_at": finished_at,
        "models_ok": models_ok,
        "tests_ok": tests_ok,
        "failures": failures,
    }


def build_meta(
    now: datetime,
    dbt: dict,
    alerts: dict | None,
    baseline: dict | None,
    exports: dict[str, dict] | None = None,
) -> dict:
    """meta.json: the dbt result, row counts (null for a snapshot that failed to export)
    and, when given, the status of each export."""
    meta = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso(now),
        "dbt": dbt,
        "snapshots": {
            "alerts": len(alerts["alerts"]) if alerts else None,
            "digest": len(alerts["digest"]) if alerts else None,
            "edits_per_min": len(baseline["edits_per_min"]) if baseline else None,
            "bot_share_hourly": len(baseline["bot_share_hourly"]) if baseline else None,
            "page_activity": len(baseline["page_activity"]) if baseline else None,
        },
    }
    if exports is not None:
        meta["exports"] = exports
    return meta


class _Rows:
    """Runs export queries on demand and remembers which one was running last."""

    def __init__(self, engine: Engine, now: datetime) -> None:
        self.engine, self.now, self.current = engine, now, None

    def __call__(self, query: q.Query) -> list[dict]:
        self.current = query.name
        return self.engine.run(query.render(self.now))


def _export(name: str, rows: _Rows, store: Store, build) -> tuple[dict | None, dict]:
    """Build, validate and write one snapshot. Returns (document or None, status)."""
    rows.current = None
    try:
        document = build()
        rows.current = None  # from here on a failure is the document's, not a query's
        write(store, name, document)
    except Exception as exc:  # noqa: BLE001 - recorded in meta.json, re-raised by the caller
        # Full detail only in the task log: Athena messages can name buckets or paths.
        log.error("%s.json export failed (query %s): %s", name, rows.current, exc)
        return None, {"status": "failed", "failed_query": rows.current}
    return document, {"status": "ok", "failed_query": None}


def export_dashboard(
    engine: Engine, store: Store, now: datetime, run_results: Path | None = None
) -> dict[str, int | None]:
    """Export alerts.json and baseline.json independently, then always write meta.json.

    A snapshot that fails (a query error, or an invalid or oversized document) is not
    written, so the site keeps its last good copy; meta.json records the failure. Raises
    ExportError after writing meta.json if any export failed, so the Airflow task fails.
    """
    rows = _Rows(engine, now)
    alerts, alerts_status = _export(
        "alerts", rows, store, lambda: build_alerts(now, rows(q.ALERTS), rows(q.DIGEST))
    )
    baseline, baseline_status = _export(
        "baseline",
        rows,
        store,
        lambda: build_baseline(
            now,
            rows(q.EDITS_PER_MIN_FINAL),
            rows(q.EDITS_PER_MIN_REALTIME),
            rows(q.BOT_SHARE_HOURLY),
            rows(q.RECORDED_HOURS),
            rows(q.PAGE_ACTIVITY),
            rows(q.WATCHLIST),
        ),
    )
    exports = {"alerts": alerts_status, "baseline": baseline_status}
    meta = build_meta(now, dbt_status(run_results), alerts, baseline, exports)
    write(store, "meta", meta)
    failed = sorted(name for name, status in exports.items() if status["status"] == "failed")
    if failed:
        raise ExportError(f"export failed for {', '.join(failed)} (see meta.json and the log)")
    return meta["snapshots"]
