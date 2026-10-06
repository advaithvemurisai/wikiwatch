"""Snapshot builders, the schema contract, and the export's all-or-nothing write."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from orchestration import snapshots as s
from orchestration.queries import EXPORT_QUERIES
from orchestration.store import LocalStore

NOW = datetime(2026, 10, 4, 12, 30, 0)


def alert_row(i=0, **overrides):
    row = {
        "alert_id": f"{i:064x}",
        "rule_id": "R2",
        "rule_name": "Large removal",
        "severity": "high",
        "wiki": "enwiki",
        "title": "Unilever",
        "category": "own_brand",
        "owner_team": "corporate-comms",
        "edit_type": "edit",
        "log_action": None,
        "editor_type": "unregistered",
        "byte_delta": -9000,
        "edits_in_window": None,
        "event_ts": datetime(2026, 10, 4, 11, 0, 5),
        "detected_at": datetime(2026, 10, 4, 11, 1, 2),
    }
    row.update(overrides)
    return row


def digest_row(**overrides):
    row = {k: 0 for k in s.DIGEST_COUNTS}
    row.update(
        {
            "digest_date": date(2026, 10, 4),
            "wiki": "enwiki",
            "title": "Unilever",
            "category": "own_brand",
            "edits": 3,
            "net_bytes": -9000,
        }  # fmt: skip
    )
    row.update(overrides)
    return row


def baseline_doc():
    m1, m2 = datetime(2026, 10, 4, 12, 0), datetime(2026, 10, 4, 12, 1)
    return s.build_baseline(
        NOW,
        final_rows=[{"minute": m1, "edits": 40}, {"minute": m2, "edits": 42}],
        realtime_rows=[{"minute": m1, "edits": 38}],
        bot_rows=[{"hour": datetime(2026, 10, 4, 12), "edits": 100, "bot_edits": 25}],
        hour_rows=[{"hour": datetime(2026, 10, 4, 11)}, {"hour": datetime(2026, 10, 4, 12)}],
        page_rows=[
            {
                "title": "Unilever",
                "category": "own_brand",
                "hour": datetime(2026, 10, 4, 12),
                "edits": 4,
            }
        ],  # fmt: skip
        watchlist_rows=[
            {"title": "Unilever", "category": "own_brand"},
            {"title": "Marmite", "category": "product"},
        ],  # fmt: skip
    )


def test_alerts_snapshot_is_valid_and_has_no_user_names():
    doc = s.build_alerts(NOW, [alert_row()], [digest_row()])
    s.validate("alerts", doc)
    assert doc["alerts"][0]["event_ts"] == "2026-10-04T11:00:05Z"
    assert "user" not in json.dumps(doc)  # editor type only, never a name


def test_alerts_are_capped_at_500():
    doc = s.build_alerts(NOW, [alert_row(i) for i in range(600)], [])
    assert len(doc["alerts"]) == 500
    s.validate("alerts", doc)


def test_baseline_merges_realtime_and_final_and_averages_per_recorded_hour():
    doc = baseline_doc()
    s.validate("baseline", doc)
    assert doc["edits_per_min"][1] == {
        "minute": "2026-10-04T12:01:00Z", "realtime_edits": None, "final_edits": 42}  # fmt: skip
    averages = {a["title"]: a for a in doc["page_averages"]}
    assert averages["Unilever"]["avg_edits_per_recorded_hour"] == 2.0  # 4 edits / 2 hours
    assert averages["Marmite"]["total_edits"] == 0
    assert doc["bot_share_hourly"][0]["bot_share"] == 0.25


def test_wrong_schema_version_is_rejected():
    doc = s.build_alerts(NOW, [], [])
    doc["schema_version"] = 2
    with pytest.raises(s.SnapshotError, match="schema_version"):
        s.validate("alerts", doc)


def test_unknown_fields_are_rejected():
    """additionalProperties is false everywhere: nothing unplanned (a user name, a bucket
    name) can ride along into a public snapshot."""
    doc = s.build_alerts(NOW, [alert_row()], [])
    with pytest.raises(s.SnapshotError):
        s.validate("alerts", {**doc, "bucket": "never-on-a-page"})
    doc["alerts"][0]["user_name"] = "SomeEditor"
    with pytest.raises(s.SnapshotError):
        s.validate("alerts", doc)


def test_oversized_snapshot_is_rejected(monkeypatch):
    monkeypatch.setattr(s, "MAX_BYTES", 100)
    with pytest.raises(s.SnapshotError, match="byte cap"):
        s.validate("alerts", s.build_alerts(NOW, [alert_row()], []))


def test_dbt_status_from_run_results(tmp_path):
    results = tmp_path / "run_results.json"
    results.write_text(
        json.dumps(
            {
                "metadata": {"generated_at": "2026-10-04T12:29:58.123456Z"},
                "results": [
                    {"unique_id": "model.wikiwatch.burst_alerts", "status": "success"},
                    {"unique_id": "test.wikiwatch.unique_x", "status": "pass"},
                    {"unique_id": "test.wikiwatch.reconcile", "status": "fail"},
                ],
            }
        )
    )
    assert s.dbt_status(results) == {
        "status": "failed", "finished_at": "2026-10-04T12:29:58Z",
        "models_ok": 1, "tests_ok": 1, "failures": 1}  # fmt: skip
    assert s.dbt_status(None)["status"] == "unknown"


class FakeEngine:
    def __init__(self, rows_by_marker):
        self.rows_by_marker = rows_by_marker
        self.sql = []

    def run(self, sql):
        self.sql.append(sql)
        for marker, rows in self.rows_by_marker.items():
            if marker in sql:
                return rows
        return []


def fake_rows(bad_alert=False):
    alert = alert_row(severity="critical") if bad_alert else alert_row()
    return {
        "from gold.watched_page_alerts": [alert],
        "from gold.watchlist_daily_digest": [digest_row()],
        "from ref.watchlist order by title": [{"title": "Unilever", "category": "own_brand"}],
    }


def test_export_writes_three_valid_snapshots(tmp_path):
    engine = FakeEngine(fake_rows())
    counts = s.export_dashboard(engine, LocalStore(tmp_path), NOW)
    assert len(engine.sql) == len(EXPORT_QUERIES)
    for name in ("alerts", "baseline", "meta"):
        s.validate(name, json.loads((tmp_path / f"dashboard/v1/{name}.json").read_text()))
    assert counts["alerts"] == 1
    meta = json.loads((tmp_path / "dashboard/v1/meta.json").read_text())
    assert meta["exports"] == {
        "alerts": {"status": "ok", "failed_query": None},
        "baseline": {"status": "ok", "failed_query": None},
    }


class FailingEngine(FakeEngine):
    """Like FakeEngine, but a query on `table` fails the way Athena does for a missing table."""

    def __init__(self, rows_by_marker, table):
        super().__init__(rows_by_marker)
        self.table = table

    def run(self, sql):
        if self.table in sql:
            raise RuntimeError(f"Athena query failed: TABLE_NOT_FOUND: {self.table}")
        return super().run(sql)


def read(tmp_path, name):
    path = tmp_path / f"dashboard/v1/{name}.json"
    return json.loads(path.read_text()) if path.exists() else None


def test_a_failing_query_affects_only_its_own_snapshot(tmp_path):
    engine = FailingEngine(fake_rows(), "gold.burst_alerts")
    with pytest.raises(s.ExportError, match="alerts"):
        s.export_dashboard(engine, LocalStore(tmp_path), NOW)
    assert read(tmp_path, "alerts") is None
    s.validate("baseline", read(tmp_path, "baseline"))
    meta = read(tmp_path, "meta")
    s.validate("meta", meta)
    assert meta["exports"]["alerts"] == {"status": "failed", "failed_query": "alerts"}
    assert meta["exports"]["baseline"]["status"] == "ok"
    assert meta["snapshots"]["alerts"] is None and meta["snapshots"]["edits_per_min"] == 0


def test_a_failed_export_keeps_the_last_good_snapshot(tmp_path):
    store = LocalStore(tmp_path)
    s.export_dashboard(FakeEngine(fake_rows()), store, NOW)
    before = read(tmp_path, "alerts")
    with pytest.raises(s.ExportError):
        s.export_dashboard(FailingEngine(fake_rows(), "gold.burst_alerts"), store, NOW)
    assert read(tmp_path, "alerts") == before


def test_an_invalid_snapshot_is_not_written_and_meta_says_so(tmp_path):
    with pytest.raises(s.ExportError):
        s.export_dashboard(FakeEngine(fake_rows(bad_alert=True)), LocalStore(tmp_path), NOW)
    assert read(tmp_path, "alerts") is None
    meta = read(tmp_path, "meta")
    assert meta["exports"]["alerts"] == {"status": "failed", "failed_query": None}
    assert read(tmp_path, "baseline") is not None


def test_meta_without_exports_still_validates():
    """meta.json files written before the exports field existed stay valid."""
    meta = s.build_meta(NOW, s.dbt_status(None), None, None)
    assert "exports" not in meta
    s.validate("meta", meta)
