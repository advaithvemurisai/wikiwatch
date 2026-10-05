"""health.json, lag from progress records, and Athena result typing."""

from __future__ import annotations

import json
from datetime import date, datetime

from orchestration import freshness as f
from orchestration.engines import AthenaEngine, athena_value
from orchestration.snapshots import validate
from orchestration.store import LocalStore

NOW = datetime(2026, 10, 4, 12, 30, 0)
SESSION = "redpanda-0f1e2d3c"


def progress(query, minute, end=None, trigger=800, rate=40.0, session=SESSION):
    sources = [{"start_offset": None, "end_offset": end, "latest_offset": end}] if end else []
    return {
        "session_id": session, "query": query, "batch_id": minute,
        "timestamp": f"2026-10-04T12:{minute:02d}:00.000Z",
        "trigger_ms": trigger, "input_rows_per_second": rate, "sources": sources,
    }  # fmt: skip


def write_progress(store, records, name="122900-abc.jsonl"):
    path = store.root / f"ops/stream_progress/dt=2026-10-04/{name}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def test_lag_is_latest_offset_minus_silver_end_offset():
    records = [
        progress("silver_edits", 10, end={"wiki_edits": {"0": 90, "1": 50}}),
        progress("silver_edits", 20, end=json.dumps({"wiki_edits": {"0": 100, "1": 60}})),
        progress("bronze_edits", 21, end={"wiki_edits": {"0": 120, "1": 70}}),
    ]
    rows = f.lag_rows({0: 130, 1: 60, 2: 5}, f.processed_offsets(records))
    assert rows == [
        {"partition": 0, "latest_offset": 130, "processed_offset": 100, "lag": 30},
        {"partition": 1, "latest_offset": 60, "processed_offset": 60, "lag": 0},
        {"partition": 2, "latest_offset": 5, "processed_offset": None, "lag": None},
    ]


def test_streaming_stats_per_query():
    records = [progress("silver_edits", m, trigger=t) for m, t in ((1, 500), (2, 900), (3, 700))]
    (stats,) = f.streaming_stats(records)
    assert stats == {
        "query": "silver_edits", "batches": 3, "input_rows_per_second": 40.0,
        "batch_duration_ms_p50": 700, "batch_duration_ms_max": 900}  # fmt: skip


def test_only_this_sessions_recent_records_are_used(tmp_path):
    store = LocalStore(tmp_path)
    write_progress(store, [
        progress("silver_edits", 25),
        progress("silver_edits", 26, session="redpanda-old"),
    ])  # fmt: skip
    old = tmp_path / "ops/stream_progress/dt=2026-10-03/235900-x.jsonl"
    old.parent.mkdir(parents=True)
    old.write_text(json.dumps({**progress("silver_edits", 1), "timestamp": "2026-10-03T08:00:00Z"}))
    records = f.recent_progress(store, SESSION, NOW)
    assert [r["batch_id"] for r in records] == [25]


def test_health_snapshot_is_valid():
    kafka = f.KafkaState(SESSION, {"wiki_edits": {0: 130, 1: 60}, "wiki_edits_dlq": {0: 1}})
    health = f.build_health(
        NOW,
        kafka,
        {
            "bronze": datetime(2026, 10, 4, 12, 29),
            "silver": datetime(2026, 10, 4, 12, 28),
            "gold": None,
        },  # fmt: skip
        {"bronze": 190, "silver_unique": 185, "dlq": 1},
        {"samples": 12, "p50": 16.3, "p95": 60.66},
        [progress("silver_edits", 20, end={"wiki_edits": {"0": 100, "1": 60}})],
    )
    validate("health", health)
    assert health["funnel"] == {"received": 191, "bronze": 190, "silver_unique": 185, "dlq": 1}
    assert health["detection_seconds"] == {"samples": 12, "p50": 16.3, "p95": 60.7}
    assert "redpanda" not in json.dumps(health)  # no session or broker identifiers on pages


def test_lag_snapshot_key_is_stable_per_run_time():
    assert f.lag_snapshot_key(NOW) == "ops/lag_snapshots/dt=2026-10-04/123000.json"
    assert f.lag_snapshot_key(NOW) == f.lag_snapshot_key(NOW)


class FakeEngine:
    def run(self, sql):
        if "max(ingested_at)" in sql:
            return [{"bronze": NOW, "silver": NOW, "gold": NOW}]
        if "count(distinct meta_id)" in sql:
            assert f"session_id = '{SESSION}'" in sql
            return [{"bronze": 10, "silver_unique": 9, "dlq": 0}]
        return [{"samples": 0, "p50": None, "p95": None}]


def test_run_freshness_writes_health_and_lag_snapshot(tmp_path):
    store = LocalStore(tmp_path)
    kafka = f.KafkaState(SESSION, {"wiki_edits": {0: 10}})
    f.run_freshness(FakeEngine(), store, kafka, NOW)
    validate("health", json.loads((tmp_path / "dashboard/v1/health.json").read_text()))
    assert (tmp_path / "ops/lag_snapshots/dt=2026-10-04/123000.json").exists()


def test_athena_values_are_typed_like_trino():
    assert athena_value("42", "bigint") == 42
    assert athena_value("0.25", "double") == 0.25
    assert athena_value("true", "boolean") is True
    assert athena_value("2026-10-04 12:00:01.123", "timestamp") == datetime(
        2026, 10, 4, 12, 0, 1, 123000
    )
    assert athena_value("2026-10-04", "date") == date(2026, 10, 4)
    assert athena_value(None, "varchar") is None


class FakeAthena:
    def __init__(self):
        self.started = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):  # noqa: N803 - boto3 naming
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_paginator(self, name):
        meta = {
            "ColumnInfo": [
                {"Name": "minute", "Type": "timestamp"},
                {"Name": "edits", "Type": "bigint"},
            ]
        }
        pages = [
            {"ResultSet": {"ResultSetMetadata": meta, "Rows": [
                {"Data": [{"VarCharValue": "minute"}, {"VarCharValue": "edits"}]},
                {"Data": [{"VarCharValue": "2026-10-04 12:00:00.000"}, {"VarCharValue": "40"}]}]}},
            {"ResultSet": {"ResultSetMetadata": meta, "Rows": [
                {"Data": [{"VarCharValue": "2026-10-04 12:01:00.000"}, {}]}]}},
        ]  # fmt: skip

        class Paginator:
            def paginate(self, **_):
                return iter(pages)

        return Paginator()


def test_athena_engine_uses_the_workgroup_and_types_every_page():
    client = FakeAthena()
    rows = AthenaEngine("wikiwatch", "us-east-1", client=client).run("select 1")
    assert client.started[0]["WorkGroup"] == "wikiwatch"
    assert rows == [
        {"minute": datetime(2026, 10, 4, 12, 0), "edits": 40},
        {"minute": datetime(2026, 10, 4, 12, 1), "edits": None},
    ]
