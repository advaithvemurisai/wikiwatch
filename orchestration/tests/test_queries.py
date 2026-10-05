"""Every export and health query is partition-pruned (CLAUDE.md cost rules)."""

from __future__ import annotations

import re
from datetime import datetime

import pytest

from orchestration.queries import ALL_QUERIES, PARTITION_COLUMNS, session_param

NOW = datetime(2026, 10, 4, 12, 30, 0)
TABLE_REF = re.compile(r"\b((?:bronze|silver|gold|ref)\.[a-z_]+)\b")
RENDER_PARAMS = {"session_id": "redpanda-0f1e2d3c"}


@pytest.mark.parametrize("query", ALL_QUERIES, ids=lambda q: q.name)
def test_every_table_read_is_declared(query):
    sql = query.render(NOW, **RENDER_PARAMS)
    assert set(TABLE_REF.findall(sql)) == set(query.scans.values())


@pytest.mark.parametrize("query", ALL_QUERIES, ids=lambda q: q.name)
def test_every_partitioned_scan_has_a_literal_bound(query):
    sql = query.render(NOW, **RENDER_PARAMS)
    for alias, table in query.scans.items():
        column = PARTITION_COLUMNS[table]
        if column is None:
            continue  # tiny unpartitioned ref table
        prefix = rf"(?:{alias}\.)?" if alias else ""
        bound = rf"{prefix}{column} >= (?:timestamp|date) '\d{{4}}-\d{{2}}-\d{{2}}[ \d:]*'"
        assert re.search(bound, sql), f"{query.name}: {table} has no literal bound on {column}"


@pytest.mark.parametrize("query", ALL_QUERIES, ids=lambda q: q.name)
def test_no_runtime_clock_in_sql(query):
    """Bounds must be literals: now() or current_timestamp defeats static pruning checks."""
    sql = query.render(NOW, **RENDER_PARAMS).lower()
    assert "now()" not in sql and "current_timestamp" not in sql and "current_date" not in sql


def test_rendering_is_deterministic_for_a_run_time():
    for query in ALL_QUERIES:
        assert query.render(NOW, **RENDER_PARAMS) == query.render(NOW, **RENDER_PARAMS)


def test_bounds_follow_the_run_time():
    from orchestration.queries import ALERTS

    assert "timestamp '2026-09-27 12:30:00'" in ALERTS.render(NOW)


@pytest.mark.parametrize("bad", ["x'; drop table t; --", "redpanda id", ""])
def test_session_id_is_validated_before_it_reaches_sql(bad):
    with pytest.raises(ValueError):
        session_param(bad)
