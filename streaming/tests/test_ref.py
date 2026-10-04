"""Seed contracts and the ref loader."""

from __future__ import annotations

from collections import namedtuple
from pathlib import Path

import pytest

from streaming.lib.ref import (
    SeedError,
    load_ref,
    load_ref_if_empty,
    validate_rules,
    validate_watchlist,
)
from streaming.lib.tables import REF_ALERT_RULES, REF_WATCHLIST, create_tables

SEEDS = Path(__file__).resolve().parents[2] / "dbt" / "seeds"
W = namedtuple("W", "wiki title category owner_team")
R = namedtuple(
    "R",
    "rule_id rule_name severity min_removed_bytes min_removed_fraction min_edits "
    "window_minutes enabled",
)
RULES = [
    R("R1", "x", "high", None, None, None, None, True),
    R("R2", "x", "medium", 2000, 0.2, None, None, True),
    R("R3", "x", "low", None, None, None, None, True),
    R("R4", "x", "low", None, None, None, None, True),
    R("R5", "x", "medium", None, None, 5, 10, True),
]


@pytest.fixture
def ref_tables(spark):
    for table in (REF_WATCHLIST, REF_ALERT_RULES):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    create_tables(spark, (REF_WATCHLIST, REF_ALERT_RULES))
    return spark


def test_real_seeds_load_and_match_the_v1_watchlist(ref_tables):
    counts = load_ref(ref_tables, SEEDS)
    assert counts == {REF_WATCHLIST: 60, REF_ALERT_RULES: 5}
    categories = {r.category for r in ref_tables.table(REF_WATCHLIST).collect()}
    assert categories == {"own_brand", "product", "competitor"}
    assert {r.wiki for r in ref_tables.table(REF_WATCHLIST).collect()} == {"enwiki"}


def test_reload_replaces_rather_than_appends(ref_tables):
    load_ref(ref_tables, SEEDS)
    load_ref(ref_tables, SEEDS)
    assert ref_tables.table(REF_WATCHLIST).count() == 60


def test_load_if_empty_only_loads_once(ref_tables):
    assert load_ref_if_empty(ref_tables, SEEDS) is not None
    assert load_ref_if_empty(ref_tables, SEEDS) is None


def test_duplicate_watchlist_rows_are_rejected():
    rows = [W("enwiki", "Acme", "product", "t"), W("enwiki", "Acme", "product", "t")]
    with pytest.raises(SeedError, match="duplicate"):
        validate_watchlist(rows)


def test_unknown_category_is_rejected():
    with pytest.raises(SeedError, match="category"):
        validate_watchlist([W("enwiki", "Acme", "person", "t")])


def test_rules_must_cover_r1_to_r5_once():
    with pytest.raises(SeedError, match="R1 to R5"):
        validate_rules(RULES[:4])


def test_unknown_severity_is_rejected():
    bad = [r._replace(severity="critical") if r.rule_id == "R1" else r for r in RULES]
    with pytest.raises(SeedError, match="severity"):
        validate_rules(bad)


def test_r2_needs_its_thresholds():
    bad = [r._replace(min_removed_bytes=None) if r.rule_id == "R2" else r for r in RULES]
    with pytest.raises(SeedError, match="R2"):
        validate_rules(bad)


@pytest.mark.parametrize("minutes", [0, 7, 45])
def test_r5_window_must_divide_an_hour(minutes):
    bad = [r._replace(window_minutes=minutes) if r.rule_id == "R5" else r for r in RULES]
    with pytest.raises(SeedError, match="divide 60"):
        validate_rules(bad)
