"""Alert rules R1 to R4 on a local Iceberg catalog, using the real seed files."""

from __future__ import annotations

from pathlib import Path

import pytest

from streaming.lib.alerts import process_batch
from streaming.lib.ref import load_ref
from streaming.lib.tables import GOLD_ALERTS, REF_ALERT_RULES, REF_WATCHLIST, create_tables
from streaming.lib.transform import to_silver
from streaming.tests.test_transform import event, kafka_rows

SEEDS = Path(__file__).resolve().parents[2] / "dbt" / "seeds"
WATCHED = "Unilever"
TEMP_ACCOUNT = "~2026-12345-67"
DOC_IP = "192.0.2.10"  # RFC 5737 documentation address, never a real editor


@pytest.fixture
def lake(spark):
    for table in (GOLD_ALERTS, REF_WATCHLIST, REF_ALERT_RULES):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    create_tables(spark, (REF_WATCHLIST, REF_ALERT_RULES, GOLD_ALERTS))
    load_ref(spark, SEEDS)
    return spark


def edit(meta_id: str, **overrides) -> dict:
    """A small watched-page edit (+50 bytes) in the article namespace; override anything."""
    fields = {"title": WATCHED, "user": "ExampleUser", "length": {"old": 10_000, "new": 10_050}}
    fields.update(overrides)
    return event(meta={"id": meta_id, "dt": "2026-10-03T12:00:00Z"}, **fields)


def log_event(meta_id: str, log_type: str, log_action: str, **overrides) -> dict:
    return edit(
        meta_id,
        type="log",
        log_type=log_type,
        log_action=log_action,
        length=None,
        revision=None,
        **overrides,
    )


def run(spark, *events) -> list[tuple[str, str, str]]:
    """Process one batch; return all alerts as sorted (meta_id, rule_id, severity)."""
    process_batch(to_silver(kafka_rows(spark, list(events))))
    return sorted((r.meta_id, r.rule_id, r.severity) for r in spark.table(GOLD_ALERTS).collect())


# ---------------------------------------------------------------- R1 and R4


@pytest.mark.parametrize(
    ("log_type", "action"),
    [("delete", "delete"), ("delete", "delete_redir"), ("move", "move"), ("move", "move_redir")],
)
def test_r1_page_deleted_or_moved(lake, log_type, action):
    assert run(lake, log_event("a", log_type, action)) == [("a", "R1", "high")]


@pytest.mark.parametrize(("log_type", "action"), [("delete", "restore"), ("delete", "revision")])
def test_r1_ignores_restores_and_revision_hiding(lake, log_type, action):
    assert run(lake, log_event("a", log_type, action)) == []


@pytest.mark.parametrize("action", ["protect", "modify", "unprotect", "move_prot"])
def test_r4_protection_change(lake, action):
    assert run(lake, log_event("a", "protect", action)) == [("a", "R4", "low")]


def test_bots_can_raise_r1(lake):
    assert run(lake, log_event("a", "delete", "delete", bot=True)) == [("a", "R1", "high")]


# ---------------------------------------------------------------- R2 boundaries


@pytest.mark.parametrize(
    ("old", "new", "alerts"),
    [
        (10_000, 8_000, [("a", "R2", "medium")]),  # exactly -2,000 bytes: "at most -2,000"
        (10_000, 8_001, []),  # -1,999 bytes and under 20%
        (1_000, 800, []),  # exactly 20% removed: rule needs "more than 20%"
        (1_000, 799, [("a", "R2", "medium")]),  # 20.1% removed
        (0, 0, []),  # empty page: no division
        (500, 900, []),  # growth
    ],
)
def test_r2_large_removal_boundaries(lake, old, new, alerts):
    assert run(lake, edit("a", length={"old": old, "new": new})) == alerts


def test_r2_never_for_bots(lake):
    assert run(lake, edit("a", bot=True, length={"old": 10_000, "new": 100})) == []


def test_r2_is_high_when_editor_is_unregistered(lake):
    removal = edit("a", user=TEMP_ACCOUNT, length={"old": 10_000, "new": 100})
    assert run(lake, removal) == [("a", "R2", "high"), ("a", "R3", "low")]


# ---------------------------------------------------------------- R3


@pytest.mark.parametrize("user", [TEMP_ACCOUNT, DOC_IP])
def test_r3_unregistered_editor(lake, user):
    assert run(lake, edit("a", user=user)) == [("a", "R3", "low")]


def test_r3_on_page_creation(lake):
    created = edit("a", user=TEMP_ACCOUNT, type="new", length={"old": None, "new": 300})
    assert run(lake, created) == [("a", "R3", "low")]


def test_registered_small_edit_raises_nothing(lake):
    assert run(lake, edit("a", length={"old": 10_000, "new": 10_050})) == []


# ---------------------------------------------------------------- scope


def test_unwatched_pages_never_alert(lake):
    removal = edit("a", title="Some Other Page", length={"old": 10_000, "new": 100})
    assert run(lake, removal) == []


def test_only_article_namespace_counts(lake):
    talk = edit("a", namespace=1, user=TEMP_ACCOUNT, length={"old": 10_000, "new": 100})
    assert run(lake, talk) == []


# ---------------------------------------------------------------- determinism and config


def test_alert_ids_are_deterministic_and_reprocessing_adds_nothing(lake):
    removal = edit("a", user=TEMP_ACCOUNT, length={"old": 10_000, "new": 100})
    run(lake, removal)
    first = {r.alert_id for r in lake.table(GOLD_ALERTS).collect()}
    run(lake, removal)  # same edit again (replay or retried batch)
    second = [r.alert_id for r in lake.table(GOLD_ALERTS).collect()]
    assert sorted(second) == sorted(first) and len(first) == 2  # R2 and R3, distinct IDs
    assert all(len(i) == 64 for i in first)


def test_threshold_change_applies_to_the_next_batch_without_restart(lake):
    small = {"old": 10_000, "new": 8_500}  # -1,500 bytes, 15%
    assert run(lake, edit("a", length=small)) == []
    lake.sql(f"UPDATE {REF_ALERT_RULES} SET min_removed_bytes = 1000 WHERE rule_id = 'R2'")
    assert run(lake, edit("b", length=small)) == [("b", "R2", "medium")]


def test_disabled_rule_raises_nothing(lake):
    lake.sql(f"UPDATE {REF_ALERT_RULES} SET enabled = false WHERE rule_id = 'R3'")
    assert run(lake, edit("a", user=TEMP_ACCOUNT)) == []


def test_alerts_carry_watchlist_context_and_timings(lake):
    run(lake, log_event("a", "delete", "delete"))
    row = lake.table(GOLD_ALERTS).collect()[0]
    assert (row.category, row.owner_team, row.log_action) == (
        "own_brand",
        "corporate-comms",
        "delete",
    )
    assert row.detected_at is not None and row.event_ts is not None
