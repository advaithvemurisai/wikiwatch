"""Load the reference seeds (dbt/seeds/*.csv) into Iceberg ref tables.

This loader is the only writer of ref.watchlist and ref.alert_rules (docs/adr/0006); dbt
reads them as sources. Each load validates the CSVs and overwrites the whole table in one
Iceberg commit, so readers see either the old or the new version, never a mix.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .tables import REF_ALERT_RULES, REF_WATCHLIST

WATCHLIST_SCHEMA = "wiki STRING, title STRING, category STRING, owner_team STRING"
RULES_SCHEMA = (
    "rule_id STRING, rule_name STRING, severity STRING, min_removed_bytes BIGINT, "
    "min_removed_fraction DECIMAL(10,4), min_edits INT, window_minutes INT, enabled BOOLEAN"
)
CATEGORIES = {"own_brand", "product", "competitor"}
SEVERITIES = {"high", "medium", "low"}
RULE_IDS = {"R1", "R2", "R3", "R4", "R5"}


class SeedError(ValueError):
    """A seed file breaks its contract."""


def read_seed(spark, path: Path, schema: str) -> DataFrame:
    """Read a seed CSV with an explicit schema (no inference)."""
    return spark.read.csv(str(path), header=True, schema=schema, mode="FAILFAST")


def validate_watchlist(rows: list) -> None:
    """No duplicate pages, known categories, no blank fields."""
    keys = [(r.wiki, r.title) for r in rows]
    if len(keys) != len(set(keys)):
        raise SeedError("watchlist has duplicate (wiki, title) rows")
    for r in rows:
        if not all((r.wiki, r.title, r.category, r.owner_team)):
            raise SeedError(f"watchlist row has a blank field: {r.title!r}")
        if r.category not in CATEGORIES:
            raise SeedError(f"unknown category {r.category!r} for {r.title!r}")


def validate_rules(rows: list) -> None:
    """Each of R1 to R5 exactly once, known severities, R2 and R5 thresholds present."""
    ids = [r.rule_id for r in rows]
    if sorted(ids) != sorted(RULE_IDS):
        raise SeedError(f"alert_rules must define R1 to R5 exactly once, got {sorted(ids)}")
    for r in rows:
        if r.severity not in SEVERITIES:
            raise SeedError(f"unknown severity {r.severity!r} for {r.rule_id}")
        if r.enabled is None:
            raise SeedError(f"{r.rule_id}: enabled must be true or false")
    r2 = next(r for r in rows if r.rule_id == "R2")
    if r2.min_removed_bytes is None or r2.min_removed_fraction is None:
        raise SeedError("R2 needs min_removed_bytes and min_removed_fraction")
    r5 = next(r for r in rows if r.rule_id == "R5")
    if r5.min_edits is None or r5.window_minutes is None:
        raise SeedError("R5 needs min_edits and window_minutes")
    if r5.window_minutes <= 0 or 60 % r5.window_minutes:
        # dbt's burst_alerts aligns windows inside each hour (:00, :10, ...).
        raise SeedError("R5 window_minutes must divide 60 (for example 5, 10, 15, 30)")


def load_ref(spark, seeds_dir: Path) -> dict[str, int]:
    """Validate both seeds, then overwrite both ref tables. Returns row counts."""
    watchlist = read_seed(spark, seeds_dir / "watchlist.csv", WATCHLIST_SCHEMA)
    rules = read_seed(spark, seeds_dir / "alert_rules.csv", RULES_SCHEMA)
    validate_watchlist(watchlist.collect())
    validate_rules(rules.collect())
    counts = {}
    for table, df in ((REF_WATCHLIST, watchlist), (REF_ALERT_RULES, rules)):
        stamped = df.withColumn("loaded_at", F.current_timestamp().cast("timestamp_ntz"))
        stamped.writeTo(table).overwrite(F.lit(True))
        counts[table] = df.count()
    return counts


def load_ref_if_empty(spark, seeds_dir: Path) -> dict[str, int] | None:
    """Load the seeds only when a ref table is still empty (first start)."""
    if all(spark.table(t).limit(1).count() for t in (REF_WATCHLIST, REF_ALERT_RULES)):
        return None
    return load_ref(spark, seeds_dir)
