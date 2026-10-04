"""Per-edit alert rules R1 to R4 (docs/v1.md, Alert rules) and the alert MERGE.

Rules, as implemented (thresholds come from ref.alert_rules, read fresh every batch):
  common  watched page (wiki + title in ref.watchlist), article namespace (0) only
  R1      log event, log_type delete or move, log_action an actual deletion or move
          (delete, delete_redir, move, move_redir; restores and revision hiding excluded)
  R2      edit with byte_delta <= -min_removed_bytes, or more than min_removed_fraction of
          the page removed; never for bots; severity high when R3 also holds
  R3      edit or new page by an unregistered editor (IP or temporary account); bots are
          never "unregistered" because the bot flag wins in editor_type
  R4      log event with log_type protect
alert_id = sha256("<rule_id>:<meta_id>"): reprocessing the same edit gives the same ID.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .silver import insert_new_rows
from .tables import GOLD_ALERTS, REF_ALERT_RULES, REF_WATCHLIST

R1_LOG_TYPES = ("delete", "move")
R1_LOG_ACTIONS = ("delete", "delete_redir", "move", "move_redir")
EDIT_TYPES = ("edit", "new")

ALERT_COLUMNS = [
    "alert_id",
    "rule_id",
    "severity",
    "meta_id",
    "event_ts",
    "ingested_at",
    "wiki",
    "title",
    "category",
    "owner_team",
    "edit_type",
    "log_type",
    "log_action",
    "editor_type",
    "byte_delta",
    "length_old",
    "length_new",
]


def alert_id(rule_id: Column, meta_id: Column) -> Column:
    """Deterministic alert ID: sha256 of "<rule_id>:<meta_id>" as hex."""
    return F.sha2(F.concat_ws(":", rule_id, meta_id), 256)


def r2_condition(min_bytes: int, min_fraction) -> Column:
    """Large removal by a non-bot editor. Uses exact decimals at the 20% boundary."""
    removed = F.col("length_old") - F.col("length_new")
    by_bytes = F.col("byte_delta") <= F.lit(-int(min_bytes))
    by_share = (F.col("length_old") > 0) & (
        removed.cast("decimal(20,4)")
        > F.lit(str(min_fraction)).cast("decimal(10,4)") * F.col("length_old")
    )
    return (
        (F.col("edit_type") == "edit")
        & (F.col("editor_type") != "bot")
        & F.coalesce(by_bytes | by_share, F.lit(False))
    )


def r3_condition() -> Column:
    return F.col("edit_type").isin(*EDIT_TYPES) & (F.col("editor_type") == "unregistered")


def find_alerts(edits: DataFrame, watchlist: DataFrame, rules: list) -> DataFrame:
    """Apply R1 to R4 to Silver-shaped edits. ``rules`` are rows of ref.alert_rules."""
    by_id = {r.rule_id: r for r in rules}
    watched = edits.where(F.col("namespace") == 0).join(
        F.broadcast(watchlist.select("wiki", "title", "category", "owner_team")),
        ["wiki", "title"],
    )
    candidates: list[tuple[str, Column, Column]] = []
    if by_id["R1"].enabled:
        r1 = (
            (F.col("edit_type") == "log")
            & F.col("log_type").isin(*R1_LOG_TYPES)
            & F.col("log_action").isin(*R1_LOG_ACTIONS)
        )
        candidates.append(("R1", r1, F.lit(by_id["R1"].severity)))
    if by_id["R2"].enabled:
        r2 = r2_condition(by_id["R2"].min_removed_bytes, by_id["R2"].min_removed_fraction)
        severity = F.when(r3_condition(), F.lit("high")).otherwise(F.lit(by_id["R2"].severity))
        candidates.append(("R2", r2, severity))
    if by_id["R3"].enabled:
        candidates.append(("R3", r3_condition(), F.lit(by_id["R3"].severity)))
    if by_id["R4"].enabled:
        r4 = (F.col("edit_type") == "log") & (F.col("log_type") == "protect")
        candidates.append(("R4", r4, F.lit(by_id["R4"].severity)))

    found = None
    for rule_id, condition, severity in candidates:
        part = (
            watched.where(condition)
            .withColumn("rule_id", F.lit(rule_id))
            .withColumn("severity", severity)
            .withColumn("alert_id", alert_id(F.col("rule_id"), F.col("meta_id")))
            .select(*ALERT_COLUMNS)
        )
        found = part if found is None else found.unionByName(part)
    return found


def process_batch(edits: DataFrame, alerts_table: str = GOLD_ALERTS) -> int:
    """foreachBatch body: re-read the ref tables, find alerts, insert the new ones.

    REFRESH TABLE makes a threshold or watchlist change visible in the very next
    micro-batch, whatever the catalog cache holds. Returns the number of unique alerts
    in the batch (already-known alert IDs are not inserted again).
    """
    spark = edits.sparkSession
    for table in (REF_WATCHLIST, REF_ALERT_RULES):
        spark.sql(f"REFRESH TABLE {table}")
    rules = spark.table(REF_ALERT_RULES).collect()
    alerts = find_alerts(edits, spark.table(REF_WATCHLIST), rules)
    if alerts is None:
        return 0
    return insert_new_rows(alerts, alerts_table, key="alert_id", stamp_column="detected_at")
