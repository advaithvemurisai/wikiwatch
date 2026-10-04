"""Iceberg table definitions for Bronze and Silver (format-version 2, UTC partitions).

Timestamps are stored as TIMESTAMP_NTZ holding UTC wall-clock time: every value is UTC by
convention, and Athena, Trino and Spark all read Iceberg's plain ``timestamp`` type the
same way. ``hours(...)`` partitions give one folder per UTC date and hour.
"""

from __future__ import annotations

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}

BRONZE_EDITS = "lake.bronze.wiki_edits_raw"
BRONZE_DLQ = "lake.bronze.wiki_edits_dlq"
SILVER_EDITS = "lake.silver.wiki_edits"
REF_WATCHLIST = "lake.ref.watchlist"
REF_ALERT_RULES = "lake.ref.alert_rules"
GOLD_EDITS_PER_MIN = "lake.gold.edits_per_min"
GOLD_ALERTS = "lake.gold.watched_page_alerts"

_DDL = {
    BRONZE_EDITS: """
        meta_id STRING COMMENT 'meta.id from the payload; null if missing',
        raw STRING COMMENT 'payload exactly as received',
        kafka_key STRING,
        session_id STRING COMMENT 'Redpanda cluster ID; Kafka offsets restart in every session',
        kafka_partition INT,
        kafka_offset BIGINT,
        ingested_at TIMESTAMP_NTZ COMMENT 'Kafka record time: when the producer published it (UTC)'
    """,
    BRONZE_DLQ: """
        error STRING COMMENT 'validation rule that failed (field path and rule, no values)',
        raw STRING COMMENT 'rejected payload exactly as received',
        rejected_at TIMESTAMP_NTZ,
        session_id STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        ingested_at TIMESTAMP_NTZ
    """,
    SILVER_EDITS: """
        meta_id STRING NOT NULL COMMENT 'unique event ID (dedup key)',
        event_ts TIMESTAMP_NTZ NOT NULL COMMENT 'meta.dt: when the change happened (UTC)',
        ingested_at TIMESTAMP_NTZ COMMENT 'when the producer published it (UTC)',
        processed_at TIMESTAMP_NTZ COMMENT 'when this row was merged into Silver (UTC)',
        wiki STRING,
        title STRING,
        namespace INT,
        edit_type STRING COMMENT 'edit, new, log, categorize or external',
        log_type STRING,
        log_action STRING,
        user_name STRING COMMENT 'null for IP-address editors (privacy)',
        is_bot BOOLEAN,
        editor_type STRING COMMENT 'registered, unregistered or bot',
        length_old BIGINT,
        length_new BIGINT,
        byte_delta BIGINT COMMENT 'length_new - length_old; full size for new pages',
        revision_old BIGINT,
        revision_new BIGINT,
        is_late BOOLEAN COMMENT 'ingested more than 10 minutes after event_ts'
    """,
    REF_WATCHLIST: """
        wiki STRING NOT NULL,
        title STRING NOT NULL COMMENT 'exact Wikipedia title (no redirects)',
        category STRING NOT NULL COMMENT 'own_brand, product or competitor',
        owner_team STRING NOT NULL,
        loaded_at TIMESTAMP_NTZ
    """,
    REF_ALERT_RULES: """
        rule_id STRING NOT NULL,
        rule_name STRING NOT NULL,
        severity STRING NOT NULL COMMENT 'high, medium or low',
        min_removed_bytes BIGINT COMMENT 'R2: removal of at least this many bytes',
        min_removed_fraction DECIMAL(10, 4) COMMENT 'R2: removal of more than this share',
        min_edits INT COMMENT 'R5: edits per window',
        window_minutes INT COMMENT 'R5: window length',
        enabled BOOLEAN NOT NULL,
        loaded_at TIMESTAMP_NTZ
    """,
    GOLD_EDITS_PER_MIN: """
        window_start TIMESTAMP_NTZ COMMENT 'nullable: Iceberg streaming appends check nullability',
        window_end TIMESTAMP_NTZ,
        wiki STRING,
        edits BIGINT COMMENT 'edit and new events, duplicates removed within the watermark',
        bot_edits BIGINT,
        bytes_changed BIGINT COMMENT 'sum of absolute byte_delta'
    """,
    GOLD_ALERTS: """
        alert_id STRING NOT NULL COMMENT 'sha256(rule_id:meta_id), deterministic',
        rule_id STRING NOT NULL,
        severity STRING NOT NULL,
        meta_id STRING NOT NULL,
        event_ts TIMESTAMP_NTZ NOT NULL,
        ingested_at TIMESTAMP_NTZ,
        detected_at TIMESTAMP_NTZ COMMENT 'when the alert was committed (UTC)',
        wiki STRING,
        title STRING,
        category STRING,
        owner_team STRING,
        edit_type STRING,
        log_type STRING,
        log_action STRING,
        editor_type STRING,
        byte_delta BIGINT,
        length_old BIGINT,
        length_new BIGINT
    """,
}

_PARTITIONING = {
    BRONZE_EDITS: "hours(ingested_at)",
    BRONZE_DLQ: "days(ingested_at)",
    SILVER_EDITS: "hours(event_ts)",
    GOLD_EDITS_PER_MIN: "days(window_start)",
    GOLD_ALERTS: "days(event_ts)",
}


def create_table_sql(table: str) -> str:
    """Return the CREATE TABLE IF NOT EXISTS statement for one table."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    partitioning = f"PARTITIONED BY ({_PARTITIONING[table]}) " if table in _PARTITIONING else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ({_DDL[table]}) USING iceberg "
        f"{partitioning}TBLPROPERTIES ({props})"
    )


ALL_TABLES = (
    BRONZE_EDITS,
    BRONZE_DLQ,
    SILVER_EDITS,
    REF_WATCHLIST,
    REF_ALERT_RULES,
    GOLD_EDITS_PER_MIN,
    GOLD_ALERTS,
)


def create_tables(spark, tables: tuple[str, ...] = ALL_TABLES):
    """Create the namespaces and tables if missing (idempotent)."""
    for namespace in sorted({t.rsplit(".", 1)[0] for t in tables}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for table in tables:
        spark.sql(create_table_sql(table))
