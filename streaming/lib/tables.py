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
}

_PARTITIONING = {
    BRONZE_EDITS: "hours(ingested_at)",
    BRONZE_DLQ: "days(ingested_at)",
    SILVER_EDITS: "hours(event_ts)",
}


def create_table_sql(table: str) -> str:
    """Return the CREATE TABLE IF NOT EXISTS statement for one table."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ({_DDL[table]}) USING iceberg "
        f"PARTITIONED BY ({_PARTITIONING[table]}) TBLPROPERTIES ({props})"
    )


def create_tables(spark, tables: tuple[str, ...] = (BRONZE_EDITS, BRONZE_DLQ, SILVER_EDITS)):
    """Create the namespaces and tables if missing (idempotent)."""
    for namespace in sorted({t.rsplit(".", 1)[0] for t in tables}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for table in tables:
        spark.sql(create_table_sql(table))
