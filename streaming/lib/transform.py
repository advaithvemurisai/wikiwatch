"""Kafka records to Bronze rows and Silver rows (pure DataFrame functions).

Every function takes and returns a DataFrame, so each step is unit-tested on a local
SparkSession without Kafka or Iceberg.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

LATE_AFTER_SECONDS = 10 * 60  # is_late threshold from docs/plan.md

# Patterns for editor_type. Temporary accounts were verified live on 2026-10-03 as
# ~YYYY-NNNNN-NN; the digit counts are kept loose in case the format changes.
IPV4 = r"^([0-9]{1,3}\.){3}[0-9]{1,3}$"
IPV6 = r"^[0-9A-Fa-f]*:[0-9A-Fa-f:]*:[0-9A-Fa-f:.]*$"
TEMP_ACCOUNT = r"^~[0-9]{4}-[0-9]+-[0-9]+$"

_REVISION = StructType([StructField("old", LongType()), StructField("new", LongType())])
EVENT_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("dt", StringType()),
                    StructField("domain", StringType()),
                ]
            ),
        ),
        StructField("type", StringType()),
        StructField("namespace", LongType()),
        StructField("title", StringType()),
        StructField("wiki", StringType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("length", _REVISION),
        StructField("revision", _REVISION),
        StructField("log_type", StringType()),
        StructField("log_action", StringType()),
    ]
)


def kafka_columns(kafka_df: DataFrame) -> DataFrame:
    """Decode a Kafka source DataFrame into string payload plus partition, offset and time.

    ``ingested_at`` is the Kafka record timestamp (the producer's publish time), turned
    into a UTC TIMESTAMP_NTZ. The session time zone is UTC (see session.py).
    """
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").cast("timestamp_ntz").alias("ingested_at"),
    )


def to_bronze(df: DataFrame, session_id: str) -> DataFrame:
    """Bronze row: the raw payload, its meta.id, and where it came from in Kafka."""
    return df.select(
        F.get_json_object("raw", "$.meta.id").alias("meta_id"),
        "raw",
        "kafka_key",
        F.lit(session_id).alias("session_id"),
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    )


def to_bronze_dlq(df: DataFrame, session_id: str) -> DataFrame:
    """Bronze DLQ row: unwrap the producer's {error, raw, rejected_at} envelope."""
    return df.select(
        F.get_json_object("raw", "$.error").alias("error"),
        F.get_json_object("raw", "$.raw").alias("raw"),
        _utc_ntz(F.get_json_object("raw", "$.rejected_at")).alias("rejected_at"),
        F.lit(session_id).alias("session_id"),
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    )


def is_ip_address(user: Column) -> Column:
    """True when the editor name is an IPv4 or IPv6 address."""
    return user.rlike(IPV4) | user.rlike(IPV6)


def editor_type(user: Column, bot: Column) -> Column:
    """registered, unregistered (IP or temporary account) or bot. Bot wins.

    A missing (suppressed) user name counts as registered: suppression is applied to
    account names, and treating it as unregistered would raise false R3 alerts.
    """
    unregistered = is_ip_address(user) | user.rlike(TEMP_ACCOUNT)
    return (
        F.when(F.coalesce(bot, F.lit(False)), "bot")
        .when(F.coalesce(unregistered, F.lit(False)), "unregistered")
        .otherwise("registered")
    )


def byte_delta(edit_type: Column, length_old: Column, length_new: Column) -> Column:
    """Size change in bytes: new - old; the full size for a new page; null otherwise."""
    return (
        F.when(length_old.isNotNull() & length_new.isNotNull(), length_new - length_old)
        .when((edit_type == "new") & length_new.isNotNull(), length_new)
        .otherwise(F.lit(None).cast("long"))
    )


def is_late(ingested_at: Column, event_ts: Column) -> Column:
    """True when the event reached the pipeline more than 10 minutes after it happened."""
    lag = F.unix_timestamp(ingested_at.cast("timestamp")) - F.unix_timestamp(
        event_ts.cast("timestamp")
    )
    return lag > LATE_AFTER_SECONDS


def to_silver(df: DataFrame) -> DataFrame:
    """Parse and type payloads into Silver rows; drop rows without meta_id or event time.

    Dropped rows stay in Bronze. The producer validates every event, so drops here mean
    an upstream format change, not normal traffic. IP addresses never reach Silver:
    ``user_name`` is null for IP editors, after ``editor_type`` has used it.
    """
    event = F.from_json("raw", EVENT_SCHEMA)
    parsed = df.select(event.alias("e"), "ingested_at")
    user = F.col("e.user")
    rows = parsed.select(
        F.col("e.meta.id").alias("meta_id"),
        _utc_ntz(F.col("e.meta.dt")).alias("event_ts"),
        "ingested_at",
        F.col("e.wiki").alias("wiki"),
        F.col("e.title").alias("title"),
        F.col("e.namespace").cast("int").alias("namespace"),
        F.col("e.type").alias("edit_type"),
        F.col("e.log_type").alias("log_type"),
        F.col("e.log_action").alias("log_action"),
        F.when(is_ip_address(user), F.lit(None).cast("string")).otherwise(user).alias("user_name"),
        F.col("e.bot").alias("is_bot"),
        editor_type(user, F.col("e.bot")).alias("editor_type"),
        F.col("e.length.old").alias("length_old"),
        F.col("e.length.new").alias("length_new"),
        byte_delta(F.col("e.type"), F.col("e.length.old"), F.col("e.length.new")).alias(
            "byte_delta"
        ),
        F.col("e.revision.old").alias("revision_old"),
        F.col("e.revision.new").alias("revision_new"),
    )
    rows = rows.where(F.col("meta_id").isNotNull() & F.col("event_ts").isNotNull())
    return rows.withColumn("is_late", is_late(F.col("ingested_at"), F.col("event_ts")))


def _utc_ntz(value: Column) -> Column:
    """Parse an ISO-8601 string (e.g. 2026-10-03T21:04:56.484Z) to UTC TIMESTAMP_NTZ.

    ``try_cast`` returns null instead of failing the batch under Spark 4's ANSI mode.
    """
    return value.try_cast("timestamp").cast("timestamp_ntz")
