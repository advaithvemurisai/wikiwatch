"""WikiWatch streaming app: one SparkSession, one query (and checkpoint) per sink.

    bronze_edits  wiki_edits      -> lake.bronze.wiki_edits_raw   (append)
    bronze_dlq    wiki_edits_dlq  -> lake.bronze.wiki_edits_dlq   (append)
    silver_edits  wiki_edits      -> lake.silver.wiki_edits       (insert-only MERGE)
    gold_edits_per_min  wiki_edits -> lake.gold.edits_per_min     (windows, append)
    gold_alerts   wiki_edits      -> lake.gold.watched_page_alerts (R1-R4, insert-only MERGE)

The two Gold queries read Kafka through the same to_silver transform as Silver instead of
reading the Silver table: one trigger instead of two keeps per-edit detection under
2 minutes (docs/adr/0006-alerts-and-ref-tables.md).

Run with spark-submit inside the spark container (it is the container's main process).
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from streaming.lib import progress
from streaming.lib.alerts import process_batch as process_alert_batch
from streaming.lib.ref import load_ref_if_empty
from streaming.lib.session import (
    build_spark,
    checkpoint_path,
    lake_bucket,
    session_id_from_cluster_id,
)
from streaming.lib.silver import merge_into_silver
from streaming.lib.tables import BRONZE_DLQ, BRONZE_EDITS, GOLD_EDITS_PER_MIN, create_tables
from streaming.lib.transform import kafka_columns, to_bronze, to_bronze_dlq, to_silver
from streaming.lib.windows import edits_per_minute

TOPICS = ("wiki_edits", "wiki_edits_dlq")
TRIGGER = "1 minute"
MAX_OFFSETS_PER_TRIGGER = 200_000  # bounds a catch-up batch after a long resume
SEEDS_DIR = Path(os.environ.get("SEEDS_DIR", "/opt/wikiwatch/dbt/seeds"))


def log(record: dict) -> None:
    stamped = {"ts": datetime.now(UTC).isoformat(timespec="seconds"), **record}
    print(json.dumps(stamped), flush=True)


def kafka_admin(spark, bootstrap: str):
    """Kafka AdminClient from the JVM (kafka-clients is already on Spark's classpath)."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    return jvm.org.apache.kafka.clients.admin.Admin.create(props)


def wait_for_topics(admin, topics=TOPICS, poll_seconds: float = 5.0) -> None:
    """Block until the producer has created the topics.

    Subscribing first would be harmless now that auto-creation is off, but waiting keeps
    the logs clear about why nothing is flowing yet.
    """
    while True:
        existing = set(admin.listTopics().names().get())
        missing = [t for t in topics if t not in existing]
        if not missing:
            return
        log({"msg": "waiting_for_topics", "missing": missing})
        time.sleep(poll_seconds)


def read_topic(spark, bootstrap: str, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "true")
        .load()
    )


def main() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    spark = build_spark("wikiwatch-stream")
    bucket = lake_bucket()

    admin = kafka_admin(spark, bootstrap)
    wait_for_topics(admin)
    session_id = session_id_from_cluster_id(admin.describeCluster().clusterId().get())
    admin.close()
    create_tables(spark)
    loaded = load_ref_if_empty(spark, SEEDS_DIR)
    log({"msg": "startup", "session_id": session_id, "ref_loaded": loaded is not None})

    buffer = progress.ProgressBuffer()
    spark.streams.addListener(progress.ProgressListener(buffer, session_id))
    flusher = progress.Flusher(buffer, progress.hadoop_writer(spark, bucket))
    flusher.start()

    edits = kafka_columns(read_topic(spark, bootstrap, "wiki_edits"))
    dlq = kafka_columns(read_topic(spark, bootstrap, "wiki_edits_dlq"))

    def sink(df, query: str):
        return (
            df.writeStream.queryName(query)
            .trigger(processingTime=TRIGGER)
            .option("checkpointLocation", checkpoint_path(bucket, session_id, query))
        )

    sink(to_bronze(edits, session_id), "bronze_edits").format("iceberg").outputMode(
        "append"
    ).option("fanout-enabled", "true").toTable(BRONZE_EDITS)
    sink(to_bronze_dlq(dlq, session_id), "bronze_dlq").format("iceberg").outputMode(
        "append"
    ).option("fanout-enabled", "true").toTable(BRONZE_DLQ)
    sink(to_silver(edits), "silver_edits").foreachBatch(
        lambda batch, batch_id: merge_into_silver(batch)
    ).start()
    sink(edits_per_minute(to_silver(edits)), "gold_edits_per_min").format("iceberg").outputMode(
        "append"
    ).option("fanout-enabled", "true").toTable(GOLD_EDITS_PER_MIN)
    sink(to_silver(edits), "gold_alerts").foreachBatch(
        lambda batch, batch_id: process_alert_batch(batch)
    ).start()

    try:
        spark.streams.awaitAnyTermination()
    finally:
        flusher.stop()


if __name__ == "__main__":
    main()
