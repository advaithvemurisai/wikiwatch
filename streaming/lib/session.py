"""Build the SparkSession and its Iceberg catalog from environment variables.

The same code runs locally (Iceberg REST catalog + SeaweedFS) and on EC2 (AWS Glue + S3).
Only the env file changes: CATALOG_TYPE picks the catalog, and nothing here holds
credentials. S3 credentials come from the standard AWS environment variables locally and
from the EC2 instance role in the cloud.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

CATALOG_NAME = "lake"
SUPPORTED_CATALOG_TYPES = ("rest", "glue")


def catalog_conf(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return the Spark config entries for the `lake` Iceberg catalog.

    Args:
        env: Environment to read from. Defaults to ``os.environ``.

    Raises:
        ValueError: If CATALOG_TYPE is unknown or a required variable is missing.
    """
    env = os.environ if env is None else env
    catalog_type = env.get("CATALOG_TYPE", "").strip().lower()
    if catalog_type not in SUPPORTED_CATALOG_TYPES:
        raise ValueError(
            f"CATALOG_TYPE must be one of {SUPPORTED_CATALOG_TYPES}, got {catalog_type!r}"
        )
    warehouse = _require(env, "WAREHOUSE")

    prefix = f"spark.sql.catalog.{CATALOG_NAME}"
    conf = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": catalog_type,
        f"{prefix}.warehouse": warehouse,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.defaultCatalog": CATALOG_NAME,
        "spark.sql.session.timeZone": "UTC",
    }
    if catalog_type == "rest":
        conf[f"{prefix}.uri"] = _require(env, "CATALOG_URI")
        conf[f"{prefix}.s3.endpoint"] = _require(env, "S3_ENDPOINT")
        conf[f"{prefix}.s3.path-style-access"] = "true"
    return conf


def build_spark(app_name: str, env: Mapping[str, str] | None = None):
    """Create (or reuse) the SparkSession with the `lake` catalog configured.

    PySpark is imported here, not at module level, so `catalog_conf` stays testable
    without a JVM.
    """
    from pyspark.sql import SparkSession

    env = os.environ if env is None else env
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    builder = builder.config("spark.driver.memory", env.get("SPARK_DRIVER_MEMORY", "1g"))
    # Micro-batches are small (tens of events per second): a few shuffle partitions
    # avoid hundreds of tiny tasks and tiny files per batch.
    builder = builder.config("spark.sql.shuffle.partitions", "4")
    for key, value in {**catalog_conf(env), **s3a_conf(env)}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def s3a_conf(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Hadoop s3a settings for streaming checkpoints and ops files.

    Locally s3a points at SeaweedFS with path-style URLs; on EC2 (no S3_ENDPOINT) it uses
    real S3. Credentials always come from the default AWS chain (env vars locally, the
    instance role in the cloud), never from config.
    """
    env = os.environ if env is None else env
    conf = {
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.endpoint.region": env.get("AWS_REGION", "us-east-1"),
    }
    endpoint = env.get("S3_ENDPOINT", "").strip()
    if endpoint:
        conf["spark.hadoop.fs.s3a.endpoint"] = endpoint
        conf["spark.hadoop.fs.s3a.path.style.access"] = "true"
        conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
            endpoint.startswith("https")
        ).lower()
    return conf


def lake_bucket(env: Mapping[str, str] | None = None) -> str:
    """Return the bucket name from WAREHOUSE (``s3://<bucket>/...``)."""
    env = os.environ if env is None else env
    warehouse = _require(env, "WAREHOUSE")
    match = re.fullmatch(r"s3a?://([a-z0-9][a-z0-9.-]+)(/.*)?", warehouse)
    if not match:
        raise ValueError("WAREHOUSE must look like s3://<bucket>/")
    return match.group(1)


def session_id_from_cluster_id(cluster_id: str) -> str:
    """Turn a Kafka cluster ID into a path-safe session ID.

    Redpanda generates a new cluster ID whenever the broker is created from scratch and
    keeps it across plain restarts. Using it as the session ID gives exactly the rule in
    invariant 6: a Spark restart reuses its checkpoint, a new broker starts a new one.
    """
    session = re.sub(r"[^A-Za-z0-9-]+", "-", cluster_id).strip("-")
    if not session:
        raise ValueError("empty Kafka cluster ID")
    return session


def checkpoint_path(bucket: str, session_id: str, query: str) -> str:
    """Per-session, per-query checkpoint location: _checkpoints/<session_id>/<query>."""
    return f"s3a://{bucket}/_checkpoints/{session_id}/{query}"


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value
