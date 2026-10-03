"""Build the SparkSession and its Iceberg catalog from environment variables.

The same code runs locally (Iceberg REST catalog + SeaweedFS) and on EC2 (AWS Glue + S3).
Only the env file changes: CATALOG_TYPE picks the catalog, and nothing here holds
credentials. S3 credentials come from the standard AWS environment variables locally and
from the EC2 instance role in the cloud.
"""

from __future__ import annotations

import os
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
    for key, value in catalog_conf(env).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value
