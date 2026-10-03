"""Local SparkSession with an Iceberg catalog on a temp folder (no Docker, no S3)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
BREW_JAVA_17 = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if "JAVA_HOME" not in os.environ and BREW_JAVA_17.exists():
        os.environ["JAVA_HOME"] = str(BREW_JAVA_17)
    # Workers must run the same Python as the driver (the venv's), not whatever is on PATH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("wikiwatch-tests")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
