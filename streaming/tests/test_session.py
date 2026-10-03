"""Tests for the env-driven Iceberg catalog config (no JVM needed)."""

from __future__ import annotations

import pytest

from streaming.lib.session import (
    catalog_conf,
    checkpoint_path,
    lake_bucket,
    s3a_conf,
    session_id_from_cluster_id,
)

REST_ENV = {
    "CATALOG_TYPE": "rest",
    "CATALOG_URI": "http://iceberg-rest:8181",
    "WAREHOUSE": "s3://warehouse/",
    "S3_ENDPOINT": "http://seaweedfs:8333",
}


def test_rest_catalog_points_at_rest_server_and_local_s3():
    conf = catalog_conf(REST_ENV)
    assert conf["spark.sql.catalog.lake.type"] == "rest"
    assert conf["spark.sql.catalog.lake.uri"] == "http://iceberg-rest:8181"
    assert conf["spark.sql.catalog.lake.s3.endpoint"] == "http://seaweedfs:8333"
    assert conf["spark.sql.catalog.lake.s3.path-style-access"] == "true"
    assert conf["spark.sql.session.timeZone"] == "UTC"


def test_glue_catalog_uses_aws_defaults_only():
    conf = catalog_conf({"CATALOG_TYPE": "glue", "WAREHOUSE": "s3://lake/"})
    assert conf["spark.sql.catalog.lake.type"] == "glue"
    assert not any(k.endswith((".uri", ".s3.endpoint")) for k in conf)


def test_conf_contains_no_credentials():
    env = {**REST_ENV, "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "y"}
    assert not any("key" in k.lower() or "secret" in k.lower() for k in catalog_conf(env))


@pytest.mark.parametrize("bad", ["", "hive"])
def test_unknown_catalog_type_is_rejected(bad):
    with pytest.raises(ValueError, match="CATALOG_TYPE"):
        catalog_conf({**REST_ENV, "CATALOG_TYPE": bad})


def test_catalog_type_ignores_case_and_whitespace():
    assert (
        catalog_conf({**REST_ENV, "CATALOG_TYPE": " REST "})["spark.sql.catalog.lake.type"]
        == "rest"
    )


@pytest.mark.parametrize("missing", ["CATALOG_URI", "S3_ENDPOINT", "WAREHOUSE"])
def test_rest_requires_its_settings(missing):
    env = {k: v for k, v in REST_ENV.items() if k != missing}
    with pytest.raises(ValueError, match=missing):
        catalog_conf(env)


def test_session_id_is_path_safe_and_stable():
    cluster = "redpanda.2cb8c8d7-ace8-43d8-99ce-48f39658ae74"
    assert session_id_from_cluster_id(cluster) == "redpanda-2cb8c8d7-ace8-43d8-99ce-48f39658ae74"
    assert session_id_from_cluster_id(cluster) == session_id_from_cluster_id(cluster)


def test_empty_cluster_id_is_rejected():
    with pytest.raises(ValueError):
        session_id_from_cluster_id("...")


def test_checkpoints_are_per_session_and_per_query():
    a = checkpoint_path("warehouse", "redpanda-1", "silver_edits")
    b = checkpoint_path("warehouse", "redpanda-2", "silver_edits")
    assert a == "s3a://warehouse/_checkpoints/redpanda-1/silver_edits"
    assert a != b


def test_lake_bucket_comes_from_warehouse():
    assert lake_bucket({"WAREHOUSE": "s3://warehouse/"}) == "warehouse"
    with pytest.raises(ValueError):
        lake_bucket({"WAREHOUSE": "/local/path"})


def test_s3a_points_at_local_s3_only_when_an_endpoint_is_set():
    local = s3a_conf({"S3_ENDPOINT": "http://seaweedfs:8333"})
    assert local["spark.hadoop.fs.s3a.endpoint"] == "http://seaweedfs:8333"
    assert local["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    cloud = s3a_conf({})
    assert "spark.hadoop.fs.s3a.endpoint" not in cloud
    assert not any("secret" in k or "access.key" in k for k in {**local, **cloud})
