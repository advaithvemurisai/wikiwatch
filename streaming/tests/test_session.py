"""Tests for the env-driven Iceberg catalog config (no JVM needed)."""

from __future__ import annotations

import pytest

from streaming.lib.session import catalog_conf

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
