"""docker-compose.aws.yml: on EC2 the local stand-ins never start and nothing needs them."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
LOCAL_ONLY = {"seaweedfs", "seaweedfs-init", "iceberg-rest", "trino"}


class Override:
    """A value tagged !override in the overlay: it replaces the base value."""

    def __init__(self, value):
        self.value = value


class ComposeLoader(yaml.SafeLoader):
    """SafeLoader that understands Compose's merge tags."""


def _override(loader: yaml.SafeLoader, node: yaml.Node) -> Override:
    if isinstance(node, yaml.SequenceNode):
        return Override(loader.construct_sequence(node, deep=True))
    return Override(loader.construct_mapping(node, deep=True))


ComposeLoader.add_constructor("!override", _override)

BASE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
OVERLAY = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[  # noqa: S506
    "services"
]


def merged() -> dict[str, dict]:
    """Apply the overlay the way Compose does for the keys it uses (!override replaces)."""
    services = {name: dict(svc) for name, svc in BASE.items()}
    for name, patch in OVERLAY.items():
        assert name in services, f"overlay names unknown service {name}"
        for key, value in patch.items():
            assert isinstance(value, Override), f"{name}.{key} must use !override"
            services[name][key] = value.value
    return services


def services_in(profile: str, services: dict[str, dict]) -> set[str]:
    return {name for name, svc in services.items() if profile in svc.get("profiles", [])}


def test_overlay_removes_exactly_the_local_stand_ins_from_core_and_dbt():
    services = merged()
    removed = (services_in("core", BASE) | services_in("dbt", BASE)) - (
        services_in("core", services) | services_in("dbt", services)
    )
    assert removed == LOCAL_ONLY
    assert services_in("local", services) == LOCAL_ONLY


def test_cloud_core_is_redpanda_console_postgres_and_spark():
    assert services_in("core", merged()) == {"redpanda", "redpanda-console", "postgres", "spark"}


def test_nothing_started_on_ec2_depends_on_a_local_stand_in():
    services = merged()
    for name, svc in services.items():
        if name in LOCAL_ONLY:
            continue
        depends = set(svc.get("depends_on", {}))
        assert not depends & LOCAL_ONLY, f"{name} still depends on {depends & LOCAL_ONLY}"


def test_spark_and_producer_get_credentials_only_from_the_instance_role():
    services = merged()
    for name in ("spark", "producer"):
        env = services[name]["environment"]
        assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_ENDPOINT"} & set(env), name


def test_spark_uses_glue_and_the_lake_bucket_on_ec2():
    env = merged()["spark"]["environment"]
    assert env["CATALOG_TYPE"].startswith("${CATALOG_TYPE")
    assert "WAREHOUSE_BUCKET" in env["WAREHOUSE"]
    # Everything else the streaming app reads stays the same as locally.
    for key in ("KAFKA_BOOTSTRAP", "STREAM_TRIGGER", "PYTHONPATH"):
        assert env[key] == BASE["spark"]["environment"][key]


def test_producer_settings_match_local_apart_from_credentials():
    env = merged()["producer"]["environment"]
    base = BASE["producer"]["environment"]
    for key in ("KAFKA_BOOTSTRAP", "SCHEMA_REGISTRY_URL", "WIKIWATCH_USER_AGENT"):
        assert env[key] == base[key]
    assert "WAREHOUSE_BUCKET" in env["STATE_BUCKET"]


def test_cloud_airflow_queries_athena_and_builds_dbt_on_athena():
    env = merged()["airflow"]["environment"]
    assert env["QUERY_ENGINE"] == "athena"
    assert env["DBT_TARGET"] == "aws"
    assert "ATHENA_WORKGROUP" in env and "ATHENA_S3_STAGING_DIR" in env


def test_cloud_airflow_requires_a_login():
    env = merged()["airflow"]["environment"]
    assert env["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] == "False"
    assert env["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"] == "admin:admin"
    command = " ".join(merged()["airflow"]["command"])
    assert "umask 077" in command and "set -x" not in command  # password file never echoed


def test_cloud_airflow_reads_connections_from_ssm():
    env = merged()["airflow"]["environment"]
    assert env["AIRFLOW__SECRETS__BACKEND"].endswith("SystemsManagerParameterStoreBackend")
    assert '"/wikiwatch/airflow/connections"' in env["AIRFLOW__SECRETS__BACKEND_KWARGS"]


def test_cloud_airflow_has_no_static_keys_or_local_endpoints():
    env = merged()["airflow"]["environment"]
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_ENDPOINT", "TRINO_HOST"):
        assert name not in env, name
    assert set(merged()["airflow"]["depends_on"]) == {"postgres"}
