"""Contract tests for docker-compose.yml: the rules in CLAUDE.md, checked on every PR."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
SERVICES: dict[str, dict] = COMPOSE["services"]

EXPECTED_PROFILES = {
    "core": {
        "redpanda",
        "redpanda-console",
        "seaweedfs",
        "seaweedfs-init",
        "iceberg-rest",
        "spark",
    },
    "airflow": {"airflow-postgres", "airflow"},
    "dbt": {"trino"},
}
# Budgets from CLAUDE.md: core within 6 GB, airflow and dbt about 2 GB each.
BUDGET_MIB = {"core": 6 * 1024, "airflow": 2 * 1024, "dbt": 2 * 1024}


def to_mib(value: str) -> int:
    """Convert a compose memory value (or ${VAR:-default}) to MiB."""
    match = re.fullmatch(r"\$\{\w+:-(\w+)\}", value)
    if match:
        value = match.group(1)
    number, unit = re.fullmatch(r"(\d+)([mg])", value.lower()).groups()
    return int(number) * (1024 if unit == "g" else 1)


def test_every_service_is_in_exactly_the_expected_profile():
    actual: dict[str, set[str]] = {}
    for name, svc in SERVICES.items():
        profiles = svc.get("profiles", [])
        assert len(profiles) == 1, f"{name} must belong to exactly one profile"
        actual.setdefault(profiles[0], set()).add(name)
    assert actual == EXPECTED_PROFILES


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_every_service_has_a_memory_limit(name):
    assert "mem_limit" in SERVICES[name], f"{name} has no mem_limit"
    assert to_mib(str(SERVICES[name]["mem_limit"])) > 0


@pytest.mark.parametrize("profile", sorted(BUDGET_MIB))
def test_profile_memory_within_budget(profile):
    total = sum(to_mib(str(SERVICES[s]["mem_limit"])) for s in EXPECTED_PROFILES[profile])
    assert total <= BUDGET_MIB[profile], f"{profile} limits total {total} MiB"


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_images_are_pinned_to_an_exact_tag(name):
    image = SERVICES[name]["image"]
    repo, _, tag = image.rpartition(":")
    assert repo and tag, f"{name}: image {image!r} has no tag"
    assert tag != "latest" and "latest" not in tag, f"{name}: {image!r} is not pinned"


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_ports_bind_to_loopback_only(name):
    for port in SERVICES[name].get("ports", []):
        assert str(port).startswith("127.0.0.1:"), f"{name} exposes {port} beyond localhost"


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_no_privileged_or_host_network(name):
    svc = SERVICES[name]
    assert not svc.get("privileged", False)
    assert svc.get("network_mode") != "host"


def test_redpanda_is_ephemeral():
    """Invariant 1: nothing may depend on broker state surviving a session."""
    assert "volumes" not in SERVICES["redpanda"]


def test_no_secret_literals_in_compose():
    """Secrets reach containers only through ${VAR} interpolation from the env file."""
    text = (ROOT / "docker-compose.yml").read_text()
    for key in ("PASSWORD", "SECRET", "FERNET_KEY", "ACCESS_KEY"):
        for line in text.splitlines():
            if key in line and ":" in line and not line.strip().startswith("#"):
                value = line.split(":", 1)[1].strip()
                if value and "printf" not in line and "postgresql" not in value:
                    assert value.startswith("${"), f"literal secret-like value: {line.strip()}"
