"""Contract tests for .env.example and the files that read its variables."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = (ROOT / ".env.example").read_text()
COMPOSE_TEXT = (ROOT / "docker-compose.yml").read_text()
SECRET_HINTS = ("KEY", "SECRET", "PASSWORD", "TOKEN")
IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")


def example_vars() -> dict[str, str]:
    pairs = {}
    for line in EXAMPLE.splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            pairs[key.strip()] = value.strip()
    return pairs


def test_every_compose_variable_is_documented_in_env_example():
    used = set(re.findall(r"\$\{(\w+)(?::[-?][^}]*)?\}", COMPOSE_TEXT))
    missing = used - set(example_vars())
    assert not missing, f"add to .env.example: {sorted(missing)}"


def test_secret_values_are_placeholders_only():
    for key, value in example_vars().items():
        if any(hint in key for hint in SECRET_HINTS) and not key.endswith("MEM_LIMIT"):
            assert value == "changeme", f"{key} must be the placeholder 'changeme'"


def test_env_example_contains_no_ip_addresses():
    assert not IPV4.search(EXAMPLE)


def test_trino_catalog_reads_only_variables_compose_provides():
    props = (ROOT / "conf/trino/catalog/iceberg.properties").read_text()
    needed = set(re.findall(r"\$\{ENV:(\w+)\}", props))
    trino_env = yaml.safe_load(COMPOSE_TEXT)["services"]["trino"]["environment"]
    assert needed <= set(trino_env), f"trino needs {sorted(needed - set(trino_env))}"
