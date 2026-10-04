"""Tests for scripts/init_env_local.py."""

from __future__ import annotations

import base64
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("init_env_local", ROOT / "scripts/init_env_local.py")
init_env_local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(init_env_local)

EXAMPLE = (
    "# comment = changeme\nCATALOG_TYPE=rest\nS3_SECRET_KEY=changeme\nAIRFLOW_FERNET_KEY=changeme\n"
)


def parse(text: str) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#")
    )


def test_placeholders_are_replaced_and_other_lines_kept():
    out = init_env_local.render(EXAMPLE)
    values = parse(out)
    assert values["CATALOG_TYPE"] == "rest"
    assert values["S3_SECRET_KEY"] != "changeme" and len(values["S3_SECRET_KEY"]) >= 24
    assert "# comment = changeme" in out


def test_fernet_key_is_valid():
    key = parse(init_env_local.render(EXAMPLE))["AIRFLOW_FERNET_KEY"]
    assert len(base64.urlsafe_b64decode(key)) == 32


def test_each_run_generates_different_secrets():
    a = parse(init_env_local.render(EXAMPLE))["S3_SECRET_KEY"]
    b = parse(init_env_local.render(EXAMPLE))["S3_SECRET_KEY"]
    assert a != b


def test_existing_values_are_never_overwritten(tmp_path):
    example, target = tmp_path / "example", tmp_path / "target"
    example.write_text(EXAMPLE)
    target.write_text("CATALOG_TYPE=glue\nS3_SECRET_KEY=mine\nAIRFLOW_FERNET_KEY=mine\n")
    assert init_env_local.main(["--example", str(example), "--target", str(target)]) == 0
    assert target.read_text() == "CATALOG_TYPE=glue\nS3_SECRET_KEY=mine\nAIRFLOW_FERNET_KEY=mine\n"


def test_new_keys_are_appended_with_fresh_secrets_and_only_names_printed(tmp_path, capsys):
    example, target = tmp_path / "example", tmp_path / "target"
    example.write_text(EXAMPLE + "NEW_DB_PASSWORD=changeme\nNEW_SETTING=on\n")
    target.write_text("CATALOG_TYPE=rest\nS3_SECRET_KEY=mine\nAIRFLOW_FERNET_KEY=mine\n")
    init_env_local.main(["--example", str(example), "--target", str(target)])
    values = parse(target.read_text())
    assert values["S3_SECRET_KEY"] == "mine"  # untouched
    assert values["NEW_SETTING"] == "on"
    assert values["NEW_DB_PASSWORD"] != "changeme" and len(values["NEW_DB_PASSWORD"]) >= 24
    printed = capsys.readouterr().out
    assert "NEW_DB_PASSWORD" in printed and values["NEW_DB_PASSWORD"] not in printed


def test_secrets_are_never_printed_and_file_is_private(tmp_path, capsys):
    example, target = tmp_path / "example", tmp_path / "target"
    example.write_text(EXAMPLE)
    init_env_local.main(["--example", str(example), "--target", str(target)])
    printed = capsys.readouterr().out
    for value in parse(target.read_text()).values():
        if value != "rest":
            assert value not in printed
    assert oct(target.stat().st_mode & 0o777) == "0o600"
