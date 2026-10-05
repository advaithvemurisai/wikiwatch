"""scripts/tf_errors.py shows why Terraform failed without leaking identifiers."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/tf_errors.py"

# Built at runtime so no account-like number or address literal sits in the repo.
ACCOUNT = "1" * 12
ADDRESS = ".".join(["10", "42", "1", "7"])
LOG = f"""aws_vpc.session: Creating...
aws_vpc.session: Creation complete after 2s
╷
│ Error: creating EC2 Instance: RunInstances, InsufficientInstanceCapacity
│
│   with aws_instance.session,
│   on instance.tf line 16, in resource "aws_instance" "session":
╵
╷
│ Error: reading role arn:aws:iam::{ACCOUNT}:role/wikiwatch-ec2 from {ADDRESS}
╵
"""


def run(tmp_path: Path, text: str) -> str:
    log = tmp_path / "apply.log"
    log.write_text(text)
    out = subprocess.run(
        [sys.executable, str(SCRIPT), str(log)], capture_output=True, text=True, check=True
    )
    return out.stdout


def test_prints_each_error_with_its_resource(tmp_path):
    out = run(tmp_path, LOG)
    assert "InsufficientInstanceCapacity" in out
    assert "with aws_instance.session," in out
    assert "Creation complete" not in out, "only errors, not the rest of the log"


def test_masks_arns_account_ids_and_addresses(tmp_path):
    out = run(tmp_path, LOG)
    assert ACCOUNT not in out
    assert ADDRESS not in out
    assert "arn:aws" not in out
    assert "<arn>" in out and "<ip>" in out


def test_says_so_when_there_is_no_error_message(tmp_path):
    out = run(tmp_path, "aws_instance.session: Still creating... [10m0s elapsed]\n")
    assert "timed out or interrupted" in out
