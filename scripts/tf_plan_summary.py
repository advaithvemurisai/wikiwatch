"""Print a Terraform plan as addresses and actions only, and refuse plans that hold secrets.

CI logs are public, and full plan output contains ARNs (with the account ID), bucket names
and instance details. The workflows therefore send `terraform plan` output to /dev/null and
print this summary of `terraform show -json <planfile>` instead. It never prints a value.

It also enforces "Terraform never creates or outputs secret values": a plan that would put a
secret into state (an SSM parameter, a Secrets Manager value, an access key, a generated
password or key) or that declares a sensitive output fails.

Usage: terraform show -json tfplan > plan.json && python scripts/tf_plan_summary.py plan.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

# Resource types whose state would contain a secret value.
SECRET_RESOURCE_TYPES = frozenset(
    {
        "aws_ssm_parameter",
        "aws_secretsmanager_secret_version",
        "aws_iam_access_key",
        "aws_iam_user_login_profile",
        "random_password",
        "tls_private_key",
    }
)

SYMBOLS = {
    ("create",): "+",
    ("delete",): "-",
    ("update",): "~",
    ("delete", "create"): "-/+",
    ("create", "delete"): "+/-",
}


def summarize(plan: dict) -> tuple[list[str], list[str]]:
    """Return (summary lines, problems) for a `terraform show -json` plan document."""
    lines: list[str] = []
    problems: list[str] = []
    counts: Counter[str] = Counter()

    for change in plan.get("resource_changes", []):
        actions = tuple(change["change"]["actions"])
        if change.get("mode") == "data" or actions in {("no-op",), ("read",)}:
            continue
        symbol = SYMBOLS.get(actions, "?")
        lines.append(f"  {symbol} {change['address']}")
        if "create" in actions:
            counts["add"] += 1
        if "update" in actions:
            counts["change"] += 1
        if "delete" in actions:
            counts["destroy"] += 1
        if change["type"] in SECRET_RESOURCE_TYPES and "create" in actions:
            problems.append(f"{change['address']} would store a secret value in state")

    for name, output in plan.get("output_changes", {}).items():
        if output.get("after_sensitive") is True:
            problems.append(f"output {name} is sensitive; Terraform must not output secrets")

    total = f"Plan: {counts['add']} to add, {counts['change']} to change, "
    total += f"{counts['destroy']} to destroy."
    return [*lines, total] if lines else ["No changes.", total], problems


def main(argv: list[str] | None = None) -> int:
    """Print the summary; exit 1 if the plan would put a secret into state or outputs."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("plan_json", type=Path, help="output of terraform show -json <planfile>")
    args = parser.parse_args(argv)

    lines, problems = summarize(json.loads(args.plan_json.read_text()))
    print("\n".join(lines))
    for problem in problems:
        print(f"ERROR: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
