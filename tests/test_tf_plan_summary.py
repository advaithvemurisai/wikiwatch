"""scripts/tf_plan_summary.py: CI prints addresses and actions only, never values."""

from __future__ import annotations

import json

from scripts.tf_plan_summary import main, summarize

SECRET_LOOKING = "arn:aws:iam::ACCOUNT:role/example"


def change(address: str, actions: list[str], rtype: str = "aws_s3_bucket", mode: str = "managed"):
    return {
        "address": address,
        "mode": mode,
        "type": rtype,
        "change": {"actions": actions, "after": {"arn": SECRET_LOOKING, "bucket": "lake-x"}},
    }


def test_summary_lists_addresses_and_counts_without_values():
    plan = {
        "resource_changes": [
            change("aws_s3_bucket.lake", ["create"]),
            change("aws_iam_role.ec2", ["update"]),
            change("aws_instance.session", ["delete", "create"], "aws_instance"),
            change("aws_vpc.old", ["delete"], "aws_vpc"),
            change("aws_glue_catalog_database.lake", ["no-op"]),
            change("data.aws_caller_identity.current", ["read"], mode="data"),
        ]
    }
    lines, problems = summarize(plan)
    assert lines == [
        "  + aws_s3_bucket.lake",
        "  ~ aws_iam_role.ec2",
        "  -/+ aws_instance.session",
        "  - aws_vpc.old",
        "Plan: 2 to add, 1 to change, 2 to destroy.",
    ]
    assert problems == []
    assert not any(SECRET_LOOKING in line or "lake-x" in line for line in lines)


def test_no_changes():
    lines, problems = summarize({"resource_changes": [change("aws_vpc.s", ["no-op"])]})
    assert lines == ["No changes.", "Plan: 0 to add, 0 to change, 0 to destroy."]
    assert problems == []


def test_resources_that_store_secrets_fail_the_plan():
    plan = {
        "resource_changes": [
            change("aws_ssm_parameter.fernet", ["create"], "aws_ssm_parameter"),
            change("aws_iam_access_key.ci", ["create"], "aws_iam_access_key"),
        ]
    }
    _, problems = summarize(plan)
    assert len(problems) == 2


def test_deleting_a_secret_resource_is_allowed():
    """Removing a leftover secret resource is the fix, so it must not block the plan."""
    _, problems = summarize({"resource_changes": [change("x.y", ["delete"], "aws_ssm_parameter")]})
    assert problems == []


def test_sensitive_outputs_fail_the_plan():
    plan = {"output_changes": {"password": {"after_sensitive": True}, "ok": {}}}
    _, problems = summarize(plan)
    assert problems == ["output password is sensitive; Terraform must not output secrets"]


def test_main_exit_codes(tmp_path, capsys):
    clean = tmp_path / "clean.json"
    clean.write_text(json.dumps({"resource_changes": [change("aws_vpc.s", ["create"])]}))
    assert main([str(clean)]) == 0
    assert "aws_vpc.s" in capsys.readouterr().out

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"resource_changes": [change("p.x", ["create"], "random_password")]}))
    assert main([str(bad)]) == 1
    assert "secret" in capsys.readouterr().err
