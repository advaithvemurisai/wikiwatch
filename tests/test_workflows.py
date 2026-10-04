"""GitHub Actions hygiene: pinned actions, least-privilege tokens, no input injection, and
Terraform output kept out of public logs."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = ROOT / ".github/workflows"
WORKFLOWS = {p.name: p.read_text() for p in sorted(WORKFLOW_DIR.glob("*.yml"))}
ACTIONS = {
    p.relative_to(ROOT).as_posix(): p.read_text()
    for p in sorted((ROOT / ".github/actions").glob("*/action.yml"))
}
ALL_FILES = {**WORKFLOWS, **ACTIONS}


def load(text: str) -> dict:
    data = yaml.safe_load(text)
    # PyYAML reads the bare key `on` as the boolean True.
    if True in data:
        data["on"] = data.pop(True)
    return data


def test_expected_workflows_exist():
    assert {"ci.yml", "terraform-plan.yml", "demo-up.yml", "demo-down.yml"} <= set(WORKFLOWS)
    assert "nightly-destroy.yml" in WORKFLOWS


@pytest.mark.parametrize("name", sorted(ALL_FILES))
def test_third_party_actions_are_pinned_to_a_commit_sha(name):
    for ref in re.findall(r"uses:\s*([^\s#]+)", ALL_FILES[name]):
        if ref.startswith("./"):
            continue
        assert re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", ref), f"{name}: {ref} is not pinned"


@pytest.mark.parametrize("name", sorted(WORKFLOWS))
def test_default_token_is_read_only(name):
    assert load(WORKFLOWS[name])["permissions"] == {"contents": "read"}


@pytest.mark.parametrize("name", sorted(WORKFLOWS))
def test_id_token_only_on_jobs_that_assume_a_role(name):
    for job_name, job in load(WORKFLOWS[name])["jobs"].items():
        text = yaml.safe_dump(job)
        assumes_role = "configure-aws-credentials" in text
        has_id_token = job.get("permissions", {}).get("id-token") == "write"
        assert assumes_role == has_id_token, f"{name}:{job_name}"


@pytest.mark.parametrize("name", sorted(ALL_FILES))
def test_no_expressions_from_user_input_inside_run_scripts(name):
    """Inputs reach shell scripts through env vars only, never by text substitution."""
    data = load(ALL_FILES[name])
    jobs = data.get("jobs", {}) or {"composite": data.get("runs", {})}
    for job in jobs.values():
        for step in job.get("steps", []):
            run = step.get("run", "")
            assert "${{" not in run, f"{name}: step {step.get('name')} expands an expression"


@pytest.mark.parametrize("name", sorted(ALL_FILES))
def test_terraform_output_never_reaches_the_log(name):
    """plan/apply/init output holds ARNs and network details; only the summary is printed."""
    for line in ALL_FILES[name].splitlines():
        if re.match(r"\s*terraform (init|plan|apply|show)\b", line):
            assert ">" in line, f"{name}: {line.strip()}"


@pytest.mark.parametrize("name", ["terraform-plan.yml", "demo-up.yml", "demo-down.yml"])
def test_account_id_is_masked(name):
    assert "mask-aws-account-id: true" in WORKFLOWS[name]


def test_nightly_destroy_also_masks_and_runs_on_a_schedule():
    data = load(WORKFLOWS["nightly-destroy.yml"])
    assert data["on"]["schedule"] == [{"cron": "0 6 * * *"}]
    assert "mask-aws-account-id: true" in WORKFLOWS["nightly-destroy.yml"]


@pytest.mark.parametrize("name", ["demo-up.yml", "demo-down.yml", "nightly-destroy.yml"])
def test_compute_workflows_share_one_concurrency_group_and_run_from_main(name):
    data = load(WORKFLOWS[name])
    assert data["concurrency"] == {"group": "compute-stack", "cancel-in-progress": False}
    for job in data["jobs"].values():
        assert job["if"] == "github.ref == 'refs/heads/main'"


def test_plans_on_prs_are_read_only_and_skip_forks():
    data = load(WORKFLOWS["terraform-plan.yml"])
    assert data["on"]["pull_request"]["paths"][0] == "infra/**"
    plan = data["jobs"]["plan"]
    assert "head.repo.full_name == github.repository" in plan["if"]
    assert "AWS_PLAN_ROLE_ARN" in yaml.safe_dump(plan)
    assert "AWS_DEPLOY_ROLE_ARN" not in WORKFLOWS["terraform-plan.yml"]
    assert "-lock=false" in WORKFLOWS["terraform-plan.yml"]
    assert not re.search(r"^\s*terraform apply", WORKFLOWS["terraform-plan.yml"], re.M)


def test_soak_flag_is_set_only_after_a_successful_apply():
    steps = load(WORKFLOWS["demo-up.yml"])["jobs"]["up"]["steps"]
    names = [s.get("name", "") for s in steps]
    assert names.index("Apply") < names.index("Set the soak flag (48 hours)")
