"""Static guardrail checks on infra/: the cost and security rules from CLAUDE.md and
docs/plan.md, checked on every PR without AWS access. terraform validate and tflint check
syntax and provider rules; these tests check the project's own rules."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "infra"
STACKS = ("bootstrap", "foundation", "compute")


def stack_text(stack: str) -> str:
    return "\n".join(p.read_text() for p in sorted((INFRA / stack).glob("*.tf")))


def all_infra_text() -> str:
    return "\n".join(stack_text(s) for s in STACKS)


def resource_types(text: str) -> set[str]:
    return set(re.findall(r'^resource "(\w+)"', text, flags=re.MULTILINE))


def block(text: str, header: str) -> str:
    """Return one top-level block, from its header line to the closing brace at column 0."""
    match = re.search(rf"^{re.escape(header)} \{{\n.*?^\}}", text, flags=re.MULTILINE | re.DOTALL)
    assert match, f"block not found: {header}"
    return match.group(0)


@pytest.mark.parametrize("stack", STACKS)
def test_versions_are_pinned_exactly(stack):
    text = stack_text(stack)
    assert 'required_version = "1.16.5"' in text
    assert re.search(r'version\s*=\s*"\d+\.\d+\.\d+"', text), "AWS provider must be pinned"
    assert (INFRA / stack / ".terraform.lock.hcl").exists(), "commit the provider lock file"


@pytest.mark.parametrize("stack", STACKS)
def test_every_resource_is_tagged_through_default_tags(stack):
    provider = block(stack_text(stack), 'provider "aws"')
    assert re.search(r'Project\s*=\s*"wiki-lakehouse"', provider)


@pytest.mark.parametrize("stack", STACKS)
def test_state_is_remote_encrypted_and_natively_locked(stack):
    backend = block(stack_text(stack).replace("  backend", "backend"), 'backend "s3"')
    assert "use_lockfile = true" in backend
    assert "encrypt      = true" in backend or "encrypt = true" in backend
    assert "bucket" not in backend, "the bucket name is passed at init, never committed"
    assert "dynamodb" not in backend


def test_no_forbidden_cost_resources():
    """CLAUDE.md cost rules: no NAT gateway, no Elastic IP, no RDS, EMR, MWAA or MSK."""
    forbidden = {
        "aws_nat_gateway",
        "aws_eip",
        "aws_eip_association",
        "aws_db_instance",
        "aws_rds_cluster",
        "aws_emr_cluster",
        "aws_mwaa_environment",
        "aws_msk_cluster",
        "aws_cloudwatch_log_group",
    }
    assert not resource_types(all_infra_text()) & forbidden


def test_terraform_never_creates_secret_values():
    secret_types = {
        "aws_ssm_parameter",
        "aws_secretsmanager_secret_version",
        "aws_iam_access_key",
        "aws_iam_user",
        "random_password",
        "tls_private_key",
    }
    assert not resource_types(all_infra_text()) & secret_types


@pytest.mark.parametrize("stack", STACKS)
def test_no_output_is_sensitive_and_secret_like_variables_are(stack):
    text = stack_text(stack)
    assert "sensitive = true" not in "\n".join(re.findall(r'^output ".*?^\}', text, re.M | re.S))
    for name, body in re.findall(r'^variable "(\w+)" \{\n(.*?)^\}', text, re.M | re.S):
        if re.search(r"password|secret|token|key$|email", name):
            assert "sensitive   = true" in body or "sensitive = true" in body, name


def test_state_bucket_is_versioned_private_encrypted_and_tls_only():
    text = stack_text("bootstrap")
    assert 'status = "Enabled"' in text
    assert 'sse_algorithm = "AES256"' in text
    assert text.count("= true") >= 4  # all four public access block settings
    assert '"aws:SecureTransport"' in text
    assert "prevent_destroy = true" in text


def test_lake_bucket_rules():
    text = stack_text("foundation")
    assert "aws_s3_bucket_versioning" not in text, "no versioning on the lake bucket"
    assert '"aws:SecureTransport"' in text
    assert "restrict_public_buckets = true" in text
    lifecycle = block(text, 'resource "aws_s3_bucket_lifecycle_configuration" "lake"')
    assert re.search(r"athena_results_prefix\s*\}\s*expiration \{\s*days = 7", lifecycle)
    assert re.search(r"checkpoints_prefix\s*\}\s*expiration \{\s*days = 14", lifecycle)
    assert "days_after_initiation = 1" in lifecycle


def test_glue_databases_and_athena_limit():
    text = stack_text("foundation")
    assert 'glue_databases = ["ref", "bronze", "silver", "gold", "ops"]' in text
    assert "bytes_scanned_cutoff_per_query  = local.one_gb" in text
    assert "enforce_workgroup_configuration = true" in text
    assert 'metric_name         = "ProcessedBytes"' in text
    assert re.search(r'variable "athena_daily_alarm_gb".*?default\s+= 20\n', text, re.S)


def test_budget_thresholds_ignore_credits_and_stop_new_launches():
    text = stack_text("foundation")
    assert "default     = [10, 25, 40]" in text
    assert re.search(r'variable "budget_limit_usd".*?default\s+= 50\n', text, re.S)
    assert "include_credit = false" in text, "credits would hide all spend from the budget"
    assert 'notification_type          = "FORECASTED"' in text
    action = block(text, 'resource "aws_budgets_budget_action" "stop"')
    assert 'approval_model     = "AUTOMATIC"' in action
    assert '"ec2:RunInstances"' in block(text, 'data "aws_iam_policy_document" "budget_stop"')


def test_oidc_roles_are_locked_to_the_repository_and_events():
    text = stack_text("foundation")
    # Immutable subject (owner@id/repo@id), as GitHub sends it for this repository.
    assert 'default     = "repo:advaithvemurisai@219215219/wikiwatch@1403818521"' in text
    assert 'plan   = "${var.github_oidc_subject}:pull_request"' in text
    assert 'deploy = "${var.github_oidc_subject}:ref:refs/heads/main"' in text
    assert "*" not in "".join(re.findall(r'"repo:[^"]*"', text)), "no wildcards in subjects"
    assert ":environment:production" in text


def test_vercel_role_reads_only_the_dashboard_prefix():
    policy = block(stack_text("foundation"), 'data "aws_iam_policy_document" "vercel"')
    assert re.findall(r'"s3:\w+"', policy) == ['"s3:GetObject"']
    assert "dashboard_prefix" in policy


def test_ec2_role_reads_only_wikiwatch_parameters():
    text = stack_text("foundation")
    policy = block(text, 'data "aws_iam_policy_document" "ec2"')
    # The managed Session Manager policy grants ssm:GetParameter on every parameter.
    assert "policy/AmazonSSMManagedInstanceCore" not in text
    assert "aws_iam_role_policy_attachment" not in text
    params = re.search(r'sid\s+= "SessionParameters".*?\}', policy, re.S).group(0)
    assert "resources = [local.ssm_param_arn" in params
    assert re.search(r'ssm_param_arn\s+= ".*:parameter/wikiwatch"\n', text)
    assert not re.search(r'"ssm:GetParameter[^"]*"[^\]]*\]\s*resources\s*=\s*\["\*"\]', policy)


def test_network_has_no_inbound_path():
    text = stack_text("compute")
    types = resource_types(text)
    assert "aws_vpc_security_group_ingress_rule" not in types
    assert "aws_security_group_rule" not in types
    assert not re.search(r"^\s*ingress\s*\{", text, re.M)
    assert 'vpc_endpoint_type = "Gateway"' in text
    assert "key_name" not in text, "no SSH key: access is through SSM only"


def test_instance_guardrails():
    instance = block(stack_text("compute"), 'resource "aws_instance" "session"')
    assert "instance_type          = local.instance_type" in instance
    assert 'instance_type = "t4g.xlarge"' in stack_text("compute")
    assert 'http_tokens   = "required"' in instance
    assert "encrypted             = true" in instance
    assert "delete_on_termination = true" in instance
    assert 'instance_initiated_shutdown_behavior = "terminate"' in instance
    assert 'cpu_credits = "standard"' in instance
    assert "monitoring             = false" in instance
    assert "associate_public_ip_address" not in instance


def test_capacity_shortage_fails_cleanly_and_can_move_zones():
    instance = block(stack_text("compute"), 'resource "aws_instance" "session"')
    assert 'create = "10m"' in instance, "must end before the workflow's apply timeout"
    subnet = block(stack_text("compute"), 'resource "aws_subnet" "public"')
    assert "availability_zone       = local.session_az" in subnet
    assert (
        "contains(data.aws_ec2_instance_type_offerings.session.locations, local.session_az)"
        in subnet
    )


def test_session_limit_is_4_hours_or_48_for_soak():
    text = stack_text("compute")
    assert "shutdown_minutes = var.soak_mode ? 48 * 60 : 4 * 60" in text
    assert 'default     = "spot"' in text


USER_DATA = (INFRA / "compute/templates/user_data.sh.tftpl").read_text()


def test_user_data_arms_shutdown_before_anything_else():
    commands = [
        line
        for line in USER_DATA.splitlines()
        if line.strip() and not line.startswith("#") and not line.startswith(("set ", "exec "))
    ]
    assert commands[1].startswith("shutdown -h +${shutdown_minutes}"), commands[:2]


def test_user_data_never_prints_secrets():
    assert "set -x" not in USER_DATA
    assert not re.search(r"\b(printenv|env\s*$|cat \.env)", USER_DATA, re.M)
    assert "umask 077" in USER_DATA
    assert "--with-decryption" in USER_DATA
    # Secrets go into the env file, not onto the command line or the log.
    assert re.search(r"--output text \\\n\s*\| awk .*>> \.env\.aws", USER_DATA)


def test_user_data_checks_out_a_tag_and_verifies_dockers_key():
    assert '--branch "${repo_tag}"' in USER_DATA
    assert "9DC858229FC7DD38854AE2D88D81803C0EBFCD88" in USER_DATA
    assert "make up-airflow ENV_FILE=.env.aws" in USER_DATA  # core + Airflow on EC2
    # The cloud path is checked at boot, after the services start and before "finished".
    up = USER_DATA.index("make up-airflow ENV_FILE=.env.aws")
    check = USER_DATA.index("make boot-check ENV_FILE=.env.aws")
    assert up < check < USER_DATA.index('echo "boot finished')
    assert "make produce" not in USER_DATA.split("# 5.")[1].split("\n", 3)[-1]


def test_repo_tag_is_validated_before_it_reaches_the_boot_script():
    text = stack_text("compute")
    assert 'can(regex("^v[0-9]+' in text


IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?:/\d{1,2})?\b")
ALLOWED_RANGES = {"10.42.0.0/16", "10.42.1.0/24", "0.0.0.0/0"}


def test_only_private_or_any_address_ranges_appear():
    found = set(IPV4.findall(all_infra_text() + USER_DATA))
    assert found <= ALLOWED_RANGES, found - ALLOWED_RANGES
