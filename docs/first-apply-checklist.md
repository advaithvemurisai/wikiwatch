# First apply checklist (bootstrap and foundation)

A one-time manual setup. It creates the state bucket and the persistent foundation, then
hands everything else to GitHub Actions. Work through it in **AWS CloudShell** (the browser
terminal in the AWS console), so no AWS keys ever exist on the laptop. Nothing here prints
a secret, and nothing it outputs belongs in the repo, an issue or a screenshot.

Why it is manual: CI reaches AWS through OIDC roles that the foundation stack creates, so
those roles cannot exist before the first apply (ADR 0010).

## 0. Before you start

- [ ] **Upgrade the account to the Paid Plan** (Billing, Account plan). On the Free Plan the
      account closes after 6 months and takes the lake with it. Unused credits carry over.
- [ ] Sign in as an admin IAM Identity Center user or admin IAM user with MFA, not root.
- [ ] Switch the console region to **us-east-1 (N. Virginia)**.
- [ ] In Service Quotas, EC2, check that both "Running On-Demand Standard instances" and
      "All Standard Spot Instance Requests" allow at least 4 vCPUs (one t4g.xlarge). New
      accounts sometimes start lower; request an increase if needed.

## 1. Terraform in CloudShell

Open CloudShell, then install Terraform 1.16.5 and check its checksum:

```bash
TF=1.16.5; ARCH=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
cd ~ && curl -fsSLO "https://releases.hashicorp.com/terraform/${TF}/terraform_${TF}_linux_${ARCH}.zip" \
  && curl -fsSLO "https://releases.hashicorp.com/terraform/${TF}/terraform_${TF}_SHA256SUMS"
sha256sum -c --ignore-missing "terraform_${TF}_SHA256SUMS"   # must print: OK
mkdir -p ~/bin && unzip -o "terraform_${TF}_linux_${ARCH}.zip" terraform -d ~/bin && terraform version
git clone https://github.com/advaithvemurisai/wikiwatch.git ~/wikiwatch
```

## 2. Bootstrap: the state bucket

The first apply uses local state (the bucket does not exist yet), then moves the state into
the bucket it just created.

```bash
cd ~/wikiwatch/infra/bootstrap
printf 'terraform {\n  backend "local" {}\n}\n' > backend_override.tf
terraform init
terraform apply                      # review: 6 resources, all for one bucket; type yes
STATE_BUCKET=$(terraform output -raw state_bucket)
rm backend_override.tf
terraform init -migrate-state -backend-config="bucket=$STATE_BUCKET"   # answer yes
rm -f terraform.tfstate terraform.tfstate.backup
echo "export STATE_BUCKET=$STATE_BUCKET" >> ~/.bashrc
```

- [ ] `terraform plan` in `infra/bootstrap` now says "No changes" (state read from the bucket).

## 3. Session secrets in SSM

The instance reads every `/wikiwatch/env/<NAME>` parameter into its root-only env file at
boot. Terraform never creates them. Values are generated in place and never echoed:

```bash
put() { aws ssm put-parameter --name "/wikiwatch/env/$1" --type SecureString \
          --value "$2" --overwrite > /dev/null && echo "set $1"; }
rand() { python3 -c 'import secrets; print(secrets.token_urlsafe(24))'; }
put POSTGRES_PASSWORD "$(rand)"
put CATALOG_DB_PASSWORD "$(rand)"
put AIRFLOW_DB_PASSWORD "$(rand)"
put AIRFLOW_JWT_SECRET "$(rand)"
put AIRFLOW_API_SECRET_KEY "$(rand)"
put AIRFLOW_ADMIN_PASSWORD "$(rand)"   # Airflow UI login (user: admin); read it later with
                                       # aws ssm get-parameter --with-decryption, in CloudShell
put AIRFLOW_FERNET_KEY "$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
put WIKIWATCH_USER_AGENT 'WikiWatch/0.1 (https://github.com/advaithvemurisai/wikiwatch)'
```

- [ ] `aws ssm get-parameters-by-path --path /wikiwatch/env/ --query 'Parameters[].Name'`
      lists all 8 names (names only, no values).

## 4. Foundation

```bash
cd ~/wikiwatch/infra/foundation
terraform init -backend-config="bucket=$STATE_BUCKET"
export TF_VAR_state_bucket=$STATE_BUCKET
export TF_VAR_vercel_team_slug=<your Vercel team slug>
read -rs -p "Alert email: " TF_VAR_alert_email && export TF_VAR_alert_email && echo
terraform plan -out=tfplan           # review, then:
terraform apply tfplan && rm tfplan
terraform output                     # role ARNs for step 5; stays in CloudShell
```

- [ ] Confirm the SNS subscription email ("AWS Notification - Subscription Confirmation"),
      or the Athena alarm cannot reach you. Budget emails need no confirmation.
- [ ] Billing, Budgets: `wikiwatch-monthly` shows alerts at $10, $25, $40 actual and $50
      forecast, and one action at $50 actual.

## 5. Connect GitHub

In the repository's Settings, Secrets and variables, Actions (web UI, so the values never
pass through the laptop's shell history):

| Kind | Name | Value |
| --- | --- | --- |
| Secret | `AWS_PLAN_ROLE_ARN` | `github_plan_role_arn` output |
| Secret | `AWS_DEPLOY_ROLE_ARN` | `github_deploy_role_arn` output |
| Secret | `TF_STATE_BUCKET` | `$STATE_BUCKET` |
| Secret | `ALERT_EMAIL` | the alert email (so PR plans match the applied config) |
| Variable | `VERCEL_TEAM_SLUG` | the same slug as above |
| Variable | `TF_PLAN_ENABLED` | `true` (turns on the plan job in terraform-plan) |

The ARNs and bucket name are stored as secrets so GitHub masks them in logs, not because
they grant access: only this repository's workflows can assume the roles.

- [ ] Billing, Cost allocation tags: activate `Project` (it appears up to 24 hours after
      the first tagged resource exists).

## 6. Verify

- [ ] Open a pull request that touches `infra/`. In `terraform-plan`, the plan job prints
      "No changes" for bootstrap and foundation and the session resources for compute.
- [ ] Tag a release on main and push it, then run **demo-up** with that tag. After about
      10 minutes, from CloudShell:
      `aws ssm start-session --target <instance id from the demo-up log>`, then
      `sudo tail -n 50 /var/log/wikiwatch-boot.log` ends with "boot finished".
- [ ] On the instance: `cd /opt/wikiwatch && sudo make produce MODE=fresh ENV_FILE=.env.aws`
      for the very first run (`resume` after that).
- [ ] Run **demo-down**; the compute stack is empty afterwards.
- [ ] Run **nightly-destroy** once by hand (workflow_dispatch) with a session up, to prove
      the safety net, as the v1 acceptance criteria require.

## Vercel (after foundation)

- Follow docs/vercel-setup.md: root directory `web`, OIDC federation in team issuer mode,
  and `SNAPSHOT_SOURCE`, `SNAPSHOT_BUCKET` (the `lake_bucket` output), `AWS_REGION` and
  `AWS_ROLE_ARN` (the `vercel_role_arn` output) for the **production** environment only.

## Laptop tools (no AWS access needed)

`make lint` needs tflint 0.64.0. It is not in Homebrew, so install it from the GitHub
release and check its checksum:

```bash
gh release download v0.64.0 -R terraform-linters/tflint -p tflint_darwin_arm64.zip -p checksums.txt
grep ' tflint_darwin_arm64.zip$' checksums.txt | shasum -a 256 -c -   # must print: OK
unzip tflint_darwin_arm64.zip && install -m 755 tflint ~/.local/bin/  # ~/.local/bin on PATH
```
