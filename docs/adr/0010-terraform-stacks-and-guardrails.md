# ADR 0010: Terraform stacks, CI access and cost guardrails

Status: accepted (2026-10-04)

## Context

v1 runs on a $100 credit with a hard stop at $50. The lake must survive between sessions
so the dashboard keeps working, while compute exists only for a few hours at a time. CI
must reach AWS without stored keys, and the repo, the CI logs and the Terraform state are
public or semi-public, so no account ID, IP or secret may show up in any of them.

## Decisions

1. **Three stacks.** `bootstrap` (the state bucket), `foundation` (lake, Glue, Athena,
   IAM, budget; persistent) and `compute` (VPC and one instance; created and destroyed per
   session). Destroying compute can never touch data, and the lake and state buckets carry
   `prevent_destroy`. State lives in S3 with native lock files (`use_lockfile`), no
   DynamoDB. The bucket name is passed at `init`, so it never appears in the repo.
2. **Who applies what.** `bootstrap` and `foundation` are applied once by hand from AWS
   CloudShell (docs/first-apply-checklist.md), because the CI roles are created by
   `foundation` itself. After that, GitHub Actions only plans them. `compute` is applied
   and destroyed only by the workflows, with the deploy role.
3. **Two GitHub OIDC roles.** The plan role trusts only `pull_request` events from this
   repository and can read resource configuration and state, but not lake data. It cannot
   write the lock file, so PR plans run with `-lock=false`. The deploy role trusts only
   `main`, can manage only what `compute` creates, can pass only the instance role, and
   can launch only t4g.xlarge. Fork PRs never get a token.
4. **Plan output stays out of logs.** Full plan and apply output contains ARNs (with the
   account ID) and network details. Workflows send it to `/dev/null` and print
   `scripts/tf_plan_summary.py` instead (addresses and actions only), which also fails a
   plan that would put a secret into state. `mask-aws-account-id` covers error messages.
5. **Budget action cannot stop instances, so it blocks launches instead.** AWS's
   "stop EC2 instances" budget action needs instance IDs fixed when the action is created,
   and every session gets a new ID. At $50 actual the action attaches a deny policy for
   `ec2:RunInstances` (and `rds:CreateDBInstance`, as in docs/plan.md) to the deploy role.
   Running instances are covered by the self-shutdown and nightly-destroy (below). The
   budget excludes credits; otherwise credits net every month to $0 and nothing fires.
6. **Athena's 20 GB/day limit is an alarm, not a cap.** Athena has no daily scan limit
   that Terraform can set (the old per-workgroup data usage controls are not in the
   provider). A CloudWatch alarm on the workgroup's `ProcessedBytes` emails when a UTC day
   passes 20 GB (about $0.10 a month). The 1 GB per-query cutoff is enforced by Athena.
7. **Sessions end themselves.** The boot script arms `shutdown -h +240` before doing
   anything else, and the instance's shutdown behavior is `terminate`, so even a failed
   boot stops billing within 4 hours and leaves no disk behind. A soak test uses 48 hours
   and an SSM flag (`/wikiwatch/soak_until`) that nightly-destroy honours only while it is
   less than 48 hours in the future. Nightly-destroy (06:00 UTC, 1 a.m. Central Daylight
   Time) removes whatever is left.
8. **Instance details.** Spot by default (on-demand for soak), Ubuntu 24.04 arm64 so the
   same ARM64 images as the M1 Mac run unchanged, Docker from Docker's apt repository with
   the signing key checked by fingerprint, standard CPU credits (no surplus-credit
   charges), IMDSv2 with a hop limit of 2 (containers reach the instance role through
   Docker's bridge), no SSH key and no inbound rule. The EC2 role lists the Session Manager
   actions itself because the managed `AmazonSSMManagedInstanceCore` policy would also
   grant read access to every SSM parameter in the account.
9. **Cloud overlay for Compose.** `docker-compose.aws.yml` moves SeaweedFS, the REST
   catalog and Trino to a `local` profile and removes the static S3 key variables from
   Spark and the producer, so the instance role is their only credential. The base file
   requires SeaweedFS's key variables even for services that will not start, so the boot
   script sets them to an unused placeholder.

## Consequences

- "terraform plan passes in CI" depends on the one-time manual setup; until then CI runs
  fmt, validate and tflint only.
- A runaway running instance is stopped by its own timer and by nightly-destroy, not by
  the budget action. The budget action is the guard against new launches after $50.
- docs/plan.md still says the budget action "stops running EC2 and RDS instances" and
  lists a "20 GB daily workgroup limit"; both should point to this ADR.
- The instance starts only the core services. The producer is started by hand over SSM
  (`make produce MODE=... ENV_FILE=.env.aws`), as on the laptop, because the wrong mode can
  skip events. Airflow on EC2 arrives with Task 8.
