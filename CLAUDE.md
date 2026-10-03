# CLAUDE.md - WikiWatch

## What this project is

WikiWatch is a real-time brand page monitoring system built as a data engineering
portfolio project. It streams every Wikimedia edit, keeps a lakehouse on AWS, and
alerts a (fictional) communications team when a watched Wikipedia page gets a risky edit.

The engineering is the point. Correctness, recoverability, cost control and security
matter more than features.

## Source of truth

- `docs/v1.md` - v1 scope, business story, alert rules, data model, acceptance criteria.
  This wins over everything else while building v1.
- `docs/plan.md` - full technical design (state model, streaming design, security, cost).
- `TASKS.md` - the ordered build tasks.

If a request conflicts with these docs, stop and say so before writing code.
Do not add tools, services or features that the docs do not list. v2/v3 items
(CDC, SCD2, RDS, Slack delivery, maintenance DAGs, soak test) are out of scope for v1.

## Stack

Python 3.13, Redpanda (Kafka API) + Schema Registry, Spark 4.1 Structured Streaming
(local mode, one application), Apache Iceberg v2, AWS Glue catalog + S3 in the cloud,
Iceberg REST catalog + SeaweedFS locally (MinIO is unmaintained; see docs/adr/0002-seaweedfs-local-s3.md), dbt-athena in the cloud and dbt-trino on local
Trino, Athena, Airflow 3 (LocalExecutor; see docs/adr/0001-airflow-3.md), Next.js (App Router, TypeScript, Node 24 LTS) on Vercel,
Terraform, GitHub Actions with OIDC.

Pin exact versions in Task 1 and record them here:

| Component | Version |
| --- | --- |
| Python (host and Spark image) | 3.13 |
| Redpanda / Redpanda Console | v26.2.3 / v3.12.0 |
| SeaweedFS | 4.48 |
| Iceberg REST catalog (`apache/iceberg-rest-fixture`) | 1.10.1 |
| Spark (`apache/spark`, Scala 2.13, Java 17) | 4.1.3 |
| Iceberg Spark runtime / AWS bundle | 1.12.0 / 1.12.0 |
| Trino | 483 |
| Airflow | 3.3.2 (python3.13 image) |
| Postgres (Airflow metadata) | 18.6 |
| Terraform | 1.16.4 |
| gitleaks / pre-commit / ruff / sqlfluff / pytest | 8.30.1 / 4.6.2 / 0.16.10 / 4.4.0 / 9.1.1 |
| Node (Task 8) | 24 LTS |

Measured peak memory per profile (2026-10-03, M1 Air 8 GB, Docker VM 6 GB, `docker stats`
sampled every second; macOS swap stayed flat at about 4.6 GB and memory pressure stayed
normal, 35 to 42% free):

| Profile | Limits (sum) | Measured peak | Measured during |
| --- | --- | --- | --- |
| core | 4.4 GB | about 1.0 GB | `make smoke` (Spark job running) |
| dbt (Trino) | 2.0 GB | about 0.9 GB | `make smoke` (Trino query) |
| airflow (Airflow + Postgres) | 2.0 GB | about 1.1 GB | idle, no DAGs yet |

## Environment

- Development machine: M1 MacBook Air. Use ARM64 or multi-arch images only.
- Every Docker Compose service has a memory limit. Compose profiles keep the stack small:
  `core` (Redpanda, SeaweedFS, Iceberg REST catalog, Spark) must fit in 6 GB; `airflow` and
  `dbt` (Trino) are separate profiles of about 2 GB each. On an 8 GB machine, never run
  more than `core` plus one extra profile. Always start services through `make` targets.
- `.env.local` holds only throwaway local credentials (SeaweedFS, local Postgres). Real cloud
  secrets never exist on the laptop; they live only in SSM. Permission rules are a
  guardrail, not a sandbox, so this is what actually keeps secrets safe.
- The same compose file runs locally (`.env.local`, SeaweedFS) and on EC2 (`.env.aws`, S3).
- All timestamps are UTC. All partitions use UTC dates and hours.

## Design invariants (never break these)

1. Redpanda is ephemeral. Nothing may depend on broker offsets surviving a session.
2. Durable state lives only in Wikimedia's stream history (upstream) and S3 (downstream).
3. The producer stores its last event ID in S3 every 30 seconds and supports `fresh` and `resume` modes.
4. The producer validates events against `schemas/` and publishes plain JSON. Invalid events go to `wiki_edits_dlq`.
5. `wiki_edits` is keyed by `wiki` + `title`, 6 partitions.
6. Spark checkpoints live under a per-session S3 path and are never reused across sessions.
7. Silver dedup is an insert-only `MERGE` on `meta_id`, pruned to the last 3 hours of `event_hour`.
8. Real-time windows: 1-minute tumbling, 2-minute watermark, append mode.
9. Alerts have a deterministic `alert_id`. Reprocessing never creates duplicates.
10. Bronze is append-only. Silver and Gold must be rebuildable from Bronze.
11. Every Airflow task is idempotent.
12. The web app never queries Athena. It reads only JSON snapshots from the S3 `dashboard/`
    prefix (or `web/fixtures/` locally), validated against `schemas/dashboard/`, and keeps
    working when compute is destroyed.

## Security rules

- Never write secrets, passwords, keys, webhooks, account IDs, hostnames or IPs into code,
  config, tests, fixtures, logs, docs or commit messages.
- Local secrets go in `.env.local` (git-ignored). Commit `.env.example` with placeholders only.
- Cloud secrets live in SSM Parameter Store. Terraform never creates or outputs secret values.
- Mark sensitive Terraform variables `sensitive = true`.
- Never print environment variables or config dumps in logs or error messages.
- The dashboard, fixture and README must never contain IP addresses. Show `editor_type` only.
- The web app reaches AWS only through Vercel OIDC federation. AWS calls run only in server
  code. Never put a secret in a `NEXT_PUBLIC_` variable.
- gitleaks must pass before every commit.

## Cost rules

- Never run `terraform apply`, `terraform destroy` or `aws` commands that create resources.
  Deploys happen only through the GitHub Actions workflows.
- No NAT Gateway, no Elastic IP, no EMR, no MWAA, no managed Kafka, no CloudWatch log shipping.
- No S3 versioning on the lake bucket. Lifecycle rules on Athena results and checkpoints.
- Every Athena query from dbt or the snapshot export must be partition-pruned.

## Working style

- One task from `TASKS.md` per session. Start in plan mode: list the files you will create
  or change, the tests you will write, and any assumption you are making. Wait for approval.
- Every change ships with tests. Keep `make test` and the end-to-end replay test green.
- Small commits with clear messages. One logical change per commit.
- Prefer simple, readable code over abstractions. Type hints and docstrings on public functions.
- When a design choice has a real trade-off, draft a short ADR in `docs/adr/`.
- After finishing a task, explain the key logic in plain language so the owner can defend it
  in an interview, and list anything that was assumed rather than verified.
- If something in the docs turns out to be wrong in practice, say so and propose the doc change.
  Do not silently work around it.

## Commands

| Command | Purpose |
| --- | --- |
| `make venv` / `make env-local` | One-time setup: Python 3.13 dev tools, `.env.local` with random local secrets |
| `make up` / `make down` | Start or stop the `core` profile |
| `make up-airflow` | `core` plus Airflow |
| `make up-dbt` | `core` plus Trino for dbt work |
| `make test` | Unit, contract and dbt tests |
| `make e2e` | End-to-end fixture replay test |
| `make lint` | ruff, sqlfluff, terraform fmt, tflint |
| `make secrets-check` | gitleaks on the working tree and history |
| `make web` | Run the Next.js app locally on fixture snapshots |
| `make smoke` | Spark writes an Iceberg table, Trino reads it (needs `make up-dbt`) |

## Verified facts (fill in from the first live session)

| Fact | Value | Verified on |
| --- | --- | --- |
| Event rate, all wikis (events per second, p50 and peak) | | |
| Temporary account name format | | |
| Wikimedia stream history window for resume | | |
| Redpanda Schema Registry JSON Schema support | Supported per Redpanda docs (drafts 04 to 2020-12); live check pending | 2026-10-03 (docs only) |

Until a fact is verified, treat it as an assumption and flag code that depends on it.
