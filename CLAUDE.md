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
| Spark Kafka connector / kafka-clients | 4.1.3 / 3.9.1 |
| hadoop-aws (s3a) + AWS SDK modules | 3.4.2 + s3-transfer-manager, apache-client 2.54.17 |
| Java (host, for Spark unit tests) | OpenJDK 17 (Homebrew) |
| Trino | 483 |
| Airflow | 3.3.2 (python3.13 image) |
| Postgres (local catalog + Airflow metadata) / JDBC driver | 18.6 / 42.7.13 |
| Terraform | 1.16.4 |
| gitleaks / pre-commit / ruff / sqlfluff / pytest | 8.30.1 / 4.6.2 / 0.16.10 / 4.4.0 / 9.1.1 |
| Producer image / libs | python:3.13.16-slim; confluent-kafka 2.15.1, httpx 0.28.1, jsonschema 4.26.0, boto3 1.43.108 |
| dbt-core / dbt-trino / dbt-athena | 1.12.5 / 1.10.6 / 1.11.1 |
| Node (Task 8) | 24 LTS |

Measured peak memory per profile (2026-10-03, M1 Air 8 GB, Docker VM 6 GB, `docker stats`
sampled every second; macOS swap stayed flat at about 4.6 GB and memory pressure stayed
normal, 35 to 42% free):

| Profile | Limits (sum) | Measured peak | Measured during |
| --- | --- | --- | --- |
| core | 4.4 GB | about 1.0 GB | `make smoke` (Spark job running) |
| dbt (Trino) | 2.0 GB | about 0.9 GB | `make smoke` (Trino query) |
| airflow (Airflow + Postgres) | 2.0 GB | about 1.1 GB | idle, no DAGs yet |

Under load (2026-10-03, Task 3: streaming app + producer catching up 1 h + Trino queries):
Spark about 1.1 GB, SeaweedFS 382 MB (so its limit was raised from 384 MB to 512 MB),
Iceberg REST 260 MB, Redpanda 215 MB, Console 170 MB, producer 72 MB, Trino about 1.2 GB.
Container peaks summed to about 3.4 GB. macOS memory pressure stayed normal (38% free), but
total swap use rose to about 6.9 GB, so close other heavy apps while the stack runs.

Task 4 (2026-10-03, five streaming queries + producer + Trino): Spark peaked at 2.2 GB of
its 2.5 GB limit, so the limit was raised to 3 GB; Postgres 40 MB, Iceberg REST 170 MB,
SeaweedFS 370 MB; all containers together about 4.2 GB; memory pressure normal (32% free).

## Environment

- Development machine: M1 MacBook Air. Use ARM64 or multi-arch images only.
- Every Docker Compose service has a memory limit. Compose profiles keep the stack small:
  `core` (Redpanda, SeaweedFS, Iceberg REST catalog backed by Postgres, Spark) must fit in
  6 GB; `airflow` and `dbt` (Trino) are separate profiles of about 2 GB each. The core
  Postgres also holds Airflow's metadata (ADR 0007). On an 8 GB machine, never run
  more than `core` plus one extra profile. Always start services through `make` targets.
- The producer has its own small `producer` profile (192 MB limit). It starts only through
  `make produce MODE=fresh|resume` and is never auto-restarted, because a restart in the
  wrong mode could silently skip events.
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
6. Spark checkpoints live under a per-session S3 path (`_checkpoints/<session_id>/<query>`, where
   `session_id` is Redpanda's cluster ID) and are never reused across sessions.
7. Silver dedup is an insert-only `MERGE` on `meta_id`, pruned to the event-hour partitions present
   in the micro-batch (duplicates share `meta.dt`, so this is exact for live data and replays;
   see docs/adr/0005-silver-dedup-and-session-id.md).
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
| `make test` | Unit and contract tests, plus dbt unit tests when Trino is up (loud SKIPPED otherwise) |
| `make e2e` | End-to-end replay test on a throwaway `wikiwatch-e2e` stack (stop the dev stack first) |
| `make lint` | ruff, sqlfluff, terraform fmt, tflint |
| `make secrets-check` | gitleaks on the working tree and history |
| `make web` | Run the Next.js app locally on fixture snapshots |
| `make smoke` | Spark writes an Iceberg table, Trino reads it (needs `make up-dbt`) |
| `make produce MODE=fresh\|resume` | Start the SSE producer (`fresh` only for the very first run or after long gaps) |
| `make produce-stop` / `make producer-logs` | Stop the producer gracefully / follow its JSON logs |
| `make spark-logs` | Follow the streaming app (it starts with `make up` and waits for topics) |
| `make replay FILE=...` | Publish a recorded JSONL file through the producer's publish path |
| `make check-lake` | Trino checks: Silver duplicates, Bronze offset gaps, Bronze-to-Silver completeness |
| `make load-ref` | Reload `ref.watchlist` / `ref.alert_rules` from `dbt/seeds/` (applies next micro-batch) |
| `make alert-scenario` | Replay the scripted edits; exact expected alerts and detection latency |
| `make dbt-build` / `make dbt-docs` | dbt models and all dbt tests on local Trino / lineage docs in `dbt/target/` |

## Verified facts (fill in from the first live session)

| Fact | Value | Verified on |
| --- | --- | --- |
| Event rate, all wikis (events per second, p50 and peak) | p50 about 42.5, peak 49.7 (30 s windows, 18 min sample, Saturday 21:04 to 21:22 UTC); one sample, not yet a daily profile | 2026-10-03 (live run) |
| Temporary account name format | `~YYYY-NNNNN-NN` (e.g. `~2026-` + 5 digits + `-` + 2 digits); 6 of 400 sampled events, 0 IP editors | 2026-10-03 (live sample) |
| Wikimedia stream history window for resume | 7 to 31 days per Wikimedia docs; a 70 s resume gap was verified live, longer gaps not yet | 2026-10-03 (docs + live 70 s gap) |
| Redpanda Schema Registry JSON Schema support | Supported, including BACKWARD compatibility checks (a new required field is rejected with REQUIRED_ATTRIBUTE_ADDED) | 2026-10-04 (live) |

Until a fact is verified, treat it as an assumption and flag code that depends on it.
