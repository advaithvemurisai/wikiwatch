# TASKS.md - WikiWatch v1 build tasks

Run one task per Claude Code session, in order. Paste the task block as the prompt.
Each task starts in plan mode and ends only when its "Done when" list is true.
The "You must understand" list is for you, the owner: do not merge until you can
explain those points without notes.

---

## Task 1 - Repo skeleton and local stack

```
Read CLAUDE.md, docs/v1.md and docs/plan.md first.

Task 1: create the repo skeleton and a working local stack.

Build:
- Folder layout from docs/plan.md (producer, schemas, streaming, dbt, airflow,
  web, infra, tests/e2e, docs/adr, .github/workflows).
- docker-compose.yml with profiles: core (Redpanda + Console, MinIO, Iceberg REST
  catalog, Spark) under 6 GB total; airflow (LocalExecutor + its Postgres) about 2 GB;
  dbt (Trino) about 2 GB. ARM64 images only, memory limit on every service.
  Makefile targets up, up-airflow, up-dbt, down wrap the profiles.
- .env.example with placeholders, .gitignore per the security rules, Makefile with the
  targets listed in CLAUDE.md, pre-commit config with gitleaks, ruff and sqlfluff.
- A smoke script that creates a test Iceberg table via Spark and reads it via Trino.
- Pin exact versions and record them in the CLAUDE.md version table.

Done when:
- make up (core) runs on an 8 GB M1 Air without heavy swapping; core plus one extra
  profile also runs. Record peak memory per profile in CLAUDE.md.
- The smoke script passes. make secrets-check passes.
- README has a "Run locally" section with 3 commands.
```

**You must understand:** why the local catalog is an Iceberg REST catalog and the cloud one is Glue, and how the env file switches between them.

---

## Task 2 - Producer

```
Read CLAUDE.md and the Streaming design and state model sections of docs/plan.md.

Task 2: build the SSE producer.

Build:
- Python producer that reads the Wikimedia recentchange SSE stream with a descriptive
  User-Agent (from config, not hardcoded contact details).
- fresh and resume modes. Last event ID written to object storage (MinIO locally,
  S3 in cloud) every 30 seconds. Reconnect with exponential backoff and jitter.
- JSON Schema in schemas/wiki_edits.json; validate every event; plain JSON to
  wiki_edits keyed by wiki + title; invalid events to wiki_edits_dlq with the reason.
- acks=all, enable.idempotence=true. Topic creation script with 6 partitions.
- Structured JSON logs every 30 seconds: received, published, rejected, reconnects.
- A script that records N live events to tests/e2e/fixtures/ with IP-address users removed.

Tests: unit tests for validation, key building, resume logic (mock the stream),
and DLQ routing.

Done when:
- Running locally for 10 minutes publishes events and logs counters.
- Killing and restarting in resume mode loses no event IDs (prove it with a check script).
- Record the observed event rate in the CLAUDE.md verified facts table.
```

**You must understand:** what `Last-Event-ID` does, why the ID is stored in object storage instead of local disk, and what "at-least-once" means for this producer.

---

## Task 3 - Spark Bronze and Silver

```
Read CLAUDE.md and docs/v1.md data model.

Task 3: one Spark application with the Bronze and Silver queries.

Build:
- streaming/ app with one SparkSession; each sink is its own query with its own
  checkpoint under _checkpoints/<session_id>/<query>.
- Bronze: append raw events plus ingested_at, Kafka partition and offset.
  Bronze DLQ: land wiki_edits_dlq into bronze.wiki_edits_dlq.
- Silver: parse and type fields, compute editor_type (registered, unregistered, bot),
  byte_delta, is_late, processed_at; foreachBatch insert-only MERGE on meta_id,
  pruned to the last 3 hours of event_hour.
- Table DDL with format-version 2 and partitioning by UTC date and hour.
- StreamingQueryListener that buffers progress events and flushes JSON to ops/stream_progress/.

Tests: unit tests for parsing, editor_type, byte_delta, is_late; a local test that
feeds duplicate meta_ids across two batches and asserts one row in Silver.

Done when:
- Running the producer plus Spark for 15 minutes fills Bronze and Silver.
- Silver has zero duplicate meta_id after replaying the same fixture twice.
- Restarting Spark mid-stream resumes from the checkpoint with no gaps.
```

**You must understand:** why insert-only MERGE does not rewrite files, why the MERGE is pruned to 3 hours, and why checkpoints are per session.

---

## Task 4 - Real-time windows and alerts R1 to R4

```
Read CLAUDE.md and the Alert rules section of docs/v1.md.

Task 4: add real-time Gold outputs to the Spark app.

Build:
- gold.edits_per_min: 1-minute tumbling windows per wiki, 2-minute watermark, append.
- dbt seeds for ref.watchlist (60 company and product pages on English Wikipedia,
  no biographies) and ref.alert_rules (thresholds per rule). A loader that writes them
  as Iceberg tables so Spark can read them.
- gold.watched_page_alerts: stream-static join of Silver edits with ref.watchlist and
  ref.alert_rules, applying R1 to R4 exactly as specified. Namespace 0 only; bots never
  raise R2 or R3. Deterministic alert_id; insert-only MERGE on alert_id.

Tests: one unit test per rule, including boundary values; a test that changing a
threshold in ref.alert_rules changes the next micro-batch's result without a restart;
alert_id determinism across reprocessing.

Done when:
- A scripted set of fixture edits produces exactly the expected alerts.
- Measured detection latency locally is under 2 minutes.
```

**You must understand:** how a stream-static join re-reads the static side, why the watermark is 2 minutes, and how `alert_id` prevents duplicates.

---

## Task 5 - dbt models

```
Read CLAUDE.md and docs/v1.md data model and alert rule R5.

Task 5: dbt project on local Trino (dbt-trino) that also runs on Athena (dbt-athena).

Build:
- Profiles using env_var() only. Targets: local (Trino) and aws (Athena).
- Models: gold.burst_alerts (R5, incremental merge on alert_id),
  gold.edits_per_min_final, gold.bot_ratio_hourly, gold.watchlist_daily_digest.
- Every incremental model is partition-pruned.
- Tests: unique and not_null on meta_id and alert_id, accepted_values on
  editor_type and severity, a reconciliation test that every watched-page edit
  matching a rule has an alert row. dbt unit tests for R5 boundary cases.
- Avoid SQL that works in Trino but not in Athena engine v3; note any macro used for that.

Done when:
- dbt build passes locally on data from Tasks 3 and 4.
- dbt docs generate works and shows lineage from seeds and Silver to Gold.
```

**You must understand:** why burst detection is in dbt rather than Spark, and the latency cost of that choice.

---

## Task 6 - End-to-end replay test in CI

```
Read CLAUDE.md and the Testing and CI section of docs/v1.md.

Task 6: CI that proves the pipeline end to end on every pull request.

Build:
- tests/e2e fixture of about 5,000 events with injected duplicates, late events,
  one malformed event and scripted edits on 3 watched pages. No IP addresses.
- make e2e: start a minimal compose profile (Redpanda, MinIO, REST catalog, Spark,
  Trino), replay the fixture through the producer's publish path, run Spark until
  caught up, run dbt, then assert: zero duplicate meta_id, exactly the expected
  alert_ids, 1 DLQ row, window totals match.
- .github/workflows/ci.yml: lint, unit, contract (schema backward compatible with main),
  dbt tests, e2e, gitleaks, and for web/: type check, ESLint, next build, and snapshot
  validation against schemas/dashboard/. Cache images and dependencies.

Done when:
- CI is green on a PR and fails if you deliberately break dedup or a rule.
- Total CI time under 20 minutes.
```

**You must understand:** what each assertion proves, and why a deliberately broken change must turn CI red.

---

## Task 7 - Terraform and workflows

```
Read CLAUDE.md and the Infrastructure, Security and Cost control sections of docs/plan.md.
Never run terraform apply or destroy. Only fmt, validate and plan.

Task 7: infrastructure as code for v1 (no RDS).

Build:
- infra/bootstrap: state bucket (private, versioned, encrypted), native S3 locking.
- infra/foundation: lake bucket (no versioning, lifecycle rules, TLS-only policy,
  public access blocked), Glue databases ref, bronze, silver, gold, ops, Athena workgroup
  (1 GB per query, 20 GB per day), IAM roles (EC2, GitHub OIDC restricted to this repo,
  Vercel OIDC role with s3:GetObject on dashboard/ only, trusted for the production
  environment only), budget with alerts at 10, 25, 40 and a 50 dollar stop action.
- infra/compute: VPC, public subnet, S3 gateway endpoint, t4g.xlarge (spot for sessions,
  on-demand option), no inbound rules, SSM access, user data that installs Docker,
  pulls the repo at a tag, loads secrets from SSM and runs compose, 4-hour self shutdown
  unless soak mode is set.
- default_tags Project = wiki-lakehouse on every resource.
- Workflows: demo-up and demo-down (workflow_dispatch), nightly-destroy (cron),
  terraform plan on PRs touching infra/.

Done when:
- terraform validate and plan pass for all stacks in CI.
- No secret value appears in any plan output or state.
- A written checklist in docs/ for the first manual apply of bootstrap and foundation.
```

**You must understand:** why the stacks are split, how OIDC avoids stored keys, and every guardrail that stops a surprise bill.

---

## Task 8 - Airflow DAGs, snapshot export and the Next.js app

```
Read CLAUDE.md and the v1 dashboard section of docs/v1.md, including
"How data reaches the app".

Task 8: orchestration, the snapshot contract and the public web app.

Build:
- Airflow DAGs: dbt_gold (every 30 minutes: dbt build on the right target, then export
  alerts.json, baseline.json and meta.json to dashboard/v1/) and freshness_monitor
  (every 5 minutes: Silver max event_ts, consumer lag per partition, writes
  ops/lag_snapshots/ and dashboard/v1/health.json). Both idempotent. Secrets through
  the SSM secrets backend in the cloud and env vars locally.
- schemas/dashboard/: JSON Schema per snapshot with schema_version. The export task
  validates before writing; row caps keep each file under 1 MB.
- web/: Next.js App Router app in TypeScript with 3 pages (Alerts, Baseline, Pipeline
  health) exactly as specified. One server-side data module reads snapshots from S3
  (via Vercel OIDC credentials), MinIO or web/fixtures/, chosen by env var; it
  validates every snapshot (zod or ajv) and revalidates every 5 minutes. Recharts for
  charts. Offline banner when meta.json is older than 15 minutes. Error boundaries with
  a generic message. No AWS code in client components and no secrets in NEXT_PUBLIC_
  variables. No bucket names, ARNs, account IDs, hostnames or IPs on any page.
- web/fixtures/: realistic snapshots generated from the e2e fixture run.
- vercel.json if needed; document the Vercel project settings and OIDC setup in docs/.

Tests: DAG import tests; export tests that every query is partition-pruned and output
validates; web type check, lint and build; a test that a snapshot with a wrong
schema_version shows a friendly error, not a crash.

Done when:
- make web shows all three pages on fixtures; the app also works against MinIO.
- A Vercel preview deploy builds from a PR using fixtures only.
- After compute is destroyed, the production site still loads and shows the banner.
```

**You must understand:** why the site never queries Athena, how Vercel OIDC avoids stored keys, and why the snapshot schema is a data contract just like the event schema.
