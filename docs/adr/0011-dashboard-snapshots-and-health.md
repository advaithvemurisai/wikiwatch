# ADR 0011: Dashboard snapshots, pipeline health and the web app

Status: accepted (2026-10-04)

## Context

Task 8 connects the lake to the public site: Airflow exports small JSON snapshots, and a
Next.js app on Vercel renders them (docs/v1.md, "How data reaches the app"). A few things
the docs leave open had to be decided, and one line of v1.md turned out to be wrong.

## Decisions

1. **The offline banner follows `health.json`, not `meta.json`.** `meta.json` is written
   by `dbt_gold` every 30 minutes, so "older than 15 minutes" would be true for half of
   every running session and the banner would flicker on and off. `health.json` is
   rewritten every 5 minutes by `freshness_monitor`, so its `generated_at` is the
   pipeline heartbeat: older than 15 minutes means three missed runs, which only happens
   when compute is gone. v1.md was changed to say so. (TASKS.md, Task 8, still says
   `meta.json`; v1.md wins.)
2. **One query layer, two engines.** `orchestration/queries.py` holds every export and
   health query as SQL text with literal timestamp bounds (like ADR 0008), so the same
   SQL runs on Trino locally and Athena in the cloud. Every scan of a partitioned table
   carries a bound on its partition column; a unit test checks the SQL text, and the e2e
   test runs `EXPLAIN (TYPE IO)` on Trino to prove each partitioned scan really gets a
   pushed-down constraint (14 scans).
3. **Validate everything before writing anything.** `export_dashboard` builds all three
   documents, validates each against `schemas/dashboard/` and checks its size, and only
   then writes. A failure leaves the previous snapshots in place, so the site keeps
   showing the last good export. Writes go to fixed keys (`dashboard/v1/<name>.json`), so
   a re-run of the same interval simply overwrites: the tasks are idempotent.
4. **Consumer lag without a consumer group.** Spark's Kafka source tracks offsets in its
   checkpoint, not in a consumer group, so there is no group lag to read. Lag per
   partition is Redpanda's latest offset minus the end offset of the newest
   `silver_edits` micro-batch in `ops/stream_progress/` for this session.
   "Received" in the funnel is the sum of the topics' latest offsets (they start at 0
   with every session, because Redpanda is ephemeral). The e2e test checks
   received = Bronze + DLQ and that lag drops to zero once Spark catches up.
5. **dbt lives in its own virtualenv inside a custom Airflow image.** dbt-core and
   Airflow pin conflicting libraries, so dbt runs as a CLI from `/home/airflow/dbt-venv`.
   The rest of the image is installed against Airflow's own `pip freeze` as constraints.
   `dbt_gold` exports even when `dbt build` fails (`trigger_rule=all_done`), and
   `meta.json` then reports the failure from `run_results.json`.
6. **The cloud Airflow UI needs a login.** On EC2 the admin password comes from SSM
   (`AIRFLOW_ADMIN_PASSWORD`), connections and variables from the SSM secrets backend;
   locally the simple auth manager lets everyone in as admin.
7. **The app validates with ajv against the same files.** `npm run build` (and dev,
   type check and tests) copies `schemas/dashboard/*.schema.json` into the app first,
   so the DAG and the site always check the same contract. A different `schema_version`
   is reported as its own problem ("the site does not understand this format yet"),
   separate from corrupt or invalid data. Errors never reach the page; the server log
   holds only the snapshot name and the error class.
8. **Static pages with incremental regeneration.** Every route is prerendered and
   revalidates every 5 minutes, so page views never call S3, the site survives traffic
   spikes for free, and a destroyed compute stack only means the snapshots stop changing.
   "Time ago" values are computed at render time on the server and passed to client
   components, so server and browser agree.
9. **Plain CSS with tokens, no CSS framework.** The palette (light and dark) is a
   validated reference palette; status is always an icon plus a label, every chart has a
   table view, and charts follow one axis per chart.

## Consequences

- A schema change is a coordinated change: bump `schema_version`, update the builders,
  the schemas, the TypeScript types and the fixtures together. CI fails if the fixtures,
  the Python builders or the app disagree with `schemas/dashboard/`.
- `web/fixtures/` are regenerated with `python tests/e2e/run_e2e.py --write-web-fixtures`
  and are exactly what the e2e export wrote to the lake.
- The fixture snapshots are old by design, so previews and `make web` always show the
  offline banner; that is the state a reviewer sees after `demo-down`, too.
- Freshness uses event time for Silver and Gold, so a replay of old events shows those
  layers as stale even while Bronze is fresh. That is correct: the newest event in the
  lake is old.
