# ADR 0006: Alerts read Kafka directly; one writer for the ref tables

Status: accepted (2026-10-03)

## Context

Per-edit alerts (R1 to R4) must be detected in under 2 minutes locally (Task 4) and under
5 minutes p95 on AWS (v1 success metric). The v1 diagram draws alerts as a stream-static
join on the Silver table. Reading Silver as a stream chains two 1-minute triggers
(Kafka to Silver, then Silver to alerts) plus two commits, so p95 lands around
2.5 minutes.

The v1 docs also say the watchlist and alert rules are "written by dbt seed", while
Task 4 asks for a Spark loader that writes them, which would give the tables two writers.

## Decision

1. **The alerts query reads `wiki_edits` from Kafka through the same `to_silver`
   transform that builds Silver**, then joins each micro-batch with the ref tables
   (re-read with `REFRESH TABLE` every batch) and inserts new alerts with an
   insert-only MERGE on `alert_id`. It has its own checkpoint, like every other sink.
   Detection takes one trigger instead of two.
2. **The Spark loader (`streaming/lib/ref.py`) is the only writer of `ref.watchlist` and
   `ref.alert_rules`.** The CSVs stay in `dbt/seeds/` (where analysts expect seeds), the
   loader validates them and overwrites each table in one commit, and dbt (Task 5) reads
   the tables as sources instead of running `dbt seed`.
3. **R1 matches real deletions and moves only** (`log_action` delete, delete_redir, move,
   move_redir). The spec's `log_type` delete also covers restores and revision hiding,
   which would page the team with "page deleted" when a page came back.

## Consequences

- Alerts and Silver are built by the same function from the same Kafka records, so the
  reconciliation test (every watched-page Silver edit that matches a rule has an alert)
  still holds. If Silver's transform changes, alerts change with it.
- Each Gold query is one more Kafka consumer of `wiki_edits` (four in total). At about
  40 events per second this costs nothing measurable.
- A threshold or watchlist edit takes effect in the next micro-batch: edit the CSV, run
  `make load-ref`, no restart.
- There is no second writer to drift from the first, and dbt lineage still starts at the
  ref tables.
