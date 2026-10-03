# ADR 0005: Silver dedup scope and the streaming session ID

Status: accepted (2026-10-03)

## Context

The producer is at-least-once, so Silver must drop repeats (insert-only `MERGE` on
`meta_id`). The original rule pruned the MERGE target to "the last 3 hours of
`event_hour`". Measured from the current time, that breaks every replay of old data:
the e2e fixture (Task 6), replaying a fixture twice (Task 3), a Bronze backfill, and a
resume after a long gap. The earlier copies sit outside the window, so duplicates get in.

Spark checkpoints must survive a Spark restart within a session but never be reused
across sessions (invariant 6), because Redpanda is rebuilt empty each session and its
offsets restart at 0. "Session" had no concrete definition.

## Decision

1. **Dedup scope = the event hours present in the micro-batch.** A duplicate is the same
   Wikimedia event read twice, so it has the same `meta.dt` as the original and lands in
   the same `hours(event_ts)` partition. Each MERGE matches only those partitions.
   Verified in the query plan: the Iceberg scan carries
   `filters=event_ts >= ..., event_ts < ...` and reads only `meta_id` and `event_ts`.
   Rows are also deduplicated inside each batch first, because an insert-only MERGE
   would insert both copies of a duplicate that arrives twice in one batch.
2. **Session ID = Redpanda's cluster ID** (path-safe form). Redpanda generates a new one
   whenever the broker is created from scratch and keeps it across plain restarts
   (verified 2026-10-03). Checkpoints live at `_checkpoints/<session_id>/<query>`, so a
   Spark restart resumes, and a new broker starts fresh checkpoints automatically.
   Bronze rows carry `session_id` because Kafka offsets repeat across sessions.

## Consequences

- Replays, backfills and long resumes dedup exactly, and each MERGE scans less than a
  3-hour window would.
- Correctness depends on duplicates having identical `meta.dt`. That holds for repeats
  of the same Wikimedia event; the daily dbt uniqueness test on `meta_id` (Task 5) is
  the safety net if it ever stops holding.
- A batch spanning many hours (a long catch-up) produces a long OR filter. It is
  bounded by `maxOffsetsPerTrigger` (200,000 records per batch).
- Restarting Redpanda in place keeps the session; recreating it (`make down`/`make up`,
  a new EC2 instance) starts a new one. Nothing has to be configured by hand.
