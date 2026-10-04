# ADR 0009: End-to-end replay test design

Status: accepted (2026-10-04)

## Context

v1 promises that a known set of edits produces exactly the expected alerts, with no lost
or duplicated events. Unit tests check pieces; only the real pipeline (producer publish
path, Redpanda, Spark, Iceberg, Trino, dbt) can check the promise. It must run on every
pull request, in under 20 minutes, and fail when dedup or a rule is broken.

## Decisions

1. **A recorded, privacy-scrubbed fixture.** About 5,000 live events, recorded once.
   Events on watched pages are removed (only scripted edits touch them), user names
   become pseudonyms with a random salt that is not stored, free-text and log parameter
   fields are dropped, and anything that looks like an IP is rejected. Injected cases:
   50 duplicates, 20 late events, 1 malformed event, scripted edits on 3 watched pages
   covering R1 to R5, and one event that moves the watermark past every window.
   `scripts/build_e2e_fixture.py` rebuilds it from a raw recording.
2. **Fixed timestamps.** Event times are not shifted to "now", so every alert ID is known
   in advance, including R5's window-based one. dbt runs with variables that widen its
   time windows to cover the fixture's dates.
3. **Two-phase replay.** Late events are only late if the windows query has already
   advanced its watermark past them, which happens at the end of a micro-batch. Phase 1
   is replayed and processed first; phase 2 (late events + watermark flush) follows.
   Within a single batch the late events would not be dropped.
4. **A separate, throwaway stack.** The test runs as Compose project `wikiwatch-e2e`, with
   generated secrets and volumes removed at the end, so it never touches the dev lake or
   the producer's stream position. It refuses to run while the dev stack holds the ports.
   The micro-batch trigger is 10 seconds instead of 1 minute (`STREAM_TRIGGER`).
5. **Exact assertions, computed from the fixture.** Silver unique and complete, exactly
   the expected alert IDs, exactly 1 DLQ row, Bronze row count (duplicates kept), final
   windows equal to the fixture's true counts, real-time windows equal to those counts
   minus the late events, plus `dbt build` and `check_lake.py`. The expectation code has
   its own unit tests. A missing table is reported as a failure, not a crash.

## Evidence

- Local run: passed in about 2 minutes (157 final windows, 152 real-time, 9 late edits
  dropped by the watermark).
- Breaking Silver dedup fails it (51 duplicate meta_ids; dbt uniqueness tests fail).
- Breaking R1 fails it (missing alert ID; the dbt reconciliation test also fails).
- The schema contract check fails a new required field (`REQUIRED_ATTRIBUTE_ADDED`).

## Consequences

- The fixture is about 600 KB gzipped and stays in the repo; it must be rebuilt (and the
  expected file regenerated) if the schema or the watchlist changes in a way that touches it.
- Lateness here is relative to event time within the fixture; `is_late` (ingestion vs
  event time) is not asserted because every replayed event is old by definition.
