# ADR 0008: One dbt project for local Trino and cloud Athena

Status: accepted (2026-10-04)

## Context

dbt builds the batch Gold models (Task 5): R5 burst alerts, complete per-minute windows,
hourly bot share and the daily digest. It runs on dbt-trino against local Trino and on
dbt-athena against Athena engine v3, which is Trino-based. Every scheduled Athena query
must be partition-pruned (cost rule), and R5's window definition was ambiguous in the docs.

## Decisions

1. **Same SQL on both engines.** Models use only functions that exist in Trino and in
   Athena engine v3 (`count_if`, `date_trunc`, `date_add`, `to_hex(sha256(...))`,
   `format_datetime`). The two real differences are handled in one place each:
   - Iceberg table config lives in `models/gold/schema.yml`, with both adapters' keys
     side by side (`properties` for Trino; `table_type` and `partitioned_by` for Athena).
     Each adapter reads its own keys.
   - `macros/generate_schema_name.sql` writes models to `gold`, not dbt's default
     `<target>_gold`, because Spark and dbt share the same gold database.
2. **Pruning by literal.** `event_time_filter` renders a literal timestamp at compile time
   (run start minus `lookback_hours`, rounded down to the model's grain). A literal, unlike
   a subquery on the target table, is guaranteed to prune Iceberg partitions on both
   engines. Rounding to the grain means a recomputed hour or day is always complete
   before the merge overwrites it. Data tests are likewise limited to `test_window_days`.
3. **Late data.** Incremental runs recompute the last 6 hours of event time and merge.
   Events arriving later than that (only after a long producer resume) need a
   `--full-refresh` or a run with a larger `lookback_hours`.
4. **R5 uses fixed 10-minute windows aligned to the clock.** The alert is deterministic
   (`alert_id = sha256("R5:<wiki>:<title>:<window_start>")`) and matches the docs' "rule,
   page and window" ID. Trade-off: a burst split across a boundary is not flagged; a unit
   test documents that case. Sliding windows are a v2 option. The ref loader rejects a
   `window_minutes` that does not divide 60.
5. **Reconciliation is a second implementation.** A singular test re-implements R1 to R4
   in SQL and requires an alert for every match, so a bug in either the Spark rules or
   the SQL rules shows up as a failing test. It was checked to fail when alerts are
   missing.
6. **The seed CSVs are disabled in dbt.** The Spark loader stays the only writer of
   `ref.*` (ADR 0006); dbt reads them as sources, so lineage starts at the ref tables.

## Consequences

- `dbt build` on local Trino is a faithful check of the Athena SQL, but Athena itself is
  only parse-checked until AWS exists (Task 7). Differences found then go into this ADR.
- Differences found on AWS:
  - **Where dbt-athena keeps table data.** Without `s3_data_dir`, dbt-athena writes table
    data under the query-results prefix, which expires after 7 days; Gold tables broke
    once their files expired. `profiles.yml` now sets `s3_data_dir` to `dbt/` in the lake
    bucket. Tables created before that need one `dbt build --full-refresh` of Gold,
    rebuilt from Silver.
  - **Unit-test temp tables.** Before a unit test, dbt creates an empty table from the
    model to read its column types. That node has none of the model's config, so
    dbt-athena made it a Hive table, which cannot hold `timestamp(6)`, and every R5 unit
    test errored on Athena only. `macros/resolve_table_type.sql` overrides dbt-athena's
    macro to use Iceberg for unit tests; `tests/test_dbt_macros.py` keeps the copy in
    step with the pinned dbt-athena. The boot check runs the unit tests on Athena, which
    is how this was found.
- `make test` runs the dbt unit tests when Trino is up and prints a loud SKIPPED warning
  otherwise; `make dbt-build` runs everything; CI always has Trino.
- sqlfluff lints the models with lint-time stand-ins for the run-time macros
  (`.sqlfluff`), so layout and syntax rules still apply to every model and test.
