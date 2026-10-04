# ADR 0007: Postgres behind the local Iceberg REST catalog

Status: accepted (2026-10-03)

## Context

The local Iceberg REST catalog (ADR 0004) is Apache's reference server
(`apache/iceberg-rest-fixture`), which stores catalog state in SQLite. SQLite allows one
writer at a time. Task 4 raised the number of concurrent committers to five streaming
queries plus Trino. Commits started failing with `SQLITE_BUSY: database is locked`
(HTTP 500 from the catalog), the Spark app exited on the failed query, and the container
restart-looped about every 30 seconds. WAL mode plus a 30 s busy timeout cut the failures
to roughly one every few minutes but did not remove them: SQLite can still refuse a
writer immediately when a read transaction must upgrade to a write.

No data was lost (checkpoints and idempotent Iceberg commits), but a pipeline that
restarts itself is not trustworthy, and it made detection latency unmeasurable.

## Decision

Keep the same REST catalog server and back it with Postgres:

- `docker/iceberg-rest/Dockerfile` extends the pinned fixture image with the PostgreSQL
  JDBC driver (42.7.13, pinned by SHA-256) and starts the same main class with both jars
  on the classpath.
- One `postgres` service moves into the `core` profile. It holds two databases with
  separate login roles: `iceberg_catalog` and `airflow` (`conf/postgres/init-databases.sh`).
  The `airflow` profile no longer has its own Postgres.

Rejected:
- SQLite with WAL and a busy timeout: still failed under load.
- Iceberg's JDBC catalog used directly by Spark and Trino, without the REST server: one
  container fewer, but it reverses ADR 0004 and moves local further from the cloud setup.

## Consequences

- Measured after the change: no catalog errors and no Spark restarts with five
  concurrent queries.
- `core` gains a Postgres container (256 MB limit); `airflow` loses one, so the totals
  barely move.
- `.env.local` needs `CATALOG_DB_PASSWORD` and `AIRFLOW_DB_PASSWORD`; `make env-local`
  adds missing keys with fresh random values and prints only their names.
- The catalog moved to a new database, so tables from before the switch exist only in the
  old SQLite volume. Local data is throwaway; Bronze can be replayed if needed.
- In the cloud nothing changes: Glue is the catalog, and Postgres holds only Airflow's
  metadata on EC2.
