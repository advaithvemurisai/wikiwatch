# ADR 0004: Iceberg REST catalog locally, AWS Glue in the cloud

Status: accepted (2026-10-03)

## Context

Iceberg needs a catalog: the service that maps a table name to its current metadata file
and makes each commit an atomic swap of that pointer. Spark (writer) and Trino or Athena
(readers) must share the same catalog, or they see different versions of a table.

In the cloud the catalog is AWS Glue, because Athena reads Iceberg tables only through
Glue. Glue cannot run on a laptop.

## Decision

- **Local:** the Apache Iceberg REST catalog reference server
  (`apache/iceberg-rest-fixture`), storing its state in SQLite on a Docker volume, with
  data files in SeaweedFS.
- **Cloud:** the Glue Data Catalog, with data files in S3.
- **Switch:** one environment variable. `CATALOG_TYPE=rest` (`.env.local`) or
  `CATALOG_TYPE=glue` (`.env.aws`). `streaming/lib/session.py` turns it into Spark catalog
  settings; the table name `lake.<db>.<table>` is the same in both.

## Why a REST catalog locally

- The REST protocol is Iceberg's standard catalog API, and Spark, Trino and most engines
  speak it natively. Spark and Trino see the same commits with no extra glue code.
- It is one small container (about 150 MB measured) instead of a Hive metastore plus its
  database.
- Rejected: a Hadoop catalog (file-based). It only works safely on file systems with atomic
  rename, which S3-style storage does not offer, and Trino does not support it.

## Consequences

- The local catalog and Glue are different implementations, so catalog-specific behaviour
  (for example Glue's table name rules: lowercase, no hyphens) is only checked in the cloud.
  Keep table and column names lowercase snake_case.
- `apache/iceberg-rest-fixture` is a reference server meant for testing. That is fine for
  local work and CI, and it never runs in the cloud.
- The local catalog lives on a Docker volume. `docker compose down -v` deletes it together
  with the SeaweedFS data, so both always stay consistent.
