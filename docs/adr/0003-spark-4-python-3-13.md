# ADR 0003: Spark 4.1 and Python 3.13

Status: accepted (2026-10-03)

## Context

The original stack named Spark 3.5 and Python 3.11. On 2026-10-03:

- Spark 3.5 is in an extended LTS period with security fixes only, ending November 2027.
  The newest lines are 4.1.3 and 4.2.0.
- Iceberg 1.12.0 (the latest release) ships Spark runtimes for 3.5, 4.0 and 4.1, not 4.2.
- Python 3.11 is security-only until October 2027. dbt-athena supports Python up to 3.13;
  PySpark, Airflow 3.3, dbt-core and dbt-trino support 3.13 as well.

## Decision

- Spark 4.1.x (Scala 2.13, Java 17) with `iceberg-spark-runtime-4.1_2.13` 1.12.x.
- Python 3.13 for every Python component (producer, Spark jobs, dbt, Airflow, tests).

Spark 4.2 waits until Iceberg ships a runtime for it.

## Consequences

- Spark 4 drops Scala 2.12 and needs Java 17+, so jar coordinates use `_2.13`.
- Spark 4 enables ANSI SQL mode by default: bad casts fail instead of returning null.
  Parsing code in Task 3 must handle that on purpose (`try_cast`), and the DLQ path
  must catch it.
- `dropDuplicatesWithinWatermark` and the Structured Streaming APIs used in Tasks 3 and 4
  are available in Spark 4.1.
- Unit tests with a local SparkSession need Java 17 on the host (needed by Task 3).

Sources: https://spark.apache.org/versioning-policy.html,
https://devguide.python.org/versions/, Maven Central and PyPI metadata (checked 2026-10-03).
