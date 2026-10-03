# ADR 0001: Airflow 3 instead of Airflow 2

Status: accepted (2026-10-03)

## Context

The original stack named Airflow 2 (LocalExecutor). Airflow's published support policy
lists Airflow 2 as end of life since 2026-04-22: no security or bug fixes. Airflow 3 is
the maintained line (first release 2025-04-22).

Source: https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html
(checked 2026-10-03).

## Decision

Use Airflow 3 with the LocalExecutor and a Postgres metadata database, pinned to an exact
patch release in Task 1.

## Consequences

- The project runs a supported version, so security fixes keep coming and the choice is easy
  to defend.
- Airflow 3 splits the old webserver into an API server, and tasks talk to it through the
  Task Execution API instead of reading the metadata database directly. DAG code
  in Task 8 must follow Airflow 3 imports (`airflow.sdk`), not Airflow 2 examples.
- The SSM secrets backend (Amazon provider) is still available, so the secrets design in
  `docs/plan.md` does not change.
- Most online tutorials still show Airflow 2 syntax; check the Airflow 3 docs when writing DAGs.
