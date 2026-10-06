"""The time a DAG run computes its snapshots for.

Scheduled runs have a logical date. Manual runs in Airflow 3 have none (logical_date is
None), so they fall back to the run's trigger time, run_after. Both are UTC; snapshots use
naive UTC datetimes, rounded to the second.
"""

from __future__ import annotations

from datetime import UTC, datetime


def run_time(logical_date: datetime | None, run_after: datetime | None) -> datetime:
    when = logical_date or run_after
    if when is None:
        raise ValueError("the DAG run has neither a logical date nor a run_after time")
    if when.tzinfo is not None:
        when = when.astimezone(UTC).replace(tzinfo=None)
    return when.replace(microsecond=0)
