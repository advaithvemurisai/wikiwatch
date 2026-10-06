from datetime import UTC, datetime, timedelta, timezone

import pytest

from orchestration.runtime import run_time


def test_scheduled_runs_use_the_logical_date():
    logical = datetime(2026, 10, 6, 4, 0, tzinfo=UTC)
    after = datetime(2026, 10, 6, 4, 0, 3, tzinfo=UTC)
    assert run_time(logical, after) == datetime(2026, 10, 6, 4, 0)


def test_manual_runs_without_a_logical_date_use_run_after():
    after = datetime(2026, 10, 6, 3, 45, 59, 529543, tzinfo=UTC)
    assert run_time(None, after) == datetime(2026, 10, 6, 3, 45, 59)


def test_converts_other_time_zones_to_naive_utc():
    central = timezone(timedelta(hours=-5))
    assert run_time(datetime(2026, 10, 5, 22, 0, tzinfo=central), None) == datetime(
        2026, 10, 6, 3, 0
    )


def test_a_run_without_any_time_fails_loudly():
    with pytest.raises(ValueError):
        run_time(None, None)
