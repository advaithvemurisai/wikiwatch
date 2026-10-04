"""scripts/soak_active.py: nightly-destroy skips only for a valid, unexpired soak flag."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.soak_active import main, soak_active

NOW = datetime(2026, 10, 4, 6, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "active"),
    [
        ("2026-10-05T06:00:00Z", True),  # 24 h ahead
        ("2026-10-06T06:00:00Z", True),  # exactly 48 h ahead
        ("2026-10-06T06:00:01Z", False),  # beyond 48 h: ignored
        ("2026-10-04T06:00:00Z", False),  # expired at this instant
        ("2026-10-03T06:00:00Z", False),  # expired
        ("2026-10-05T06:00:00", False),  # no time zone
        ("tomorrow", False),
        ("", False),
        ("None", False),  # what `aws ... --output text` prints for a missing value
    ],
)
def test_soak_active(value, active):
    assert soak_active(value, NOW)[0] is active


def test_main_prints_true_or_false(capsys):
    main([""])
    assert capsys.readouterr().out == "false\n"
