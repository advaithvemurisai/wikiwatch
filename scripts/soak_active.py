"""Decide whether nightly-destroy should skip because a soak test is running.

demo-up with soak=true writes the SSM parameter /wikiwatch/soak_until as a UTC timestamp
48 hours ahead. The flag counts only while it is in the future and at most 48 hours away,
so a typo or a hand-edited far-future value can never keep compute alive indefinitely.

Usage: python scripts/soak_active.py "<value or empty>"   prints true or false.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta

MAX_SOAK = timedelta(hours=48)


def soak_active(value: str, now: datetime | None = None) -> tuple[bool, str]:
    """Return (active, reason) for a soak_until value such as 2026-10-06T07:00:00Z."""
    now = now or datetime.now(UTC)
    value = value.strip()
    if not value or value == "None":
        return False, "no soak flag"
    try:
        until = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False, "soak flag is not a timestamp; ignoring it"
    if until.tzinfo is None:
        return False, "soak flag has no time zone; ignoring it"
    if until <= now:
        return False, "soak flag has expired"
    if until - now > MAX_SOAK:
        return False, "soak flag is more than 48 hours ahead; ignoring it"
    return True, f"soak test running until {until.astimezone(UTC):%Y-%m-%dT%H:%MZ}"


def main(argv: list[str] | None = None) -> int:
    """Print true or false on stdout and the reason on stderr."""
    args = sys.argv[1:] if argv is None else argv
    active, reason = soak_active(args[0] if args else "")
    print("true" if active else "false")
    print(reason, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
