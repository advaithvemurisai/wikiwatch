"""Security rule: fixtures never contain IP addresses (CLAUDE.md, BR8)."""

from __future__ import annotations

from pathlib import Path

import pytest

from wikiwatch_producer.privacy import contains_ip

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = sorted(
    p for d in ("tests/e2e/fixtures", "web/fixtures") for p in (ROOT / d).rglob("*") if p.is_file()
)


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: str(p.relative_to(ROOT)))
def test_fixture_has_no_ip_addresses(path):
    for number, line in enumerate(path.read_text(errors="ignore").splitlines(), start=1):
        assert not contains_ip(line), f"{path.name}:{number} contains an IP address"
