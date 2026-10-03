"""Detect IP addresses so they never reach fixtures, logs or the dashboard.

The patterns are deliberately broad: a version string such as ``1.2.3.4`` or a time such
as ``12:30:45`` also matches. For privacy filtering, dropping a harmless event is
acceptable; letting an IP through is not.
"""

from __future__ import annotations

import ipaddress
import re

_IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?![\w:])", re.IGNORECASE)


def is_ip_address(value: str) -> bool:
    """Return True if ``value`` is exactly an IPv4 or IPv6 address (an IP editor)."""
    try:
        ipaddress.ip_address(value.strip())
    except ValueError:
        return False
    return True


def contains_ip(text: str) -> bool:
    """Return True if ``text`` contains anything that looks like an IP address."""
    return bool(_IPV4.search(text) or _IPV6.search(text))
