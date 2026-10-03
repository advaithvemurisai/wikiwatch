"""Message keys for the wiki_edits topic (invariant 5: keyed by wiki + title)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def message_key(event: Mapping[str, Any]) -> str:
    """Return the partition key for an event: ``"<wiki>:<title>"``.

    Keying by page spreads load across partitions (a few wikis produce most traffic)
    while keeping every edit of one page in order on one partition. Wiki IDs never
    contain ``:``, so the key splits unambiguously at the first colon even when the
    title has one (``enwiki:Talk:Foo``).

    Raises:
        KeyError: If ``wiki`` or ``title`` is missing (validation runs first).
    """
    return f"{event['wiki']}:{event['title']}"
