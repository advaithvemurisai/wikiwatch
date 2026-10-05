"""Print only the error messages from a Terraform log, with identifying details masked.

Workflows send Terraform's own output to a file, never to the public log, because it holds
ARNs, account IDs and network details. When a command fails, this prints the `Error:`
blocks so the cause is visible, with ARNs, 12-digit account IDs and IPv4 addresses masked.

Usage: python3 scripts/tf_errors.py <terraform output file>
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

MAX_LINES_PER_ERROR = 8
MASKS = (
    (re.compile(r"arn:aws[a-z-]*:[^\s\"',)]*"), "<arn>"),
    (re.compile(r"\b\d{12}\b"), "<account>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?:/\d{1,2})?\b"), "<ip>"),
)


def mask(line: str) -> str:
    for pattern, replacement in MASKS:
        line = pattern.sub(replacement, line)
    return line


def error_blocks(text: str) -> list[list[str]]:
    """Each `Error:` line plus the indented detail lines that follow it."""
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for raw in text.splitlines():
        line = raw.strip(" │╷╵")
        if line.startswith("Error:"):
            current = [line]
            blocks.append(current)
        elif current is None:
            continue
        elif raw.lstrip().startswith("╵") or not raw.strip():
            current = None  # end of the error box (or of an unboxed error)
        elif line and len(current) < MAX_LINES_PER_ERROR:
            current.append(line)
    return blocks


def main() -> int:
    blocks = error_blocks(Path(sys.argv[1]).read_text(errors="replace"))
    if not blocks:
        print("Terraform failed without an Error: message (timed out or interrupted).")
    for block in blocks:
        print("\n".join(mask(line) for line in block))
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
