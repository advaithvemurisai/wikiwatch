"""Contract check: the event schema in this branch must be BACKWARD compatible with main.

Registers main's schemas/wiki_edits.json in a scratch subject of a running Schema
Registry (Redpanda), then asks the registry whether this branch's version is compatible.
Same rule the producer sets on the real subject (topics.register_schema).

Usage: python scripts/check_schema_compat.py [--registry http://localhost:18081]
                                             [--base-ref origin/main]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = "schemas/wiki_edits.json"
HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def base_schema(ref: str) -> str | None:
    """The schema at ``ref``, or None if it does not exist there yet."""
    result = subprocess.run(  # noqa: S603 - fixed git command, ref from our own CLI args
        ["git", "show", f"{ref}:{SCHEMA}"],  # noqa: S607
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--registry", default="http://localhost:18081")
    parser.add_argument("--base-ref", default="origin/main")
    args = parser.parse_args()

    current = (ROOT / SCHEMA).read_text()
    base = base_schema(args.base_ref)
    if base is None:
        print(f"{SCHEMA} does not exist on {args.base_ref}: nothing to be compatible with.")
        return 0

    registry = args.registry.rstrip("/")
    subject = f"compat-check-{uuid.uuid4().hex[:8]}"
    httpx.put(
        f"{registry}/config/{subject}", json={"compatibility": "BACKWARD"}, headers=HEADERS
    ).raise_for_status()
    httpx.post(
        f"{registry}/subjects/{subject}/versions",
        json={"schemaType": "JSON", "schema": base},
        headers=HEADERS,
    ).raise_for_status()
    response = httpx.post(
        f"{registry}/compatibility/subjects/{subject}/versions/latest?verbose=true",
        json={"schemaType": "JSON", "schema": current},
        headers=HEADERS,
    )
    response.raise_for_status()
    body = response.json()
    httpx.delete(f"{registry}/subjects/{subject}", headers=HEADERS)
    if body.get("is_compatible"):
        print(f"{SCHEMA} is BACKWARD compatible with {args.base_ref}.")
        return 0
    print(f"{SCHEMA} is NOT backward compatible with {args.base_ref}:")
    for message in body.get("messages", []):
        print(f"  - {message}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
