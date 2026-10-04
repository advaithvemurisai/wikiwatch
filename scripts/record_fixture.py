"""Record N live recentchange events to a JSONL fixture, with privacy filtering.

Keeps only valid, non-canary events and drops every event that contains anything
resembling an IP address anywhere (user, comment, title), so fixtures never hold IPs.
Also prints how many editors are temporary accounts (names starting with "~"), which
feeds the "temporary account name format" fact in CLAUDE.md.

Usage: WIKIWATCH_USER_AGENT="WikiWatch/0.1 (<contact>)" \
       python scripts/record_fixture.py --count 500 [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

from wikiwatch_producer.config import DEFAULT_STREAM_URL
from wikiwatch_producer.pipeline import is_canary
from wikiwatch_producer.privacy import contains_ip
from wikiwatch_producer.sse import SSEClient
from wikiwatch_producer.validate import EventValidator

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--out", type=Path, default=ROOT / "tests/e2e/fixtures/recorded.jsonl")
    args = parser.parse_args()

    user_agent = os.environ.get("WIKIWATCH_USER_AGENT", "")
    if "(" not in user_agent:
        print("Set WIKIWATCH_USER_AGENT to 'WikiWatch/0.1 (<contact>)' first.")
        return 2

    validator = EventValidator()
    client = SSEClient(DEFAULT_STREAM_URL, user_agent)
    stats = {"seen": 0, "kept": 0, "invalid": 0, "canary": 0, "dropped_ip": 0, "temp_accounts": 0}
    kept: list[str] = []
    last_id = None
    try:
        while stats["kept"] < args.count:
            try:
                for sse_event in client.stream(last_id):
                    last_id = sse_event.id or last_id
                    stats["seen"] += 1
                    if is_canary(sse_event.data):
                        stats["canary"] += 1
                        continue
                    result = validator.validate(sse_event.data)
                    if not result.ok:
                        stats["invalid"] += 1
                    elif contains_ip(sse_event.data):
                        stats["dropped_ip"] += 1
                    else:
                        kept.append(json.dumps(result.event, ensure_ascii=False))
                        stats["kept"] += 1
                        stats["temp_accounts"] += str(result.event.get("user", "")).startswith("~")
                    if stats["kept"] >= args.count:
                        break
            except httpx.HTTPError:
                stats["reconnects"] = stats.get("reconnects", 0) + 1  # resume via Last-Event-ID
    finally:
        client.close()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(kept) + "\n")
    print(json.dumps(stats))
    print(f"wrote {len(kept)} events to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
