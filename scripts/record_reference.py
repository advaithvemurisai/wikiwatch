"""Record a reference list of event IDs from the live stream, for the resume check.

Runs as an independent second consumer of the same stream while the producer is killed
and restarted. It stores only meta.id and meta.dt (no users, titles or comments), so the
output holds no personal data. Output goes to the git-ignored tmp/ folder.

Usage: WIKIWATCH_USER_AGENT="WikiWatch/0.1 (<contact>)" \
       python scripts/record_reference.py --seconds 600
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from wikiwatch_producer.config import DEFAULT_STREAM_URL
from wikiwatch_producer.sse import SSEClient

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, default=600)
    parser.add_argument("--out", type=Path, default=ROOT / "tmp/reference_ids.jsonl")
    args = parser.parse_args()

    user_agent = os.environ.get("WIKIWATCH_USER_AGENT", "")
    if "(" not in user_agent:
        print("Set WIKIWATCH_USER_AGENT to 'WikiWatch/0.1 (<contact>)' first.")
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.seconds
    last_id, count = None, 0
    client = SSEClient(DEFAULT_STREAM_URL, user_agent)
    with args.out.open("w") as out:
        while time.monotonic() < deadline:
            try:
                for sse_event in client.stream(last_id):
                    last_id = sse_event.id or last_id
                    try:
                        meta = json.loads(sse_event.data).get("meta", {})
                    except ValueError:
                        continue
                    if meta.get("domain") == "canary" or "id" not in meta:
                        continue
                    out.write(json.dumps({"id": meta["id"], "dt": meta.get("dt")}) + "\n")
                    count += 1
                    if time.monotonic() >= deadline:
                        break
            except Exception as exc:  # noqa: BLE001 - keep recording through network blips
                print(f"reconnecting after {type(exc).__name__}")
                time.sleep(1)
    client.close()
    print(f"recorded {count} event IDs to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
