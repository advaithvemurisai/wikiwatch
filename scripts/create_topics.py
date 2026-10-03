"""Create the Redpanda topics and register the wiki_edits schema (idempotent).

The producer does this itself at startup; this script is for running it by hand.
Usage: python scripts/create_topics.py [--bootstrap localhost:19092] [--registry URL]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient

from wikiwatch_producer.topics import TOPICS, ensure_topics, register_schema

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--registry", default="http://localhost:18081")
    args = parser.parse_args()

    created = ensure_topics(AdminClient({"bootstrap.servers": args.bootstrap}))
    schema_id = register_schema(args.registry, (ROOT / "schemas/wiki_edits.json").read_text())
    for spec in TOPICS:
        state = "created" if spec.name in created else "already existed"
        print(f"{spec.name}: {spec.partitions} partitions, {state}")
    print(f"schema registered with id {schema_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
