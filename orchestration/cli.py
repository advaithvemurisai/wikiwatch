"""Command line for the same functions the Airflow tasks call.

    python -m orchestration.cli export      [--now ISO] [--out-dir DIR] [--run-results PATH]
    python -m orchestration.cli freshness   [--now ISO] [--out-dir DIR]

By default it reads and writes the lake bucket (SeaweedFS locally, S3 in the cloud) and
queries the engine from QUERY_ENGINE. --out-dir writes the same keys to a local folder
instead (used to generate web/fixtures/). --now pins the run time, for fixtures built
from old data.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from .engines import engine_from_env
from .freshness import bootstrap_from_env, kafka_state, run_freshness
from .snapshots import export_dashboard
from .store import LocalStore, store_from_env


def parse_now(value: str | None) -> datetime:
    """Naive UTC run time (all lake timestamps are naive UTC)."""
    if value is None:
        return datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WikiWatch dashboard export and health")
    parser.add_argument("command", choices=("export", "freshness"))
    parser.add_argument("--now", help="run time, ISO-8601 (default: now, UTC)")
    parser.add_argument("--out-dir", type=Path, help="write to a local folder, not the bucket")
    parser.add_argument("--run-results", type=Path, help="dbt run_results.json for meta.json")
    args = parser.parse_args(argv)

    now = parse_now(args.now)
    engine = engine_from_env()
    store = LocalStore(args.out_dir) if args.out_dir else store_from_env()
    if args.command == "export":
        counts = export_dashboard(engine, store, now, args.run_results)
        print(json.dumps({"msg": "dashboard_exported", "at": now.isoformat(), **counts}))
    else:
        health = run_freshness(engine, store, kafka_state(bootstrap_from_env()), now)
        print(json.dumps({"msg": "health_written", "at": now.isoformat(), **health["funnel"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
