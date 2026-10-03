"""Entry point: python -m wikiwatch_producer {fresh|resume}."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from datetime import UTC, datetime

from .config import Config, ConfigError
from .pipeline import Pipeline
from .publisher import KafkaPublisher
from .runner import MODES, ResumeError, run_connections, start_position
from .sse import SSEClient
from .state import S3StateStore, s3_client
from .topics import ensure_topics, register_schema
from .validate import EventValidator


def log(record: dict) -> None:
    """Write one JSON log line. Records hold counts, timings and states only."""
    stamped = {"ts": datetime.now(UTC).isoformat(timespec="seconds"), **record}
    print(json.dumps(stamped), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="wikiwatch_producer")
    parser.add_argument("mode", choices=MODES)
    args = parser.parse_args(argv)

    try:
        config = Config.from_env()
    except ConfigError as exc:
        log({"msg": "config_error", "error": str(exc)})
        return 2

    from confluent_kafka.admin import AdminClient

    created = ensure_topics(AdminClient({"bootstrap.servers": config.kafka_bootstrap}))
    schema_id = register_schema(config.schema_registry_url, config.schema_path.read_text())
    log({"msg": "startup", "mode": args.mode, "topics_created": created, "schema_id": schema_id})

    store = S3StateStore(
        s3_client(config.s3_endpoint, config.aws_region), config.state_bucket, config.state_key
    )
    try:
        start = start_position(args.mode, store)
    except ResumeError as exc:
        log({"msg": "resume_error", "error": str(exc)})
        return 3

    pipeline = Pipeline(
        EventValidator(config.schema_path),
        KafkaPublisher(config.kafka_bootstrap),
        store,
        checkpoint_every=config.checkpoint_seconds,
        log_every=config.log_seconds,
        log=log,
    )
    pipeline.start_from(start)
    client = SSEClient(config.stream_url, config.user_agent)

    # docker stop sends SIGTERM: turn it into SystemExit so the final checkpoint runs.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        run_connections(client.stream, pipeline, log=log)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        pipeline.checkpoint()
        pipeline.log_stats()
        log({"msg": "shutdown", "checkpointed": pipeline.saved_event_id is not None})
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
