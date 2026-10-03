"""Create the Redpanda topics and register the event schema (idempotent).

Redpanda is ephemeral (invariant 1), so every session starts with an empty broker. The
producer calls ``ensure_topics`` and ``register_schema`` at startup; scripts/create_topics.py
does the same from the command line.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int


WIKI_EDITS = TopicSpec("wiki_edits", partitions=6, retention_ms=7 * DAY_MS)
WIKI_EDITS_DLQ = TopicSpec("wiki_edits_dlq", partitions=1, retention_ms=14 * DAY_MS)
TOPICS = (WIKI_EDITS, WIKI_EDITS_DLQ)
SCHEMA_SUBJECT = f"{WIKI_EDITS.name}-value"


class TopicMismatchError(RuntimeError):
    """An existing topic does not match its spec (for example wrong partition count)."""


def ensure_topics(admin, specs: tuple[TopicSpec, ...] = TOPICS) -> list[str]:
    """Create missing topics; verify existing ones. Returns the names it created."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import NewTopic

    new = [
        NewTopic(
            spec.name,
            num_partitions=spec.partitions,
            replication_factor=1,  # single broker by design (local and EC2)
            config={"retention.ms": str(spec.retention_ms)},
        )
        for spec in specs
    ]
    created = []
    for name, future in admin.create_topics(new, request_timeout=15).items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    existing = admin.list_topics(timeout=10).topics
    for spec in specs:
        actual = len(existing[spec.name].partitions)
        if actual != spec.partitions:
            raise TopicMismatchError(
                f"{spec.name} has {actual} partitions, expected {spec.partitions}"
            )
    return created


def register_schema(
    registry_url: str, schema_text: str, subject: str = SCHEMA_SUBJECT, http=httpx
) -> int:
    """Register the JSON Schema (same text returns the same ID) and set BACKWARD mode."""
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    base = registry_url.rstrip("/")
    response = http.put(
        f"{base}/config/{subject}", json={"compatibility": "BACKWARD"}, headers=headers
    )
    response.raise_for_status()
    response = http.post(
        f"{base}/subjects/{subject}/versions",
        json={"schemaType": "JSON", "schema": schema_text},
        headers=headers,
    )
    response.raise_for_status()
    return int(response.json()["id"])
