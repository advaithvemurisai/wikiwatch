"""Kafka (Redpanda) publisher with idempotent, fully acknowledged delivery."""

from __future__ import annotations

from collections import Counter


class KafkaPublisher:
    """Wraps confluent_kafka.Producer and counts delivery results per topic.

    ``acks=all`` waits for every in-sync replica, and ``enable.idempotence`` stops the
    client's own retries from writing a message twice. Together with checkpoint-after-flush
    this gives at-least-once delivery from the stream to Redpanda.
    """

    def __init__(self, bootstrap_servers: str, extra_config: dict | None = None) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 50,
                "client.id": "wikiwatch-producer",
                **(extra_config or {}),
            }
        )
        self.delivered: Counter[str] = Counter()
        self.failed = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
        else:
            self.delivered[msg.topic()] += 1

    def produce(self, topic: str, key: str | None, value: str) -> None:
        """Queue one message; a full local queue is drained before retrying once."""
        try:
            self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
        except BufferError:
            self._producer.poll(1.0)
            self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)

    def poll(self) -> None:
        """Serve delivery callbacks without blocking."""
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        """Block until all queued messages are acknowledged; return how many remain."""
        return self._producer.flush(timeout)
