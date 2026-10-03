"""Durable producer position (invariant 3): the last event ID in object storage.

Stored in S3 (SeaweedFS locally), not on local disk, because the compute instance and
its disk are destroyed after every session while the bucket survives.
"""

from __future__ import annotations

from typing import Protocol


class StateStore(Protocol):
    """Where the producer keeps its last checkpointed event ID."""

    def load(self) -> str | None: ...

    def save(self, last_event_id: str) -> None: ...


class S3StateStore:
    """Keeps the last event ID as a small text object."""

    def __init__(self, client, bucket: str, key: str) -> None:
        self._client = client
        self.bucket = bucket
        self.key = key

    def load(self) -> str | None:
        """Return the stored ID, or None if nothing has been checkpointed yet."""
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self.key)
        except self._client.exceptions.NoSuchKey:
            return None
        value = response["Body"].read().decode("utf-8").strip()
        return value or None

    def save(self, last_event_id: str) -> None:
        """Overwrite the stored ID (single object PUT, atomic on S3)."""
        self._client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=last_event_id.encode("utf-8"),
            ContentType="text/plain",
        )


def s3_client(endpoint_url: str | None, region: str):
    """Build a boto3 S3 client. Credentials come from the standard AWS chain."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or None,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}, retries={"mode": "standard"}),
    )
