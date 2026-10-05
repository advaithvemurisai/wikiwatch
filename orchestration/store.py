"""Where snapshots and ops files are read and written: the lake bucket, or a local folder.

S3Store talks to S3 on AWS (instance role) or to SeaweedFS locally (S3_ENDPOINT and the
standard AWS key variables). LocalStore writes the same keys under a folder, which is how
web/fixtures/ is generated.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol


class Store(Protocol):
    def put_json(self, key: str, document: dict) -> None: ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def get_text(self, key: str) -> str: ...


def to_json(document: dict) -> bytes:
    """Compact, stable JSON (sorted keys keep re-runs byte-identical)."""
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")


class S3Store:
    def __init__(self, bucket: str, endpoint_url: str | None, region: str, client=None):
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url or None,
                region_name=region,
                config=Config(s3={"addressing_style": "path"}, retries={"mode": "standard"}),
            )
        self._client = client
        self.bucket = bucket

    def put_json(self, key: str, document: dict) -> None:
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=to_json(document),
            ContentType="application/json",
            CacheControl="no-cache",
        )

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def get_text(self, key: str) -> str:
        body = self._client.get_object(Bucket=self.bucket, Key=key)["Body"]
        return body.read().decode("utf-8")


class LocalStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def put_json(self, key: str, document: dict) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        # Fixtures are committed: indent them so diffs are readable.
        path.write_text(json.dumps(document, sort_keys=True, indent=2) + "\n")

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        folder = base if base.is_dir() else base.parent
        if not folder.exists():
            return []
        keys = (str(p.relative_to(self.root)) for p in folder.rglob("*") if p.is_file())
        return sorted(k for k in keys if k.startswith(prefix))

    def get_text(self, key: str) -> str:
        return (self.root / key).read_text()


def store_from_env(env: Mapping[str, str] | None = None) -> S3Store:
    env = os.environ if env is None else env
    return S3Store(
        bucket=env["WAREHOUSE_BUCKET"],
        endpoint_url=env.get("S3_ENDPOINT") or None,
        region=env.get("AWS_REGION", "us-east-1"),
    )
