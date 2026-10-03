"""Producer settings, read from environment variables (never hardcoded)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .validate import DEFAULT_SCHEMA_PATH

DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


class ConfigError(ValueError):
    """A required setting is missing or invalid."""


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str
    schema_registry_url: str
    user_agent: str
    state_bucket: str
    state_key: str = "state/producer/last_event_id"
    stream_url: str = DEFAULT_STREAM_URL
    s3_endpoint: str | None = None
    aws_region: str = "us-east-1"
    schema_path: Path = DEFAULT_SCHEMA_PATH
    checkpoint_seconds: float = 30.0
    log_seconds: float = 30.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        """Build the config; raise ConfigError naming the first problem found.

        Wikimedia requires a descriptive User-Agent with contact details, so it must be
        set explicitly (in .env.local locally, SSM in the cloud) rather than defaulted.
        """
        env = os.environ if env is None else env

        def required(name: str) -> str:
            value = env.get(name, "").strip()
            if not value:
                raise ConfigError(f"{name} is not set")
            return value

        user_agent = required("WIKIWATCH_USER_AGENT")
        if user_agent == "changeme" or "(" not in user_agent:
            raise ConfigError(
                "WIKIWATCH_USER_AGENT must look like 'WikiWatch/0.1 (<contact URL or email>)'"
            )
        return cls(
            kafka_bootstrap=required("KAFKA_BOOTSTRAP"),
            schema_registry_url=required("SCHEMA_REGISTRY_URL"),
            user_agent=user_agent,
            state_bucket=required("STATE_BUCKET"),
            state_key=env.get("STATE_KEY", cls.state_key),
            stream_url=env.get("STREAM_URL", DEFAULT_STREAM_URL),
            s3_endpoint=env.get("S3_ENDPOINT") or None,
            aws_region=env.get("AWS_REGION", "us-east-1"),
            schema_path=Path(env.get("WIKIWATCH_SCHEMA_PATH", str(DEFAULT_SCHEMA_PATH))),
            checkpoint_seconds=float(env.get("CHECKPOINT_SECONDS", "30")),
            log_seconds=float(env.get("LOG_SECONDS", "30")),
        )
