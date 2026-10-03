from __future__ import annotations

import json

import pytest

from wikiwatch_producer.config import Config, ConfigError
from wikiwatch_producer.topics import SCHEMA_SUBJECT, TOPICS, register_schema

ENV = {
    "KAFKA_BOOTSTRAP": "redpanda:9092",
    "SCHEMA_REGISTRY_URL": "http://redpanda:8081",
    "WIKIWATCH_USER_AGENT": "WikiWatch/0.1 (https://example.org/contact)",
    "STATE_BUCKET": "warehouse",
}


def test_config_reads_env_with_defaults():
    config = Config.from_env(ENV)
    assert config.state_key == "state/producer/last_event_id"
    assert config.stream_url.endswith("/v2/stream/recentchange")
    assert config.checkpoint_seconds == 30.0


@pytest.mark.parametrize("missing", sorted(ENV))
def test_missing_setting_is_named(missing):
    env = {k: v for k, v in ENV.items() if k != missing}
    with pytest.raises(ConfigError, match=missing):
        Config.from_env(env)


@pytest.mark.parametrize("agent", ["changeme", "python-httpx/0.28", "WikiWatch"])
def test_user_agent_must_carry_contact_details(agent):
    with pytest.raises(ConfigError, match="WIKIWATCH_USER_AGENT"):
        Config.from_env({**ENV, "WIKIWATCH_USER_AGENT": agent})


def test_topic_specs_match_the_design():
    specs = {t.name: t for t in TOPICS}
    assert specs["wiki_edits"].partitions == 6  # invariant 5
    assert specs["wiki_edits"].retention_ms == 7 * 86_400_000
    assert specs["wiki_edits_dlq"].retention_ms == 14 * 86_400_000


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeHttp:
    def __init__(self):
        self.calls = []

    def put(self, url, json, headers):
        self.calls.append(("PUT", url, json))
        return FakeResponse({})

    def post(self, url, json, headers):
        self.calls.append(("POST", url, json))
        return FakeResponse({"id": 7})


def test_schema_registration_sets_backward_compatibility_and_registers_json():
    http = FakeHttp()
    schema = json.dumps({"type": "object"})
    assert register_schema("http://registry:8081/", schema, http=http) == 7
    (m1, url1, body1), (m2, url2, body2) = http.calls
    assert (m1, url1, body1) == (
        "PUT",
        f"http://registry:8081/config/{SCHEMA_SUBJECT}",
        {"compatibility": "BACKWARD"},
    )
    assert (m2, body2) == ("POST", {"schemaType": "JSON", "schema": schema})
    assert url2 == f"http://registry:8081/subjects/{SCHEMA_SUBJECT}/versions"
