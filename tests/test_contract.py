"""Contract: every fixture event obeys schemas/wiki_edits.json, except the planted malformed one."""

from __future__ import annotations

import json
from pathlib import Path

from tests.e2e.expectations import read_events
from wikiwatch_producer.validate import EventValidator

FIXTURES = Path(__file__).resolve().parent / "e2e" / "fixtures"


def test_e2e_fixture_events_match_the_schema():
    validator = EventValidator()
    expected = json.loads((FIXTURES / "e2e_expected.json").read_text())
    events = read_events(FIXTURES / "e2e_phase1.jsonl.gz") + read_events(
        FIXTURES / "e2e_phase2.jsonl.gz"
    )
    rejected = [validator.validate(json.dumps(e)).reason for e in events]
    rejected = [r for r in rejected if r is not None]
    assert rejected == ["title: required"] * expected["dlq_rows"]


def test_scripted_alert_events_match_the_schema_except_the_malformed_one():
    validator = EventValidator()
    lines = (FIXTURES / "scripted_alert_edits.jsonl").read_text().splitlines()
    reasons = [validator.validate(line).reason for line in lines]
    assert [r for r in reasons if r] == ["title: required"]


def test_schema_is_valid_json_schema_with_required_contract_fields():
    schema = EventValidator().schema
    assert {"meta", "type", "namespace", "title", "wiki"} <= set(schema["required"])
    assert schema["additionalProperties"] is True  # upstream additions never break validation
