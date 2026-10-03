from __future__ import annotations

import pytest

from conftest import as_data, make_event
from wikiwatch_producer.keys import message_key
from wikiwatch_producer.privacy import contains_ip, is_ip_address


def test_valid_event_passes(validator):
    result = validator.validate(as_data(make_event()))
    assert result.ok and result.reason is None


@pytest.mark.parametrize("field", ["type", "namespace", "title", "wiki", "meta"])
def test_missing_required_field_is_rejected_with_path(validator, field):
    event = make_event()
    del event[field]
    result = validator.validate(as_data(event))
    assert not result.ok
    assert result.reason == f"{field}: required"


def test_missing_meta_id_names_the_nested_path(validator):
    event = make_event(meta={"dt": "2026-10-03T12:00:00Z"})
    assert validator.validate(as_data(event)).reason == "meta.id: required"


def test_invalid_json_is_rejected(validator):
    assert validator.validate("{not json").reason == "invalid_json"


def test_non_object_is_rejected(validator):
    assert validator.validate("[1, 2]").reason == "not_an_object"


def test_reason_never_contains_the_offending_value(validator):
    secret_like = "192.0.2.44"  # RFC 5737 documentation address, never a real editor
    event = make_event(namespace=secret_like)
    result = validator.validate(as_data(event))
    assert result.reason == "namespace: type"
    assert secret_like not in result.reason


def test_unknown_extra_fields_are_allowed(validator):
    assert validator.validate(as_data(make_event(new_upstream_field={"x": 1}))).ok


def test_null_lengths_are_allowed_for_new_pages(validator):
    assert validator.validate(as_data(make_event(length={"old": None, "new": 123}))).ok


def test_key_is_wiki_and_title():
    assert message_key(make_event(title="Talk:Acme")) == "enwiki:Talk:Acme"


@pytest.mark.parametrize("value", ["192.0.2.1", "2001:db8::1", " 198.51.100.7 "])
def test_ip_editors_are_detected(value):
    assert is_ip_address(value)


@pytest.mark.parametrize("value", ["ExampleUser", "~2026-12345-67", "Bot 1.2"])
def test_named_and_temporary_accounts_are_not_ips(value):
    assert not is_ip_address(value)


@pytest.mark.parametrize(
    "text", ["reverted edits by 192.0.2.5", "see 2001:db8::7 talk", "[[User:203.0.113.9]]"]
)
def test_ips_inside_text_are_found(text):
    assert contains_ip(text)


def test_plain_text_has_no_ip():
    assert not contains_ip("Updated infobox for Acme Corporation (2026)")
