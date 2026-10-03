from __future__ import annotations

import random

import pytest

from wikiwatch_producer.sse import SSEEvent, backoff_delay, parse_events


def test_parses_id_and_data():
    lines = ["event: message", 'id: [{"topic":"t","partition":0,"timestamp":1}]', "data: {}", ""]
    assert list(parse_events(lines)) == [
        SSEEvent(data="{}", id='[{"topic":"t","partition":0,"timestamp":1}]')
    ]


def test_multiline_data_is_joined_with_newlines():
    assert list(parse_events(["data: a", "data: b", ""]))[0].data == "a\nb"


def test_comments_and_keepalives_are_ignored():
    assert list(parse_events([":ok", "", ": keep-alive", ""])) == []


def test_id_is_sticky_across_events():
    events = list(parse_events(["id: 1", "data: x", "", "data: y", ""]))
    assert [e.id for e in events] == ["1", "1"]


def test_trailing_event_without_blank_line_is_dispatched():
    assert [e.data for e in parse_events(["data: last"])] == ["last"]


def test_value_without_space_after_colon():
    assert list(parse_events(["data:x", ""]))[0].data == "x"


@pytest.mark.parametrize("attempt", range(12))
def test_backoff_stays_within_exponential_cap(attempt):
    rng = random.Random(attempt)
    delay = backoff_delay(attempt, base=1.0, cap=60.0, rng=rng)
    assert 0 <= delay <= min(60.0, 2**attempt)


def test_backoff_has_jitter():
    rng = random.Random(42)
    assert len({round(backoff_delay(5, rng=rng), 6) for _ in range(20)}) > 1
