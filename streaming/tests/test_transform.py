"""Silver and Bronze transforms on a local SparkSession."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from streaming.lib.transform import to_bronze, to_bronze_dlq, to_silver

EVENT_TS = "2026-10-03T12:00:00.000Z"
INGESTED = datetime(2026, 10, 3, 12, 5, 0)  # naive = UTC wall clock


def event(**overrides) -> dict:
    base = {
        "meta": {"id": "id-1", "dt": EVENT_TS, "domain": "en.wikipedia.org"},
        "type": "edit",
        "namespace": 0,
        "title": "Acme Corporation",
        "wiki": "enwiki",
        "user": "ExampleUser",
        "bot": False,
        "length": {"old": 10_000, "new": 7_000},
        "revision": {"old": 1, "new": 2},
    }
    base.update(overrides)
    return base


def kafka_rows(spark, payloads, ingested=INGESTED):
    rows = [
        (
            None,
            p if isinstance(p, str) else json.dumps(p),
            0,
            i,
            ingested,
        )
        for i, p in enumerate(payloads)
    ]
    schema = (
        "kafka_key string, raw string, kafka_partition int, kafka_offset long, "
        "ingested_at timestamp_ntz"
    )
    return spark.createDataFrame(rows, schema)


def silver(spark, *payloads, ingested=INGESTED):
    return [r.asDict() for r in to_silver(kafka_rows(spark, payloads, ingested)).collect()]


def test_fields_are_parsed_and_typed(spark):
    row = silver(spark, event())[0]
    assert row["meta_id"] == "id-1"
    assert row["event_ts"] == datetime(2026, 10, 3, 12, 0, 0)
    assert row["ingested_at"] == INGESTED
    assert (row["wiki"], row["title"], row["namespace"], row["edit_type"]) == (
        "enwiki",
        "Acme Corporation",
        0,
        "edit",
    )
    assert (row["length_old"], row["length_new"], row["revision_new"]) == (10_000, 7_000, 2)


@pytest.mark.parametrize(
    ("user", "bot", "expected"),
    [
        ("ExampleUser", False, "registered"),
        ("192.0.2.10", False, "unregistered"),  # RFC 5737 documentation address
        ("2001:db8::10", False, "unregistered"),  # RFC 3849 documentation address
        ("~2026-12345-67", False, "unregistered"),  # temporary account (verified format)
        ("ExampleBot", True, "bot"),
        ("~2026-12345-67", True, "bot"),  # bot flag wins
        (None, False, "registered"),  # suppressed name
    ],
)
def test_editor_type(spark, user, bot, expected):
    assert silver(spark, event(user=user, bot=bot))[0]["editor_type"] == expected


def test_ip_editor_name_never_reaches_silver(spark):
    row = silver(spark, event(user="192.0.2.10"))[0]
    assert row["user_name"] is None and row["editor_type"] == "unregistered"


def test_temporary_account_name_is_kept(spark):
    assert silver(spark, event(user="~2026-12345-67"))[0]["user_name"] == "~2026-12345-67"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"length": {"old": 10_000, "new": 7_000}}, -3_000),
        ({"length": {"old": 100, "new": 2_100}}, 2_000),
        ({"type": "new", "length": {"old": None, "new": 512}}, 512),
        ({"type": "log", "length": None}, None),
        ({"length": {"old": 5, "new": None}}, None),
    ],
)
def test_byte_delta(spark, overrides, expected):
    assert silver(spark, event(**overrides))[0]["byte_delta"] == expected


@pytest.mark.parametrize(
    ("ingested", "expected"),
    [
        (datetime(2026, 10, 3, 12, 10, 0), False),  # exactly 10 minutes: not late
        (datetime(2026, 10, 3, 12, 10, 1), True),  # 10 min 1 s: late
        (datetime(2026, 10, 3, 12, 0, 30), False),
    ],
)
def test_is_late_boundary(spark, ingested, expected):
    assert silver(spark, event(), ingested=ingested)[0]["is_late"] is expected


def test_rows_without_id_or_time_are_dropped(spark):
    no_id = event(meta={"dt": EVENT_TS})
    bad_time = event(meta={"id": "id-2", "dt": "not a time"})
    assert silver(spark, no_id, bad_time, "{broken json") == []


def test_bronze_keeps_raw_payload_and_kafka_position(spark):
    payload = json.dumps(event())
    row = to_bronze(kafka_rows(spark, [payload]), "redpanda-s1").collect()[0]
    assert row.session_id == "redpanda-s1"
    assert (row.meta_id, row.raw, row.kafka_partition, row.kafka_offset) == (
        "id-1",
        payload,
        0,
        0,
    )


def test_bronze_dlq_unwraps_the_envelope(spark):
    envelope = {
        "error": "title: required",
        "raw": "{}",
        "rejected_at": "2026-10-03T12:00:01.000+00:00",
    }
    row = to_bronze_dlq(kafka_rows(spark, [envelope]), "redpanda-s1").collect()[0]
    assert (row.error, row.raw, row.rejected_at) == (
        "title: required",
        "{}",
        datetime(2026, 10, 3, 12, 0, 1),
    )
