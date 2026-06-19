import datetime

import pytest

from backend.consumer.main import (
    build_clickhouse_event,
    build_event_dedupe_key,
    is_duplicate_event,
    parse_iso_datetime,
)

DEDUPE_KEY = (
    "event_dedupe:project:00000000-0000-0000-0000-000000000001:"
    "event:00000000-0000-0000-0000-000000000003"
)


class FakeRedis:
    def __init__(self, result: bool | None) -> None:
        self.result = result
        self.calls: list[tuple[str, str, bool, int]] = []

    async def set(self, key: str, value: str, nx: bool, ex: int) -> bool | None:
        self.calls.append((key, value, nx, ex))
        return self.result


@pytest.fixture
def anyio_backend() -> str:
    """Consumer async helper tests should use asyncio."""
    return "asyncio"


def test_parse_iso_datetime_accepts_z_suffix_as_utc():
    """ISO datetimes ending with Z should be parsed as UTC timestamps."""
    parsed = parse_iso_datetime("2026-06-17T14:30:00Z")

    assert parsed == datetime.datetime(
        2026,
        6,
        17,
        14,
        30,
        tzinfo=datetime.timezone.utc,
    )


def test_parse_iso_datetime_rejects_malformed_value():
    """Malformed datetime strings should raise the parser's ValueError."""
    with pytest.raises(ValueError):
        parse_iso_datetime("not-a-datetime")


def test_parse_iso_datetime_accepts_explicit_offset():
    """ISO datetimes with explicit offsets should keep that offset."""
    parsed = parse_iso_datetime("2026-06-17T17:30:00+03:00")

    assert parsed.utcoffset() == datetime.timedelta(hours=3)


def test_build_clickhouse_event_normalizes_event_id_and_timestamps():
    """Kafka messages should be converted into ClickHouse-ready event rows."""
    event = build_clickhouse_event(
        {
            "project_id": "00000000-0000-0000-0000-000000000001",
            "received_at": "2026-06-17T14:31:00Z",
            "event": {
                "event_id": "00000000-0000-0000-0000-000000000003",
                "url": "https://example.com/catalog",
                "title": "Catalog",
                "referrer": None,
                "user_agent": "Mozilla/5.0",
                "screen_width": 1920,
                "screen_height": 1080,
                "timestamp": "2026-06-17T14:30:00Z",
                "event_type": "page_view",
            },
        }
    )

    assert event["event_id"] == "00000000-0000-0000-0000-000000000003"
    assert event["project_id"] == "00000000-0000-0000-0000-000000000001"
    assert event["timestamp"] == datetime.datetime(
        2026,
        6,
        17,
        14,
        30,
        tzinfo=datetime.timezone.utc,
    )
    assert event["received_at"] == datetime.datetime(
        2026,
        6,
        17,
        14,
        31,
        tzinfo=datetime.timezone.utc,
    )


def test_build_clickhouse_event_rejects_missing_event_id():
    """Kafka messages without event_id should fail before ClickHouse insert."""
    with pytest.raises(ValueError, match="Missing 'event_id'"):
        build_clickhouse_event(
            {
                "project_id": "00000000-0000-0000-0000-000000000001",
                "received_at": "2026-06-17T14:31:00Z",
                "event": {
                    "timestamp": "2026-06-17T14:30:00Z",
                    "event_type": "page_view",
                },
            }
        )


def test_build_event_dedupe_key_uses_project_and_event_id():
    """Dedupe keys should be scoped by project and client-generated event id."""
    assert (
        build_event_dedupe_key(
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000003",
        )
        == DEDUPE_KEY
    )


@pytest.mark.anyio
async def test_is_duplicate_event_returns_false_for_new_event():
    """Redis SET NX success means the event has not been seen recently."""
    redis = FakeRedis(result=True)

    is_duplicate = await is_duplicate_event(
        redis,
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000003",
        ttl_seconds=86400,
    )

    assert is_duplicate is False
    assert redis.calls == [
        (
            DEDUPE_KEY,
            "1",
            True,
            86400,
        )
    ]


@pytest.mark.anyio
async def test_is_duplicate_event_returns_true_for_seen_event():
    """Redis SET NX miss means the event is a recent duplicate."""
    redis = FakeRedis(result=None)

    is_duplicate = await is_duplicate_event(
        redis,
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000003",
        ttl_seconds=86400,
    )

    assert is_duplicate is True
