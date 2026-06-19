import datetime

import pytest

from backend.consumer.main import build_clickhouse_event, parse_iso_datetime


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
