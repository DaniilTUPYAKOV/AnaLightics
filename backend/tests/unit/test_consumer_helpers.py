import datetime

import pytest

from backend.consumer.main import parse_iso_datetime


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
