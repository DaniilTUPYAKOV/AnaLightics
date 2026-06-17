import datetime

import pytest
from pydantic import ValidationError

from backend.model.schemas import Event


def valid_event_payload() -> dict:
    """Return a minimal valid analytics event payload for schema tests."""
    return {
        "url": "https://example.com/catalog",
        "title": "Catalog",
        "referrer": None,
        "user_agent": "Mozilla/5.0",
        "screen_width": 1920,
        "screen_height": 1080,
        "timestamp": "2026-06-17T14:30:00+00:00",
        "event_type": "page_view",
    }


def test_event_accepts_valid_payload():
    """A valid event payload should be parsed into the strict Event model."""
    event = Event.model_validate(valid_event_payload())

    assert str(event.url) == "https://example.com/catalog"
    assert event.referrer is None
    assert event.timestamp == datetime.datetime(
        2026,
        6,
        17,
        14,
        30,
        tzinfo=datetime.timezone.utc,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("url", "not-a-url"),
        ("title", ""),
        ("user_agent", ""),
        ("screen_width", 0),
        ("screen_height", 0),
        ("event_type", "PageView"),
        ("event_type", "page-view"),
    ],
)
def test_event_rejects_invalid_fields(field: str, value: object):
    """Invalid field values should be rejected by Pydantic validation."""
    payload = valid_event_payload()
    payload[field] = value

    with pytest.raises(ValidationError):
        Event.model_validate(payload)


def test_event_rejects_extra_fields():
    """Unexpected event fields should be rejected instead of silently accepted."""
    payload = valid_event_payload()
    payload["metadata"] = {"campaign": "summer"}

    with pytest.raises(ValidationError):
        Event.model_validate(payload)
