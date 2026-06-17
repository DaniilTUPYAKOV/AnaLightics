import datetime
from uuid import UUID

from backend.api.rate_limit import build_rate_limit_key


def test_build_rate_limit_key_uses_project_and_current_minute():
    """Rate limit keys should include project id and UTC minute window."""
    current_time = datetime.datetime(
        2026,
        6,
        17,
        14,
        30,
        45,
        tzinfo=datetime.timezone.utc,
    )
    project_id = UUID("00000000-0000-0000-0000-000000000001")

    assert (
        build_rate_limit_key(project_id, current_time)
        == "rate_limit:project:00000000-0000-0000-0000-000000000001:202606171430"
    )


def test_build_rate_limit_key_normalizes_timezone_to_utc_minute():
    """Non-UTC timestamps should be normalized before building the window key."""
    current_time = datetime.datetime(
        2026,
        6,
        17,
        17,
        30,
        tzinfo=datetime.timezone(datetime.timedelta(hours=3)),
    )
    project_id = UUID("00000000-0000-0000-0000-000000000001")

    assert build_rate_limit_key(project_id, current_time).endswith(":202606171430")
