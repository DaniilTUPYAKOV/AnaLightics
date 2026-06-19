import datetime
from uuid import UUID

import pytest

from backend.api.exceptions import TooManyRequestsError
from backend.api.rate_limit import build_rate_limit_key, enforce_fixed_window_rate_limit


class FakeRedis:
    def __init__(self, request_count: int) -> None:
        self.request_count = request_count
        self.incremented_keys: list[str] = []

    async def incr(self, key: str) -> int:
        self.incremented_keys.append(key)
        return self.request_count

    async def expire(self, key: str, ttl_seconds: int) -> None:
        return None


@pytest.fixture
def anyio_backend() -> str:
    """Rate limit async tests should use asyncio as the async backend."""
    return "asyncio"


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


@pytest.mark.anyio
async def test_enforce_fixed_window_rate_limit_rejects_requests_above_limit():
    """Requests above the configured per-minute limit should be rejected."""
    redis = FakeRedis(request_count=11)
    project_id = UUID("00000000-0000-0000-0000-000000000001")

    with pytest.raises(TooManyRequestsError):
        await enforce_fixed_window_rate_limit(
            redis,
            project_id,
            limit_per_minute=10,
        )

    assert len(redis.incremented_keys) == 1
