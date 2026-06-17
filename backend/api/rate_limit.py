import datetime
import logging
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from backend.api.exceptions import ServiceUnavailableError, TooManyRequestsError

logger = logging.getLogger(__name__)

RATE_LIMIT_WINDOW_TTL_SECONDS = 70


def build_rate_limit_key(
    project_id: UUID,
    current_time: datetime.datetime,
) -> str:
    window = current_time.astimezone(datetime.timezone.utc).strftime("%Y%m%d%H%M")
    return f"rate_limit:project:{project_id}:{window}"


async def enforce_fixed_window_rate_limit(
    redis: Redis,
    project_id: UUID,
    limit_per_minute: int,
) -> None:
    current_time = datetime.datetime.now(datetime.timezone.utc)
    key = build_rate_limit_key(project_id, current_time)

    try:
        request_count = await redis.incr(key)
        if request_count == 1:
            await redis.expire(key, RATE_LIMIT_WINDOW_TTL_SECONDS)
    except RedisError as e:
        logger.error("Redis rate limit check failed: %s", e, exc_info=True)
        raise ServiceUnavailableError("Rate limiter unavailable") from e

    if request_count > limit_per_minute:
        raise TooManyRequestsError("Rate limit exceeded")
