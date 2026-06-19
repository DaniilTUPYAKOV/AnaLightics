from collections.abc import AsyncIterator
from uuid import UUID

import pytest
from aiokafka.errors import KafkaConnectionError
from httpx import ASGITransport, AsyncClient

from backend.api import main as api_main
from backend.api.model import ProjectContext
from backend.model.config import Settings


PROJECT_ID = UUID("00000000-0000-0000-0000-000000000001")
API_KEY_ID = UUID("00000000-0000-0000-0000-000000000002")


class FakeKafkaProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.messages: list[tuple[str, dict]] = []

    async def send_and_wait(self, topic: str, message: dict) -> None:
        if self.error is not None:
            raise self.error

        self.messages.append((topic, message))


class FakeRedis:
    def __init__(self, request_count: int) -> None:
        self.request_count = request_count
        self.incremented_keys: list[str] = []
        self.expirations: list[tuple[str, int]] = []

    async def incr(self, key: str) -> int:
        self.incremented_keys.append(key)
        return self.request_count

    async def expire(self, key: str, ttl_seconds: int) -> None:
        self.expirations.append((key, ttl_seconds))


@pytest.fixture
def anyio_backend() -> str:
    """API tests should use asyncio as the async backend."""
    return "asyncio"


def valid_track_payload() -> dict:
    """Return a valid track request body accepted by the Event schema."""
    return {
        "url": "https://example.com/catalog",
        "title": "Catalog",
        "referrer": None,
        "user_agent": "Mozilla/5.0",
        "screen_width": 1920,
        "screen_height": 1080,
        "timestamp": "2026-06-19T10:30:00+00:00",
        "event_type": "page_view",
    }


def build_test_settings() -> Settings:
    """Return settings for API tests without reading local .env files."""
    return Settings(
        _env_file=None,
        event_topic="events",
        kafka_producer_max_retries=1,
        kafka_producer_retry_delay_seconds=0,
        kafka_producer_retry_max_delay_seconds=0,
    )


@pytest.fixture
async def track_api_client() -> AsyncIterator[tuple[AsyncClient, FakeKafkaProducer]]:
    """Build an ASGI client for /track with external dependencies replaced."""
    producer = FakeKafkaProducer()
    settings = build_test_settings()

    async def override_current_project() -> ProjectContext:
        return ProjectContext(
            project_id=PROJECT_ID,
            api_key_id=API_KEY_ID,
            rate_limit_per_minute=1000,
        )

    async def override_rate_limit() -> None:
        return None

    async def override_kafka_producer() -> FakeKafkaProducer:
        return producer

    def override_settings() -> Settings:
        return settings

    api_main.app.dependency_overrides[api_main.get_current_project] = (
        override_current_project
    )
    api_main.app.dependency_overrides[api_main.enforce_project_rate_limit] = (
        override_rate_limit
    )
    api_main.app.dependency_overrides[api_main.get_kafka_producer] = (
        override_kafka_producer
    )
    api_main.app.dependency_overrides[api_main.get_settings] = override_settings

    try:
        transport = ASGITransport(app=api_main.app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            yield client, producer
    finally:
        api_main.app.dependency_overrides.clear()


@pytest.mark.anyio
async def test_track_accepts_valid_event_and_sends_message_to_kafka(
    track_api_client: tuple[AsyncClient, FakeKafkaProducer],
) -> None:
    """A valid /track request should return success and publish one Kafka message."""
    client, producer = track_api_client

    response = await client.post(
        "/track",
        json=valid_track_payload(),
        headers={"X-API-Key": "ak_live_test"},
    )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"]
    assert response.json() == {
        "is_valid": True,
        "project_id": str(PROJECT_ID),
    }
    assert len(producer.messages) == 1

    topic, message = producer.messages[0]
    assert topic == "events"
    assert message["project_id"] == str(PROJECT_ID)
    assert message["api_key_id"] == str(API_KEY_ID)
    assert message["event"]["event_type"] == "page_view"
    assert message["event"]["url"] == "https://example.com/catalog"
    assert message["received_at"]


@pytest.mark.anyio
async def test_track_returns_validation_error_contract_for_invalid_event(
    track_api_client: tuple[AsyncClient, FakeKafkaProducer],
) -> None:
    """Invalid /track payloads should use the unified API error format."""
    client, producer = track_api_client
    payload = valid_track_payload()
    payload["screen_width"] = 0

    response = await client.post(
        "/track",
        json=payload,
        headers={"X-API-Key": "ak_live_test"},
    )

    body = response.json()
    assert response.status_code == 422
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert body["error"]["message"] == "Request validation failed"
    assert body["error"]["request_id"]
    assert body["error"]["details"]["errors"]
    assert producer.messages == []


@pytest.mark.anyio
async def test_track_returns_kafka_unavailable_when_publish_fails() -> None:
    """Kafka connection failures should become a stable 503 API error."""
    producer = FakeKafkaProducer(error=KafkaConnectionError("Kafka is unavailable"))
    settings = build_test_settings()

    async def override_current_project() -> ProjectContext:
        return ProjectContext(
            project_id=PROJECT_ID,
            api_key_id=API_KEY_ID,
            rate_limit_per_minute=1000,
        )

    async def override_rate_limit() -> None:
        return None

    async def override_kafka_producer() -> FakeKafkaProducer:
        return producer

    def override_settings() -> Settings:
        return settings

    api_main.app.dependency_overrides[api_main.get_current_project] = (
        override_current_project
    )
    api_main.app.dependency_overrides[api_main.enforce_project_rate_limit] = (
        override_rate_limit
    )
    api_main.app.dependency_overrides[api_main.get_kafka_producer] = (
        override_kafka_producer
    )
    api_main.app.dependency_overrides[api_main.get_settings] = override_settings

    try:
        transport = ASGITransport(app=api_main.app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/track",
                json=valid_track_payload(),
                headers={"X-API-Key": "ak_live_test"},
            )
    finally:
        api_main.app.dependency_overrides.clear()

    body = response.json()
    assert response.status_code == 503
    assert body["error"]["code"] == "KAFKA_UNAVAILABLE"
    assert body["error"]["message"] == "Event ingestion is temporarily unavailable"
    assert body["error"]["request_id"]


@pytest.mark.anyio
async def test_track_returns_429_when_rate_limit_is_exceeded() -> None:
    """Exceeded project RPM limits should stop /track before Kafka publishing."""
    producer = FakeKafkaProducer()
    redis = FakeRedis(request_count=2)
    settings = build_test_settings()

    async def override_current_project() -> ProjectContext:
        return ProjectContext(
            project_id=PROJECT_ID,
            api_key_id=API_KEY_ID,
            rate_limit_per_minute=1,
        )

    async def override_kafka_producer() -> FakeKafkaProducer:
        return producer

    async def override_redis() -> FakeRedis:
        return redis

    def override_settings() -> Settings:
        return settings

    api_main.app.dependency_overrides[api_main.get_current_project] = (
        override_current_project
    )
    api_main.app.dependency_overrides[api_main.get_kafka_producer] = (
        override_kafka_producer
    )
    api_main.app.dependency_overrides[api_main.get_redis] = override_redis
    api_main.app.dependency_overrides[api_main.get_settings] = override_settings

    try:
        transport = ASGITransport(app=api_main.app)
        async with AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/track",
                json=valid_track_payload(),
                headers={"X-API-Key": "ak_live_test"},
            )
    finally:
        api_main.app.dependency_overrides.clear()

    body = response.json()
    assert response.status_code == 429
    assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert body["error"]["message"] == "Too many events"
    assert body["error"]["request_id"]
    assert len(redis.incremented_keys) == 1
    assert redis.expirations == []
    assert producer.messages == []
