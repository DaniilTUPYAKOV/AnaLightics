import asyncio
import datetime
import json
import logging
from typing import AsyncGenerator, Annotated
from uuid import UUID

from fastapi import FastAPI, Depends, Request, Security
from fastapi.security import APIKeyHeader
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.api.exceptions import (
    ForbiddenError,
    GatewayTimeoutError,
    InternalKafkaError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
)
from backend.api.model import ProjectContext
from backend.api.rate_limit import enforce_fixed_window_rate_limit
from backend.model.config import (
    Settings,
    get_settings,
)
from backend.model.schemas import (
    APIKeyCheck,
    APIKeyCreate,
    APIKeyCreated,
    APIKeyRevoked,
    APIKeyRotate,
    Event,
)
from backend.repositories import projects

from sqlalchemy.ext.asyncio import AsyncSession
from backend.db.postgres import create_postgres_engine, create_sessionmaker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_RETRYABLE_ERRORS = (
    KafkaConnectionError,
    KafkaTimeoutError,
    asyncio.TimeoutError,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    app.state.is_shutting_down = False
    producer = None
    redis = None
    postgres_engine = None

    try:
        postgres_engine = create_postgres_engine(settings.database_url)
        app.state.db_sessionmaker = create_sessionmaker(postgres_engine)
        logger.info("PostgreSQL sessionmaker started")

        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=settings.kafka_producer_acks,
            request_timeout_ms=settings.kafka_producer_request_timeout_ms,
        )
        await asyncio.wait_for(
            producer.start(),
            timeout=settings.kafka_producer_start_timeout_seconds,
        )
        logger.info("Kafka producer started")

        redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            socket_timeout=settings.redis_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_socket_timeout_seconds,
            decode_responses=True,
        )
        await redis.ping()
        logger.info("Redis client started")

        app.state.settings = settings
        app.state.producer = producer
        app.state.redis = redis

        yield
    finally:
        app.state.is_shutting_down = True
        logger.info("shutdown_started")
        if redis is not None:
            await redis.aclose()
            logger.info("Redis client stopped")
        if producer is not None:
            await producer.stop()
            logger.info("Kafka producer stopped")
        if postgres_engine is not None:
            await postgres_engine.dispose()
            logger.info("PostgreSQL engine disposed")
        logger.info("shutdown_completed")


app = FastAPI(title="Analytics API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_db(request: Request):
    sessionmaker = request.app.state.db_sessionmaker
    async with sessionmaker() as session:
        yield session


@app.middleware("http")
async def reject_requests_during_shutdown(request: Request, call_next):
    if getattr(request.app.state, "is_shutting_down", False) and request.url.path not in {
        "/health",
        "/ready",
    }:
        return JSONResponse(
            status_code=503,
            content={"detail": "Service is shutting down"},
        )

    return await call_next(request)


async def get_current_project(
    api_key: Annotated[str | None, Security(api_key_header)],
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ProjectContext:
    if api_key is None:
        raise UnauthorizedError("API key is required")

    auth_context = await projects.get_active_project_by_api_key(db, api_key, settings)

    if auth_context is None:
        raise ForbiddenError("Invalid or inactive API key")

    project, project_api_key = auth_context

    return ProjectContext(
        project_id=project.id,
        api_key_id=project_api_key.id,
        rate_limit_per_minute=project.rate_limit_per_minute,
    )


async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    producer = request.app.state.producer
    if not producer:
        raise ServiceUnavailableError("Kafka producer not ready")
    return producer


async def get_redis(request: Request) -> Redis:
    redis = request.app.state.redis
    if not redis:
        raise ServiceUnavailableError("Redis not ready")
    return redis


async def enforce_project_rate_limit(
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> None:
    await enforce_fixed_window_rate_limit(
        redis,
        project_context.project_id,
        project_context.rate_limit_per_minute,
    )


async def send_event_to_kafka(
    producer: AIOKafkaProducer,
    topic: str,
    message: dict,
    settings: Settings,
) -> None:
    retrying = AsyncRetrying(
        stop=stop_after_attempt(settings.kafka_producer_max_retries),
        wait=wait_exponential(
            multiplier=settings.kafka_producer_retry_delay_seconds,
            max=settings.kafka_producer_retry_max_delay_seconds,
        ),
        retry=retry_if_exception_type(KAFKA_RETRYABLE_ERRORS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )

    async for attempt in retrying:
        with attempt:
            await asyncio.wait_for(
                producer.send_and_wait(topic, message),
                timeout=settings.kafka_producer_send_timeout_seconds,
            )


@app.post("/track", response_model=APIKeyCheck)
async def track_event(
    event: Event,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    _: Annotated[None, Depends(enforce_project_rate_limit)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    message = {
        "event": event.model_dump(mode="json"),
        "project_id": str(project_context.project_id),
        "api_key_id": str(project_context.api_key_id)
        if project_context.api_key_id
        else None,
        "received_at": datetime.datetime.now().isoformat(),
    }

    try:
        await send_event_to_kafka(
            producer,
            settings.event_topic,
            message,
            settings,
        )
        return {
            "is_valid": True,
            "project_id": str(project_context.project_id),
        }
    except KafkaConnectionError as e:
        logger.warning(
            "Kafka connection lost: %s",
            e,
            exc_info=True,
            extra={"topic": settings.event_topic},
        )
        raise ServiceUnavailableError("Unable to connect to Kafka cluster") from e
    except (KafkaTimeoutError, asyncio.TimeoutError) as e:
        logger.warning(
            "Kafka send timed out: %s",
            e,
            exc_info=True,
            extra={"topic": settings.event_topic},
        )
        raise GatewayTimeoutError("Request to Kafka timed out") from e
    except Exception as e:
        logger.error(
            "Unexpected Kafka error: %s",
            e,
            exc_info=True,
            extra={"topic": settings.event_topic},
        )
        raise InternalKafkaError("Internal error while sending event to Kafka") from e


@app.post("/projects/{project_id}/api-keys", response_model=APIKeyCreated)
async def create_api_key(
    project_id: UUID,
    api_key_data: APIKeyCreate,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    if project_id != project_context.project_id:
        raise ForbiddenError("Cannot create API key for another project")

    api_key, raw_api_key = await projects.create_project_api_key(
        db,
        project_id,
        api_key_data.name,
        settings,
    )

    return APIKeyCreated(
        id=api_key.id,
        project_id=api_key.project_id,
        name=api_key.name,
        key_prefix=api_key.key_prefix,
        api_key=raw_api_key,
    )


@app.post(
    "/projects/{project_id}/api-keys/{api_key_id}/rotate",
    response_model=APIKeyCreated,
)
async def rotate_api_key(
    project_id: UUID,
    api_key_id: UUID,
    api_key_data: APIKeyRotate,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    if project_id != project_context.project_id:
        raise ForbiddenError("Cannot rotate API key for another project")

    rotated_api_key = await projects.rotate_project_api_key(
        db,
        project_id,
        api_key_id,
        api_key_data.name,
        settings,
    )

    if rotated_api_key is None:
        raise NotFoundError("Active API key not found")

    api_key, raw_api_key = rotated_api_key

    return APIKeyCreated(
        id=api_key.id,
        project_id=api_key.project_id,
        name=api_key.name,
        key_prefix=api_key.key_prefix,
        api_key=raw_api_key,
    )


@app.post(
    "/projects/{project_id}/api-keys/{api_key_id}/revoke",
    response_model=APIKeyRevoked,
)
async def revoke_api_key(
    project_id: UUID,
    api_key_id: UUID,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    if project_id != project_context.project_id:
        raise ForbiddenError("Cannot revoke API key for another project")

    api_key = await projects.revoke_project_api_key(db, project_id, api_key_id)

    if api_key is None:
        raise NotFoundError("Active API key not found")

    return APIKeyRevoked(
        id=api_key.id,
        project_id=api_key.project_id,
        name=api_key.name,
        revoked_at=api_key.revoked_at,
    )


@app.get("/health")
async def health_check(request: Request):
    status = {"status": "healthy", "kafka": "connected", "redis": "connected"}

    try:
        if not request.app.state.producer:
            status["status"] = "degraded"
            status["kafka"] = "disconnected"
    except AttributeError:
        status["status"] = "degraded"
        status["kafka"] = "disconnected"

    try:
        redis = request.app.state.redis
        if not redis:
            status["status"] = "degraded"
            status["redis"] = "disconnected"
    except AttributeError:
        status["status"] = "degraded"
        status["redis"] = "disconnected"

    return status


@app.get("/ready")
async def readiness_check(request: Request):
    if getattr(request.app.state, "is_shutting_down", False):
        raise ServiceUnavailableError("Service is shutting down")

    try:
        if not request.app.state.producer:
            raise ServiceUnavailableError("Kafka producer not ready")
    except AttributeError:
        raise ServiceUnavailableError("Kafka producer not ready") from None

    try:
        redis = request.app.state.redis
        await redis.ping()
    except (AttributeError, RedisError) as e:
        raise ServiceUnavailableError("Redis not ready") from e

    return {"status": "ready", "kafka": "connected", "redis": "connected"}
