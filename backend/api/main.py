import asyncio
import datetime
import json
import time
from typing import AsyncGenerator, Annotated
from uuid import UUID, uuid4

import structlog
from fastapi import FastAPI, Depends, HTTPException, Request, Security
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.security import APIKeyHeader
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError as AioKafkaTimeoutError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette import status
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.api.exceptions import (
    ApiError,
    ForbiddenError,
    InternalKafkaError,
    KafkaTimeoutError,
    KafkaUnavailableError,
    NotFoundError,
    ServiceUnavailableError,
    UnauthorizedError,
    build_error_response,
)
from backend.api.model import ProjectContext
from backend.api.logging_config import configure_structured_logging
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
    ErrorResponse,
    Event,
)
from backend.repositories import projects

from sqlalchemy.ext.asyncio import AsyncSession
from backend.db.postgres import create_postgres_engine, create_sessionmaker

configure_structured_logging()
logger = structlog.get_logger(__name__)

KAFKA_RETRYABLE_ERRORS = (
    KafkaConnectionError,
    AioKafkaTimeoutError,
    asyncio.TimeoutError,
)


def error_response(description: str) -> dict:
    return {
        "model": ErrorResponse,
        "description": description,
        "headers": {
            "X-Request-ID": {
                "description": "Server-generated request identifier for support and logs",
                "schema": {"type": "string"},
            }
        },
    }


COMMON_ERROR_RESPONSES = {
    401: error_response("API key is missing"),
    403: error_response("API key is invalid, inactive, or not allowed"),
    422: error_response("Request validation failed"),
    500: error_response("Unexpected server error"),
}


def log_kafka_retry(retry_state) -> None:
    exception = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "kafka_send_retrying",
        attempt_number=retry_state.attempt_number,
        error_type=type(exception).__name__ if exception else None,
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
        logger.info("postgres_sessionmaker_started")

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
        logger.info("kafka_producer_started")

        redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            socket_timeout=settings.redis_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_socket_timeout_seconds,
            decode_responses=True,
        )
        await redis.ping()
        logger.info("redis_client_started")

        app.state.settings = settings
        app.state.producer = producer
        app.state.redis = redis

        yield
    finally:
        app.state.is_shutting_down = True
        logger.info("shutdown_started")
        if redis is not None:
            await redis.aclose()
            logger.info("redis_client_stopped")
        if producer is not None:
            await producer.stop()
            logger.info("kafka_producer_stopped")
        if postgres_engine is not None:
            await postgres_engine.dispose()
            logger.info("postgres_engine_disposed")
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


def get_request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


@app.middleware("http")
async def attach_request_id(request: Request, call_next):
    request_id = str(uuid4())
    request.state.request_id = request_id
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)

    try:
        if getattr(request.app.state, "is_shutting_down", False) and request.url.path not in {
            "/health",
            "/ready",
        }:
            error = ServiceUnavailableError("Service is shutting down")
            return JSONResponse(
                status_code=error.status_code,
                content=jsonable_encoder(build_error_response(error, request_id)),
                headers={"X-Request-ID": request_id},
            )

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        structlog.contextvars.clear_contextvars()


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    request_id = get_request_id(request)
    logger.warning(
        "api_error",
        error_code=exc.code,
        status_code=exc.status_code,
        details=exc.details,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(build_error_response(exc, request_id)),
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(request: Request, exc: RequestValidationError):
    request_id = get_request_id(request)
    error = ApiError(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        code="VALIDATION_ERROR",
        message="Request validation failed",
        details={"errors": exc.errors()},
    )
    return JSONResponse(
        status_code=error.status_code,
        content=jsonable_encoder(build_error_response(error, request_id)),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = get_request_id(request)
    error = ApiError(
        status_code=exc.status_code,
        code="HTTP_ERROR",
        message=str(exc.detail),
    )
    return JSONResponse(
        status_code=error.status_code,
        content=jsonable_encoder(build_error_response(error, request_id)),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    request_id = get_request_id(request)
    logger.error(
        "unhandled_api_error",
        exc_info=True,
    )
    error = ApiError()
    return JSONResponse(
        status_code=error.status_code,
        content=jsonable_encoder(build_error_response(error, request_id)),
    )


async def get_db(request: Request):
    sessionmaker = request.app.state.db_sessionmaker
    async with sessionmaker() as session:
        yield session


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
        before_sleep=log_kafka_retry,
        reraise=True,
    )

    async for attempt in retrying:
        with attempt:
            await asyncio.wait_for(
                producer.send_and_wait(topic, message),
                timeout=settings.kafka_producer_send_timeout_seconds,
            )


@app.post(
    "/track",
    response_model=APIKeyCheck,
    tags=["Tracking"],
    operation_id="trackEvent",
    summary="Track analytics event",
    responses={
        **COMMON_ERROR_RESPONSES,
        429: error_response("Project rate limit exceeded"),
        503: error_response("Kafka, Redis, or service dependency is unavailable"),
        504: error_response("Kafka send timed out"),
    },
)
async def track_event(
    request: Request,
    event: Event,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    _: Annotated[None, Depends(enforce_project_rate_limit)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    request_started_at = time.perf_counter()
    message = {
        "event": event.model_dump(mode="json"),
        "project_id": str(project_context.project_id),
        "api_key_id": str(project_context.api_key_id)
        if project_context.api_key_id
        else None,
        "received_at": datetime.datetime.now().isoformat(),
    }
    log_context = {
        "project_id": str(project_context.project_id),
        "api_key_id": str(project_context.api_key_id)
        if project_context.api_key_id
        else None,
        "topic": settings.event_topic,
        "event_type": event.event_type,
    }

    try:
        kafka_send_started_at = time.perf_counter()
        await send_event_to_kafka(
            producer,
            settings.event_topic,
            message,
            settings,
        )
        kafka_duration_ms = (time.perf_counter() - kafka_send_started_at) * 1000
        request_duration_ms = (time.perf_counter() - request_started_at) * 1000
        logger.info(
            "track_request_completed",
            **log_context,
            duration_ms=round(request_duration_ms, 2),
            kafka_duration_ms=round(kafka_duration_ms, 2),
        )
        return {
            "is_valid": True,
            "project_id": str(project_context.project_id),
        }
    except KafkaConnectionError as e:
        logger.warning(
            "kafka_send_failed",
            exc_info=True,
            **log_context,
            error_type=type(e).__name__,
        )
        raise KafkaUnavailableError() from e
    except (AioKafkaTimeoutError, asyncio.TimeoutError) as e:
        logger.warning(
            "kafka_send_failed",
            exc_info=True,
            **log_context,
            error_type=type(e).__name__,
        )
        raise KafkaTimeoutError() from e
    except Exception as e:
        logger.error(
            "kafka_send_failed",
            exc_info=True,
            **log_context,
            error_type=type(e).__name__,
        )
        raise InternalKafkaError("Internal error while sending event to Kafka") from e


@app.post(
    "/projects/{project_id}/api-keys",
    response_model=APIKeyCreated,
    tags=["API Keys"],
    operation_id="createProjectApiKey",
    summary="Create project API key",
    responses=COMMON_ERROR_RESPONSES,
)
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
    tags=["API Keys"],
    operation_id="rotateProjectApiKey",
    summary="Rotate project API key",
    responses={
        **COMMON_ERROR_RESPONSES,
        404: error_response("Active API key not found"),
    },
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
    tags=["API Keys"],
    operation_id="revokeProjectApiKey",
    summary="Revoke project API key",
    responses={
        **COMMON_ERROR_RESPONSES,
        404: error_response("Active API key not found"),
    },
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


@app.get(
    "/health",
    tags=["Health"],
    operation_id="getHealth",
    summary="Get service health status",
)
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


@app.get(
    "/ready",
    tags=["Health"],
    operation_id="getReadiness",
    summary="Get service readiness status",
    responses={503: error_response("Service dependency is not ready")},
)
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
