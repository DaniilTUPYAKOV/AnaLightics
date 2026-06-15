import asyncio
import datetime
import json
import logging
from typing import AsyncGenerator, Annotated
from uuid import UUID

from fastapi import FastAPI, Depends, Request, Security
from fastapi.security import APIKeyHeader
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError
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
    ServiceUnavailableError,
    UnauthorizedError,
)
from backend.api.model import ProjectContext
from backend.model.config import (
    Settings,
    get_settings,
)
from backend.model.schemas import APIKeyCheck, APIKeyCreate, APIKeyCreated, Event
from backend.repositories import projects

from sqlalchemy.ext.asyncio import AsyncSession
from backend.db.postgres import get_db

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

    app.state.settings = settings
    app.state.producer = producer

    try:
        yield
    finally:
        app.state.is_shutting_down = True
        logger.info("shutdown_started")
        await producer.stop()
        logger.info("Kafka producer stopped")
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


@app.get("/health")
async def health_check(request: Request):
    try:
        if request.app.state.producer:
            return {"status": "healthy", "kafka": "connected"}
    except AttributeError:
        pass
    return {"status": "degraded", "kafka": "disconnected"}


@app.get("/ready")
async def readiness_check(request: Request):
    if getattr(request.app.state, "is_shutting_down", False):
        raise ServiceUnavailableError("Service is shutting down")

    try:
        if request.app.state.producer:
            return {"status": "ready", "kafka": "connected"}
    except AttributeError:
        pass

    raise ServiceUnavailableError("Kafka producer not ready")
