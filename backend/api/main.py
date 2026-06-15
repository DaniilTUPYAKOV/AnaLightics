import asyncio
import datetime
import json
import logging
from typing import AsyncGenerator, Annotated

from fastapi import FastAPI, Depends, Request, Security
from fastapi.security import APIKeyHeader
from aiokafka.errors import KafkaConnectionError, TimeoutError as KafkaTimeoutError
from starlette.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager

from backend.api.exceptions import (
    ForbiddenError,
    GatewayTimeoutError,
    InternalKafkaError,
    ServiceUnavailableError,
    UnauthorizedError,
)
from backend.api.model import ProjectContext
from backend.model.schemas import Event, APIKeyCheck
from backend.model.config import (
    EVENT_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PRODUCER_ACKS,
    KAFKA_PRODUCER_REQUEST_TIMEOUT_MS,
    KAFKA_PRODUCER_SEND_TIMEOUT_SECONDS,
    KAFKA_PRODUCER_START_TIMEOUT_SECONDS,
)
from backend.repositories import projects

from sqlalchemy.ext.asyncio import AsyncSession
from backend.db.postgres import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=KAFKA_PRODUCER_ACKS,
        request_timeout_ms=KAFKA_PRODUCER_REQUEST_TIMEOUT_MS,
    )
    await asyncio.wait_for(
        producer.start(),
        timeout=KAFKA_PRODUCER_START_TIMEOUT_SECONDS,
    )
    logger.info("Kafka producer started")

    app.state.producer = producer

    try:
        yield
    finally:
        await producer.stop()
        logger.info("Kafka producer stopped")


app = FastAPI(title="Analytics API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_current_project(
    api_key: Annotated[str | None, Security(api_key_header)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ProjectContext:
    if api_key is None:
        raise UnauthorizedError("API key is required")

    auth_context = await projects.get_active_project_by_api_key(db, api_key)

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


@app.post("/track", response_model=APIKeyCheck)
async def track_event(
    event: Event,
    project_context: Annotated[ProjectContext, Depends(get_current_project)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)],
):
    message = {
        "event": event.model_dump(),
        "project_id": str(project_context.project_id),
        "api_key_id": str(project_context.api_key_id)
        if project_context.api_key_id
        else None,
        "received_at": datetime.datetime.now().isoformat(),
    }

    try:
        await asyncio.wait_for(
            producer.send_and_wait(EVENT_TOPIC, message),
            timeout=KAFKA_PRODUCER_SEND_TIMEOUT_SECONDS,
        )
        return {
            "is_valid": True,
            "project_id": str(project_context.project_id),
        }
    except KafkaConnectionError as e:
        logger.warning(
            "Kafka connection lost: %s", e, exc_info=True, extra={"topic": EVENT_TOPIC}
        )
        raise ServiceUnavailableError("Unable to connect to Kafka cluster") from e
    except (KafkaTimeoutError, asyncio.TimeoutError) as e:
        logger.warning(
            "Kafka send timed out: %s", e, exc_info=True, extra={"topic": EVENT_TOPIC}
        )
        raise GatewayTimeoutError("Request to Kafka timed out") from e
    except Exception as e:
        logger.error(
            "Unexpected Kafka error: %s", e, exc_info=True, extra={"topic": EVENT_TOPIC}
        )
        raise InternalKafkaError("Internal error while sending event to Kafka") from e


@app.get("/health")
async def health_check(request: Request):
    try:
        if request.app.state.producer:
            return {"status": "healthy", "kafka": "connected"}
    except AttributeError:
        pass
    return {"status": "degraded", "kafka": "disconnected"}
