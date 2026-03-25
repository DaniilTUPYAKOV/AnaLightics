import datetime
import json
import logging
from typing import AsyncGenerator, Dict, Union

from fastapi import FastAPI, HTTPException, Depends, Header, Request
from starlette.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager

from backend.model.schemas import Event, APIKeyCheck
from backend.model.config import KAFKA_BOOTSTRAP_SERVERS, EVENT_TOPIC, API_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=0,
    )
    await producer.start()
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


async def verify_api_key(x_api_key: str = Header(None)) -> Dict[str, Union[bool, str]]:
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return {"is_valid": True, "project_id": "demo-project"}


async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    producer = request.app.state.producer
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka producer not ready")
    return producer


@app.post("/track", response_model=APIKeyCheck)
async def track_event(
    event: Event,
    api_key_result: dict = Depends(verify_api_key),
    producer: AIOKafkaProducer = Depends(get_kafka_producer)
):
    event_dict = event.model_dump()

    message = {
        "event": event_dict,
        "project_id": api_key_result["project_id"],
        "received_at": datetime.datetime.now().isoformat(),
    }

    try:
        await producer.send_and_wait(EVENT_TOPIC, message)
        return {"is_valid": True, "project_id": api_key_result["project_id"]}
    except Exception as e:
        logger.error(f"Kafka send failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to send event")


@app.get("/health")
async def health_check(request: Request):
    try:
        if request.app.state.producer:
            return {"status": "healthy", "kafka": "connected"}
    except AttributeError:
        pass
    return {"status": "degraded", "kafka": "disconnected"}
