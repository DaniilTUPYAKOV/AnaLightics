
import datetime
import json
import logging
from typing import AsyncGenerator, Dict, Union

from fastapi import FastAPI, HTTPException, Depends, Header, Request
from starlette.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager

from schemas import Event, APIKeyCheck
from config import KAFKA_BOOTSTRAP_SERVERS, EVENT_TOPIC, VALID_API_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_kafka_producer() -> AIOKafkaProducer:
    """
    Initialize a Kafka producer

    This function initializes a Kafka producer using the KAFKA_BOOTSTRAP_SERVERS
    environment variable and a value serializer that encodes values as JSON and
    then as UTF-8 bytes.

    Returns:
        AIOKafkaProducer: The initialized Kafka producer
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    return producer

async def verify_api_key(x_api_key: str = Header(None)) -> Dict[str, Union[bool, str]]:
    """
    Verify the API key provided in the X-API-KEY header.

    If the API key is invalid (i.e., it does not match the VALID_API_KEY
    environment variable), an HTTPException is raised with a status code of 403 and
    the detail "Invalid API Key".

    If the API key is valid, a dictionary is returned with the key "is_valid" set to True
    and the key "project_id" set to "demo-project".

    Args:
        x_api_key (str): The API key provided in the X-API-KEY header.

    Returns:
        Dict[str, Union[bool, str]]: A dictionary with the result of the API key verification.
    """
    if not x_api_key or x_api_key != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return {"is_valid": True, "project_id": "demo-project"}

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Dict[str, AIOKafkaProducer], None]:
    """ Context manager for FastAPI App
            - Starts Kafka producer and close it after the app is stopped

    Args:
        app (FastAPI): FastAPI app

    Yields:
        AsyncGenerator: Dict with kafka producer to be included in the all requests states
    """
    producer = await init_kafka_producer()
    logger.info("Kafka producer started")

    try:
        yield {"producer": producer}
    finally:
        await producer.stop()
        logger.info("Kafka producer stopped")

app = FastAPI(title="Analytics API", version="0.1.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    if not hasattr(request.state, "producer"):
        raise HTTPException(
            status_code=500,
            detail="Kafka producer not available in request state"
        )
    return request.state.producer

@app.post("/track", response_model=APIKeyCheck)
async def track_event(
    event: Event,
    api_key_result: dict = Depends(verify_api_key),
    producer: AIOKafkaProducer = Depends(get_kafka_producer)
):
    try:
        await producer.send_and_wait(
            EVENT_TOPIC,
            {
                "event": event.model_dump(),
                "project_id": api_key_result["project_id"],
                "received_at": datetime.datetime.now().isoformat(),
            }
        )
        return {"is_valid": True, "project_id": api_key_result["project_id"]}
    except Exception as e:
        logger.error(f"Kafka send failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to send event")

@app.get("/health")
async def health_check(request: Request):
    try:
        producer = request.state.producer
        if producer is None:
            return {"status": "degraded", "kafka": "disconnected"}
        return {"status": "healthy", "kafka": "connected"}
    except (AttributeError, KeyError):
        return {"status": "degraded", "kafka": "disconnected"}