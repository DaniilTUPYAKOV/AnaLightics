import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import clickhouse_connect
from clickhouse_connect.driver.exceptions import InternalError, OperationalError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.model.config import Settings, get_settings

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CLICKHOUSE_RETRYABLE_ERRORS = (
    OperationalError,
    InternalError,
    TimeoutError,
    ConnectionError,
)


def parse_iso_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"

    return datetime.fromisoformat(value)


def build_clickhouse_event(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    event = dict(raw_data["event"])

    if "event_id" not in event:
        raise ValueError("Missing 'event_id'")

    event["event_id"] = str(UUID(str(event["event_id"])))
    event["project_id"] = raw_data.get("project_id")
    event["timestamp"] = parse_iso_datetime(event["timestamp"])
    event["received_at"] = parse_iso_datetime(raw_data.get("received_at"))

    return event


def build_event_dedupe_key(project_id: str, event_id: str) -> str:
    return f"event_dedupe:project:{project_id}:event:{event_id}"


async def is_duplicate_event(
    redis: Redis,
    project_id: str,
    event_id: str,
    ttl_seconds: int,
) -> bool:
    key = build_event_dedupe_key(project_id, event_id)
    was_set = await redis.set(key, "1", nx=True, ex=ttl_seconds)

    return was_set is None


def setup_shutdown_event() -> asyncio.Event:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_: shutdown_event.set())
            except ValueError:
                logger.warning("Signal handler for %s is not available.", sig.name)

    return shutdown_event


async def store_to_dlq_batch(
    messages: List[Dict],
    dlq_producer: AIOKafkaProducer,
    error_reason: str,
    settings: Settings,
):
    """
    Отправляет список сообщений в DLQ по одному, но конкурентно, 
    чтобы не превысить max.message.bytes в Kafka и сделать это быстро.
    """
    try:
        logger.info(
            f"Sending {len(messages)} individual events to DLQ: {settings.dlq_topic}")
        coros = []
        for msg in messages:
            dlq_message = json.dumps({
                "error": error_reason,
                "timestamp": datetime.now().isoformat(),
                "original_message": msg
            }, default=str).encode('utf-8')

            coros.append(dlq_producer.send(settings.dlq_topic, dlq_message))

        await asyncio.gather(*coros)
        logger.info("Successfully sent all events to DLQ.")
    except Exception as e:
        logger.error(f"Failed to send batch to DLQ: {e}")
        raise e


async def store_to_dlq_single(
    message: Any,
    dlq_producer: AIOKafkaProducer,
    error_reason: str,
    settings: Settings,
):
    """Отправка одиночного сообщения в DLQ (например, при ошибке парсинга)"""
    try:
        dlq_message = json.dumps({
            "error": error_reason,
            "timestamp": datetime.now().isoformat(),
            "original_message": message
        }, default=str).encode('utf-8')
        await dlq_producer.send_and_wait(settings.dlq_topic, dlq_message)
    except Exception as e:
        logger.error(f"Failed to send single message to DLQ: {e}")
        raise e


class ClickHouseWriter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
            connect_timeout=10,
            send_receive_timeout=30
        )
        self.buffer: List[Dict] = []
        self.last_flush_time = datetime.now()

    def add_to_buffer(self, event: Dict):
        self.buffer.append(event)

    async def insert_batch(self, data: List[List[Any]], columns: List[str]) -> None:
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self.settings.consumer_max_retries),
            wait=wait_exponential(multiplier=self.settings.consumer_retry_delay),
            retry=retry_if_exception_type(CLICKHOUSE_RETRYABLE_ERRORS),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

        async for attempt in retrying:
            with attempt:
                await asyncio.to_thread(
                    self.client.insert,
                    self.settings.clickhouse_table,
                    data,
                    column_names=columns,
                )

    async def flush(self, dlq_producer: AIOKafkaProducer):
        """
        Async flush with retry and DLQ logic.
        """
        if not self.buffer:
            return

        first_event = self.buffer[0]
        columns = list(first_event.keys())
        data = [[item.get(col) for col in columns] for item in self.buffer]

        try:
            await self.insert_batch(data, columns)
            logger.info(f"Written {len(self.buffer)} events to ClickHouse.")
            self.buffer = []
            self.last_flush_time = datetime.now()
            return
        except Exception as insert_error:
            logger.error(
                "All attempts to write to ClickHouse have been exhausted: %s",
                insert_error,
            )

        try:
            await store_to_dlq_batch(
                self.buffer,
                dlq_producer,
                "ClickHouse insert failed after retries",
                self.settings,
            )
            logger.info("Data saved to DLQ. Clearing buffer.")
            self.buffer = []
            self.last_flush_time = datetime.now()
            return

        except Exception as dlq_error:
            logger.critical(
                f"FATAL: Failed to write to ClickHouse AND DLQ: {dlq_error}")
            raise dlq_error


async def consume():
    settings = get_settings()
    shutdown_event = setup_shutdown_event()
    redis: Redis | None = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        decode_responses=True,
    )

    consumer = AIOKafkaConsumer(
        settings.event_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset=settings.consumer_auto_offset_reset,
        enable_auto_commit=False
    )

    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )

    writer = await asyncio.to_thread(ClickHouseWriter, settings)

    await consumer.start()
    await dlq_producer.start()
    try:
        await redis.ping()
        logger.info("Redis dedupe client started")
    except RedisError as e:
        logger.warning("Redis dedupe unavailable, continuing without it: %s", e)
        await redis.aclose()
        redis = None
    logger.info("Consumer & DLQ Producer started")

    try:
        while not shutdown_event.is_set():
            result = await consumer.getmany(
                timeout_ms=1000,
                max_records=settings.consumer_batch_size,
            )
            should_commit_processed_offsets = False

            for tp, messages in result.items():
                if messages:
                    for msg in messages:
                        raw_data = msg.value
                        try:
                            if 'event' not in raw_data:
                                logger.warning(
                                    f"Skipping invalid message format: {raw_data}")
                                await store_to_dlq_single(
                                    raw_data,
                                    dlq_producer,
                                    "Missing 'event' key",
                                    settings,
                                )
                                should_commit_processed_offsets = True
                                continue

                            event = build_clickhouse_event(raw_data)
                            if redis is not None:
                                try:
                                    if await is_duplicate_event(
                                        redis,
                                        event["project_id"],
                                        event["event_id"],
                                        settings.event_dedupe_ttl_seconds,
                                    ):
                                        logger.info(
                                            "Skipping duplicate event: %s",
                                            event["event_id"],
                                        )
                                        should_commit_processed_offsets = True
                                        continue
                                except RedisError as dedupe_error:
                                    logger.error(
                                        "Redis dedupe failed, writing event anyway: %s",
                                        dedupe_error,
                                    )
                            writer.add_to_buffer(event)
                        except Exception as parse_error:
                            logger.error(
                                f"Error parsing message: {parse_error}")
                            await store_to_dlq_single(
                                raw_data,
                                dlq_producer,
                                f"Parse error: {str(parse_error)}",
                                settings,
                            )
                            logger.info("Bad message sent to DLQ. Skipping.")
                            should_commit_processed_offsets = True
                            continue

            time_since_flush = (
                datetime.now() - writer.last_flush_time).total_seconds()

            if len(writer.buffer) >= settings.consumer_batch_size or (
                time_since_flush >= settings.consumer_flush_interval and writer.buffer
            ):
                await writer.flush(dlq_producer)
                await consumer.commit()
                should_commit_processed_offsets = False

            if should_commit_processed_offsets and not writer.buffer:
                await consumer.commit()

        logger.info("Shutdown requested. Stopping Consumer...")
    except Exception as e:
        logger.critical(f"Critical error in loop: {e}")
    finally:
        if writer.buffer:
            logger.info("Attempting to save remaining data before exit...")
            try:
                await writer.flush(dlq_producer)
                await consumer.commit()
            except Exception:
                logger.error("Failed to save remaining data.")

        await consumer.stop()
        await dlq_producer.stop()
        if redis is not None:
            await redis.aclose()

if __name__ == "__main__":
    asyncio.run(consume())
