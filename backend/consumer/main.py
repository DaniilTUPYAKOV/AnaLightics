import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import clickhouse_connect

from backend.model.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    EVENT_TOPIC,
    DLQ_TOPIC,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_DB,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CONSUMER_GROUP_ID,
    CONSUMER_MAX_RETRIES,
    CONSUMER_RETRY_DELAY,
    CONSUMER_BATCH_SIZE,
    CONSUMER_FLUSH_INTERVAL,
    CONSUMER_AUTO_OFFSET_RESET
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def store_to_dlq_batch(messages: List[Dict], dlq_producer: AIOKafkaProducer, error_reason: str):
    """
    Отправляет список сообщений в DLQ по одному, но конкурентно, 
    чтобы не превысить max.message.bytes в Kafka и сделать это быстро.
    """
    try:
        logger.info(
            f"Sending {len(messages)} individual events to DLQ: {DLQ_TOPIC}")
        coros = []
        for msg in messages:
            dlq_message = json.dumps({
                "error": error_reason,
                "timestamp": datetime.now().isoformat(),
                "original_message": msg
            }, default=str).encode('utf-8')

            coros.append(dlq_producer.send(DLQ_TOPIC, dlq_message))

        await asyncio.gather(*coros)
        logger.info("Successfully sent all events to DLQ.")
    except Exception as e:
        logger.error(f"Failed to send batch to DLQ: {e}")
        raise e


async def store_to_dlq_single(message: Any, dlq_producer: AIOKafkaProducer, error_reason: str):
    """Отправка одиночного сообщения в DLQ (например, при ошибке парсинга)"""
    try:
        dlq_message = json.dumps({
            "error": error_reason,
            "timestamp": datetime.now().isoformat(),
            "original_message": message
        }, default=str).encode('utf-8')
        await dlq_producer.send_and_wait(DLQ_TOPIC, dlq_message)
    except Exception as e:
        logger.error(f"Failed to send single message to DLQ: {e}")
        raise e


class ClickHouseWriter:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
            connect_timeout=10,
            send_receive_timeout=30
        )
        self.buffer: List[Dict] = []
        self.last_flush_time = datetime.now()

    def add_to_buffer(self, event: Dict):
        self.buffer.append(event)

    async def flush(self, dlq_producer: AIOKafkaProducer):
        """
        Async flush with retry and DLQ logic.
        """
        if not self.buffer:
            return

        for attempt in range(1, CONSUMER_MAX_RETRIES + 1):
            try:
                first_event = self.buffer[0]
                columns = list(first_event.keys())
                data = [[item.get(col) for col in columns]
                        for item in self.buffer]

                await asyncio.to_thread(
                    self.client.insert,
                    CLICKHOUSE_TABLE,
                    data,
                    column_names=columns
                )

                logger.info(
                    f"Written {len(self.buffer)} events to ClickHouse.")
                self.buffer = []
                self.last_flush_time = datetime.now()
                return

            except Exception as e:
                logger.warning(
                    f"Attempt {attempt}/{CONSUMER_MAX_RETRIES} failed: {e}")
                if attempt < CONSUMER_MAX_RETRIES:
                    sleep_time = CONSUMER_RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error(
                        "All attempts to write to ClickHouse have been exhausted.")

        try:
            await store_to_dlq_batch(self.buffer, dlq_producer, "ClickHouse insert failed after retries")
            logger.info("Data saved to DLQ. Clearing buffer.")
            self.buffer = []
            self.last_flush_time = datetime.now()
            return

        except Exception as dlq_error:
            logger.critical(
                f"FATAL: Failed to write to ClickHouse AND DLQ: {dlq_error}")
            raise dlq_error


async def consume():

    consumer = AIOKafkaConsumer(
        EVENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset=CONSUMER_AUTO_OFFSET_RESET,
        enable_auto_commit=False
    )

    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    writer = ClickHouseWriter()

    await consumer.start()
    await dlq_producer.start()
    logger.info("Consumer & DLQ Producer started")

    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=CONSUMER_BATCH_SIZE)

            for tp, messages in result.items():
                if messages:
                    for msg in messages:
                        raw_data = msg.value
                        try:
                            if 'event' not in raw_data:
                                logger.warning(
                                    f"Skipping invalid message format: {raw_data}")
                                await store_to_dlq_single(raw_data, dlq_producer, "Missing 'event' key")
                                continue

                            event = raw_data['event']
                            event['project_id'] = raw_data.get('project_id')
                            event['received_at'] = datetime.fromisoformat(
                                raw_data.get('received_at'))
                            writer.add_to_buffer(event)
                        except Exception as parse_error:
                            logger.error(
                                f"Error parsing message: {parse_error}")
                            await store_to_dlq_single(raw_data, dlq_producer, f"Parse error: {str(parse_error)}")
                            logger.info("Bad message sent to DLQ. Skipping.")
                            continue

            time_since_flush = (
                datetime.now() - writer.last_flush_time).total_seconds()

            if len(writer.buffer) >= CONSUMER_BATCH_SIZE or (time_since_flush >= CONSUMER_FLUSH_INTERVAL and writer.buffer):
                await writer.flush(dlq_producer)
                await consumer.commit()

    except KeyboardInterrupt:
        logger.info("Stopping Consumer...")
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

if __name__ == "__main__":
    asyncio.run(consume())
