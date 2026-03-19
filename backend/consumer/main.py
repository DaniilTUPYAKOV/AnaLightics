import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import clickhouse_connect

# Добавь DLQ_TOPIC в конфиг или тут
from backend.model.config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    EVENT_TOPIC,
    DLQ_TOPIC,
    CLICKHOUSE_HOST, 
    CLICKHOUSE_PORT, 
    CLICKHOUSE_TABLE,
    CONSUMER_GROUP_ID,
    CONSUMER_MAX_RETRIES,
    CONSUMER_RETRY_DELAY,
    CONSUMER_BATCH_SIZE,
    CONSUMER_FLUSH_INTERVAL,
    CONSUMER_AUTO_OFFSET_RESET
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def store_to_dlq(message: List[Dict], dlq_producer: AIOKafkaProducer):
    try:
        dlq_message = json.dumps({
            "error": "ClickHouse insert failed",
            "timestamp": datetime.now().isoformat(),
            "batch": message
        }).encode('utf-8')
        await dlq_producer.send_and_wait(DLQ_TOPIC, dlq_message)
        logger.info(f"Sent {len(message)} events to DLQ: {DLQ_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send to DLQ: {e}")
        raise e

class ClickHouseWriter:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT,
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
        Returns True if processing is successful (ClickHouse or DLQ), 
        and False/Exception if everything is bad
        """
        if not self.buffer:
            return

        for attempt in range(1, CONSUMER_MAX_RETRIES + 1):
            try:
                first_event = self.buffer[0]
                columns = list(first_event.keys())
                data = [[item.get(col) for col in columns] for item in self.buffer]

                self.client.insert(CLICKHOUSE_TABLE, data, column_names=columns)

                logger.info(f"Written {len(self.buffer)} events.")
                self.buffer = []
                self.last_flush_time = datetime.now()
                return

            except Exception as e:
                logger.warning(f"Attempt {attempt}/{CONSUMER_MAX_RETRIES} failed: {e}")
                if attempt < CONSUMER_MAX_RETRIES:
                    sleep_time = CONSUMER_RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error("All attempts to write have been exhausted.")

        try:
            logger.info(f"Sending batch ({len(self.buffer)} events) to DLQ: {DLQ_TOPIC}")
            await store_to_dlq(self.buffer, dlq_producer)
            logger.info("Data saved to DLQ. Clearing buffer.")
            self.buffer = []
            self.last_flush_time = datetime.now()
            return

        except Exception as dlq_error:
            logger.critical(f"FATAL: Failed to write to ClickHouse or DLQ: {dlq_error}")
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
                        try:
                            raw_data = msg.value
                            if 'event' not in raw_data:
                                logger.warning(f"Skipping invalid message: {raw_data}")
                                continue

                            event = raw_data['event']
                            event['project_id'] = raw_data.get('project_id')
                            event['received_at'] = datetime.fromisoformat(raw_data.get('received_at'))
                            writer.add_to_buffer(event)
                        except Exception as parse_error:
                            logger.error(f"Error parsing message: {parse_error}")
                            logger.error(f"Sending message to DLQ: {raw_data}")
                            await store_to_dlq([raw_data], dlq_producer)
                            logger.info("Message sent to DLQ. Skipping.")
                            continue

            time_since_flush = (datetime.now() - writer.last_flush_time).total_seconds()
            
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