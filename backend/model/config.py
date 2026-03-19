import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
DB_URL = os.getenv("DB_URL", "nopostgresql")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "nokafka")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "raw_events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "events_dlq")

# TODOж Получать из БД
VALID_API_KEY = os.getenv("API_KEY", "nokey")

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "noclickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "analightics.events")

# Consumer
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "analightics_group_v1")
CONSUMER_AUTO_OFFSET_RESET = os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest")
CONSUMER_MAX_RETRIES = int(os.getenv("CONSUMER_MAX_RETRIES", "3"))
CONSUMER_RETRY_DELAY = int(os.getenv("CONSUMER_RETRY_DELAY", "2"))
CONSUMER_BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "1000"))
CONSUMER_FLUSH_INTERVAL = float(os.getenv("CONSUMER_FLUSH_INTERVAL", "5.0"))
