import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
DB_URL = os.getenv("DB_URL", "nopostgresql")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "nokafka")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "noevents")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "noldq")

# TODOж Получать из БД
API_KEY = os.getenv("API_KEY", "nokey")

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "noclickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "noclickhouseport")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "noclickhousetable")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "noclickhousedb")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "noclickhouseuser")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "noclickhousepassword")

# Consumer
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "nogroup")
CONSUMER_AUTO_OFFSET_RESET = os.getenv("CONSUMER_AUTO_OFFSET_RESET", "noconsumerautooffsetreset")
CONSUMER_MAX_RETRIES = int(os.getenv("CONSUMER_MAX_RETRIES", 0))
CONSUMER_RETRY_DELAY = int(os.getenv("CONSUMER_RETRY_DELAY", 0))
CONSUMER_BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", 0))
CONSUMER_FLUSH_INTERVAL = float(os.getenv("CONSUMER_FLUSH_INTERVAL", 0))
