import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
DB_URL = os.getenv("DB_URL", "nopostgresql")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "nokafka")
EVENT_TOPIC = "raw_events"

# TODOж Получать из БД
VALID_API_KEY = os.getenv("API_KEY", "nokey")
