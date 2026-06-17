from functools import lru_cache
from typing import Any, Literal

from pydantic import computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # PostgreSQL
    db_url: str = "nopostgresql"
    postgres_user: str = "test"
    postgres_password: str = "test"
    postgres_db: str = "test"
    postgres_host: str = "localhost"
    postgres_port_external: int = 5432

    # API
    api_key: str = "secret-demo-key-123"
    api_key_hash_secret: str = "dev-api-key-hash-secret"

    # Redis
    redis_host: str = "noredis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_socket_timeout_seconds: float = 1

    # Kafka
    kafka_bootstrap_servers: str = "nokafka"
    event_topic: str = "noevents"
    dlq_topic: str = "noldq"
    kafka_producer_acks: int | Literal["all"] = 1
    kafka_producer_request_timeout_ms: int = 5000
    kafka_producer_send_timeout_seconds: float = 5
    kafka_producer_start_timeout_seconds: float = 10
    kafka_producer_max_retries: int = 3
    kafka_producer_retry_delay_seconds: float = 0.1
    kafka_producer_retry_max_delay_seconds: float = 1

    # ClickHouse
    clickhouse_host: str = "noclickhouse"
    clickhouse_port: int = 8123
    clickhouse_table: str = "noclickhousetable"
    clickhouse_db: str = "noclickhousedb"
    clickhouse_user: str = "noclickhouseuser"
    clickhouse_password: str = "noclickhousepassword"

    # Consumer
    consumer_group_id: str = "nogroup"
    consumer_auto_offset_reset: str = "earliest"
    consumer_max_retries: int = 3
    consumer_retry_delay: int = 2
    consumer_batch_size: int = 1000
    consumer_flush_interval: float = 5

    @computed_field
    @property
    def database_url(self) -> str:
        if self.db_url != "nopostgresql":
            return self.db_url

        return (
            "postgresql+asyncpg://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port_external}"
            f"/{self.postgres_db}"
        )

    @field_validator("kafka_producer_acks")
    @classmethod
    def validate_kafka_producer_acks(cls, value: Any) -> int | Literal["all"]:
        if value in {"0", 0}:
            return 0
        if value in {"1", 1}:
            return 1
        if value == "all":
            return "all"
        raise ValueError("KAFKA_PRODUCER_ACKS must be 0, 1, or all")


@lru_cache
def get_settings() -> Settings:
    return Settings()
