"""Pipeline configuration from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the event pipeline."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "events"
    kafka_dlq_topic: str = "events.dlq"
    kafka_consumer_group: str = "event-pipeline-consumer"
    kafka_auto_offset_reset: str = "earliest"

    # Retry policy
    max_retries: int = 3
    retry_backoff_ms: int = 500

    # Metrics server
    metrics_port_producer: int = 8000
    metrics_port_consumer: int = 8001


settings = Settings()
