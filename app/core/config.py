from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # App
    APP_NAME: str = "Financial Market Data ETL Pipeline"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "production"

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/etl_db"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP: str = "etl-pipeline-group"
    KAFKA_TOPIC_MARKET_DATA: str = "market-data-events"
    KAFKA_TOPIC_DLT: str = "market-data-events-dlt"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_MAX_POLL_RECORDS: int = 500

    # Pipeline
    PIPELINE_BATCH_SIZE: int = 1000  # records per processing batch
    PIPELINE_FLUSH_INTERVAL_SECONDS: int = 30  # how often to flush buffer to DB
    MAX_RETRY_ATTEMPTS: int = 3
    OUTLIER_ZSCORE_THRESHOLD: float = 3.0  # z-score for outlier detection

    # Base currency for normalization
    BASE_CURRENCY: str = "EUR"

    # File input directory (for file-based ETL)
    FILE_INPUT_DIR: str = "./data/input"
    FILE_ARCHIVE_DIR: str = "./data/archive"
    FILE_ERROR_DIR: str = "./data/errors"

    # Scheduler
    SCHEDULER_ENABLED: bool = True
    FILE_SCAN_INTERVAL_MINUTES: int = 5


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
