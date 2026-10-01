from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RECSYS_", env_file=".env", extra="ignore")

    data_dir: Path = Path("data/raw")
    artifact_dir: Path = Path("artifacts")
    tracking_uri: str = "http://mlflow:5000"
    experiment_name: str = "retailrocket-recommendations"
    assignment_salt: str = "replace-this-before-production"
    api_key: str | None = None
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "recommendation-events"
    event_log_dir: Path = Path("data/events")
    recommendation_count: int = 10


settings = Settings()
