import os
from typing import Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings using pydantic validation."""

    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Kafka
    kafka_bootstrap_servers: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_raw_audio_topic: str = Field(..., env="KAFKA_RAW_AUDIO_TOPIC")
    kafka_features_topic: str = Field(..., env="KAFKA_FEATURES_TOPIC")
    kafka_group_id: str = Field(..., env="KAFKA_GROUP_ID")

    # Redis
    redis_host: str = Field(..., env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")
    redis_password: Optional[str] = Field(None, env="REDIS_PASSWORD")
    redis_db: int = Field(0, env="REDIS_DB")

    # AWS/S3
    aws_access_key_id: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    aws_region: str = Field("us-east-1", env="AWS_REGION")
    s3_bucket: str = Field(..., env="S3_BUCKET")
    s3_offline_path: str = Field(..., env="S3_OFFLINE_PATH")

    # API
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")

    # Feature Store
    feature_store_name: str = Field(..., env="FEATURE_STORE_NAME")
    feature_update_interval_ms: int = Field(1000, env="FEATURE_UPDATE_INTERVAL_MS")

    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
