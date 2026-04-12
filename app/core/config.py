# app/core/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SERVICE_NAME: str = "inventory-service"

    # Kafka (Confluent Cloud)
    KAFKA_BOOTSTRAP_SERVERS: str = ""
    KAFKA_API_KEY: str = ""
    KAFKA_API_SECRET: str = ""
    KAFKA_GROUP_ID: str = "inventory-service-group"

    # Redis
    REDIS_URL: str = ""

    # URL interna de cineco-api para sembrar el inventario al arrancar
    CINECO_API_URL: str = "http://cineco-api:8000"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


settings = Settings()
