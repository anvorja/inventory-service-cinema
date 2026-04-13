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

    # URL interna de catalog-service para sembrar el inventario al arrancar
    CATALOG_SERVICE_URL: str = "http://catalog-service:8006"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


settings = Settings()
