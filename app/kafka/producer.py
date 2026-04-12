# app/kafka/producer.py
import json
import logging
import ssl
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.core.config import settings

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def start_producer() -> None:
    global _producer

    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.info("Kafka not configured — producer disabled")
        return

    try:
        ssl_context = ssl.create_default_context()
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=settings.KAFKA_API_KEY,
            sasl_plain_password=settings.KAFKA_API_SECRET,
            ssl_context=ssl_context,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
        )
        await _producer.start()
        logger.info("Kafka producer started")
    except Exception as e:
        logger.error("Producer failed to start: %s — continuing without Kafka", e)
        _producer = None


async def stop_producer() -> None:
    global _producer

    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped")


async def publish_event(topic: str, payload: dict[str, Any]) -> None:
    if _producer is None:
        logger.debug("Kafka unavailable — '%s' not published", topic)
        return

    try:
        await _producer.send_and_wait(topic, value=payload)
        logger.info("Event published | topic=%s | keys=%s", topic, list(payload.keys()))
    except KafkaConnectionError as e:
        logger.error("Connection error publishing '%s': %s", topic, e)
    except Exception as e:
        logger.error("Error publishing '%s': %s", topic, e)
