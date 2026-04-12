# app/kafka/consumer.py
import asyncio
import json
import logging
import ssl

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.core.config import settings
from app.kafka.producer import publish_event
from app.services.inventory import InventoryService

logger = logging.getLogger(__name__)

TOPICS = ["order.created"]
_RESTART_DELAY_SECONDS = 10


async def _handle_order_created(payload: dict, inventory: InventoryService) -> None:
    movie_id = payload.get("movie_id")
    quantity = payload.get("quantity", 0)
    user_id = payload.get("user_id")
    user_email = payload.get("user_email", "?")

    logger.info(
        "order.created received | movie_id=%s | qty=%s | user=%s",
        movie_id, quantity, user_email,
    )

    result = await inventory.reserve(movie_id, quantity)

    if result == -2:
        # Inventario no sembrado — registrar warning pero no bloquear
        # (cineco-api tiene su propio check en BD como segunda capa de seguridad)
        logger.warning(
            "Inventory not seeded for movie_id=%s — skipping Redis check", movie_id
        )
        return

    if result == -1:
        logger.warning(
            "Inventory insufficient | movie_id=%s | qty_requested=%s | user=%s",
            movie_id, quantity, user_email,
        )
        await publish_event("inventory.insufficient", {
            "movie_id": movie_id,
            "quantity_requested": quantity,
            "user_id": user_id,
            "user_email": user_email,
        })
        return

    logger.info(
        "Inventory reserved | movie_id=%s | qty=%s | remaining=%s",
        movie_id, quantity, result,
    )
    await publish_event("inventory.reserved", {
        "movie_id": movie_id,
        "quantity": quantity,
        "remaining": result,
        "user_id": user_id,
        "user_email": user_email,
    })


async def _run_consumer(inventory: InventoryService) -> None:
    ssl_context = ssl.create_default_context()
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.KAFKA_API_KEY,
        sasl_plain_password=settings.KAFKA_API_SECRET,
        ssl_context=ssl_context,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info(
        "Inventory consumer started | topics=%s | group=%s",
        TOPICS, settings.KAFKA_GROUP_ID,
    )

    try:
        async for msg in consumer:
            topic = msg.topic
            payload = msg.value
            try:
                if topic == "order.created":
                    await _handle_order_created(payload, inventory)
                await consumer.commit()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(
                    "Unhandled error | topic=%s | error=%s", topic, e
                )
                await consumer.commit()
    finally:
        await consumer.stop()
        logger.info("Inventory consumer stopped")


async def start_consumer(inventory: InventoryService) -> None:
    """
    Supervisor loop: reinicia el consumer si falla.
    Solo termina cuando la tarea es cancelada (shutdown limpio).
    """
    if not settings.KAFKA_BOOTSTRAP_SERVERS:
        logger.warning("KAFKA_BOOTSTRAP_SERVERS not set — consumer disabled")
        return

    while True:
        try:
            await _run_consumer(inventory)
            break  # salida limpia (solo ocurre si CancelledError se propaga)
        except asyncio.CancelledError:
            logger.info("Inventory consumer task cancelled — shutting down")
            raise
        except KafkaConnectionError as e:
            logger.error("Kafka connection lost: %s — restarting in %ds", e, _RESTART_DELAY_SECONDS)
        except Exception as e:
            logger.error("Consumer crashed: %s — restarting in %ds", e, _RESTART_DELAY_SECONDS)

        await asyncio.sleep(_RESTART_DELAY_SECONDS)
