# app/kafka/consumer.py
import asyncio
import json
import logging
import ssl

import httpx
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.core.config import settings
from app.kafka.producer import publish_event
from app.services.inventory import InventoryService

logger = logging.getLogger(__name__)

TOPICS = ["order.created", "inventory.release"]
_RESTART_DELAY_SECONDS = 10


async def _seed_movie_on_demand(inventory: InventoryService, movie_id: int) -> bool:
    """
    Intenta sembrar el inventario de una película consultando catalog-service.
    Retorna True si la siembra fue exitosa, False en caso contrario.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{settings.CATALOG_SERVICE_URL}/api/v1/movies/{movie_id}"
            )
            if resp.status_code == 404:
                logger.warning("Movie %s not found in catalog — cannot seed inventory", movie_id)
                return False
            resp.raise_for_status()
            data = resp.json()

        available = data.get("available_tickets")
        if available is None:
            logger.warning("Movie %s has no available_tickets field in catalog response", movie_id)
            return False

        await inventory.seed(movie_id, available)
        logger.info("On-demand seed | movie_id=%s | available=%s", movie_id, available)
        return True

    except Exception as e:
        logger.error("On-demand seed failed | movie_id=%s | error=%s", movie_id, e)
        return False


async def _handle_order_created(payload: dict, inventory: InventoryService) -> None:
    order_id = payload.get("order_id")
    movie_id = payload.get("movie_id")
    quantity = payload.get("quantity", 0)
    user_id = payload.get("user_id")
    user_email = payload.get("user_email", "?")
    showtime_id = payload.get("showtime_id")

    logger.info(
        "order.created received | movie_id=%s | qty=%s | user=%s",
        movie_id, quantity, user_email,
    )

    result = await inventory.reserve(movie_id, quantity, order_id=order_id)

    if result == -2:
        # Inventario no sembrado — intentar siembra on-demand desde catalog-service
        logger.warning(
            "Inventory not seeded for movie_id=%s — attempting on-demand seed", movie_id
        )
        seeded = await _seed_movie_on_demand(inventory, movie_id)
        if seeded:
            result = await inventory.reserve(movie_id, quantity, order_id=order_id)
        else:
            result = -1  # Tratar como insuficiente si no se pudo sembrar

    if result == -1:
        logger.warning(
            "Inventory insufficient | movie_id=%s | qty_requested=%s | user=%s",
            movie_id, quantity, user_email,
        )
        evt: dict = {
            "order_id": order_id,
            "movie_id": movie_id,
            "quantity_requested": quantity,
            "user_id": user_id,
            "user_email": user_email,
        }
        if showtime_id is not None:
            evt["showtime_id"] = showtime_id
        await publish_event("inventory.insufficient", evt)
        return

    if result == -3:
        logger.info("Duplicate reservation ignored | order_id=%s | movie_id=%s", order_id, movie_id)
        return

    logger.info(
        "Inventory reserved | movie_id=%s | qty=%s | remaining=%s",
        movie_id, quantity, result,
    )
    reserved_evt: dict = {
        "order_id": order_id,
        "movie_id": movie_id,
        "quantity": quantity,
        "remaining": result,
        "user_id": user_id,
        "user_email": user_email,
    }
    if showtime_id is not None:
        reserved_evt["showtime_id"] = showtime_id
    await publish_event("inventory.reserved", reserved_evt)


async def _handle_inventory_release(payload: dict, inventory: InventoryService) -> None:
    order_id = payload.get("order_id")
    movie_id = payload.get("movie_id")
    quantity = payload.get("quantity", 0)

    if not movie_id or quantity <= 0:
        logger.warning("inventory.release payload inválido: %s", payload)
        return

    await inventory.release(movie_id, quantity, order_id=order_id)


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
        # Solo debe reaccionar a órdenes nuevas; el backlog histórico puede compensar dos veces.
        auto_offset_reset="latest",
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
                elif topic == "inventory.release":
                    await _handle_inventory_release(payload, inventory)
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
