# app/main.py
import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from app.core.config import settings
from app.kafka.consumer import start_consumer
from app.kafka.producer import start_producer, stop_producer
from app.services.inventory import InventoryService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_redis: Redis | None = None
_inventory: InventoryService | None = None
_consumer_task: asyncio.Task | None = None


async def _seed_from_cineco_api(inventory: InventoryService) -> None:
    """
    Siembra el inventario Redis consultando cineco-api.
    Usa NX (no overwrite) para no pisar datos de una siembra previa.
    Reintenta con backoff exponencial hasta 5 veces.
    """
    for attempt in range(5):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{settings.CINECO_API_URL}/api/v1/movies")
                resp.raise_for_status()
                data = resp.json()

            # GET /api/v1/movies devuelve una lista; GET /api/v1/movies/home devuelve
            # {cartelera, coming_soon, presales}. Manejamos ambos formatos.
            if isinstance(data, list):
                movies = data
            else:
                movies = (
                    data.get("cartelera", [])
                    + data.get("coming_soon", [])
                    + data.get("presales", [])
                )

            for movie in movies:
                await inventory.seed(movie["id"], movie["available_tickets"])

            logger.info("Inventory seeded from cineco-api | %d movies", len(movies))
            return

        except Exception as e:
            wait = 2 ** attempt
            logger.warning(
                "Seed attempt %d/5 failed: %s — retrying in %ds", attempt + 1, e, wait
            )
            await asyncio.sleep(wait)

    logger.error(
        "Could not seed inventory from cineco-api after 5 attempts. "
        "Service will rely on existing Redis state (if any)."
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _redis, _inventory, _consumer_task

    logger.info("Starting Inventory Service...")

    # Redis
    _redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    _inventory = InventoryService(_redis)

    # Kafka producer
    await start_producer()

    # Sembrar inventario en background (no bloquea el arranque)
    asyncio.create_task(_seed_from_cineco_api(_inventory))

    # Kafka consumer supervisor
    _consumer_task = asyncio.create_task(start_consumer(_inventory))

    logger.info("Inventory Service ready")
    yield

    logger.info("Shutting down Inventory Service...")
    if _consumer_task:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass

    await stop_producer()
    await _redis.aclose()
    logger.info("Inventory Service stopped")


app = FastAPI(title="Inventory Service", lifespan=lifespan)


@app.get("/health")
async def health():
    consumer_running = _consumer_task is not None and not _consumer_task.done()

    redis_ok = False
    if _redis:
        try:
            await _redis.ping()
            redis_ok = True
        except Exception:
            pass

    return {
        "status": "healthy" if consumer_running and redis_ok else "degraded",
        "consumer": "running" if consumer_running else "stopped",
        "redis": "connected" if redis_ok else "disconnected",
    }


@app.get("/inventory/{movie_id}")
async def get_inventory(movie_id: int):
    """Devuelve el stock disponible en Redis para una película."""
    if _inventory is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    count = await _inventory.get(movie_id)
    if count is None:
        raise HTTPException(
            status_code=404,
            detail=f"Inventory for movie {movie_id} not found. May not be seeded yet.",
        )

    return {"movie_id": movie_id, "available_tickets": count}


@app.post("/inventory/{movie_id}/sync")
async def sync_inventory(movie_id: int, available: int):
    """
    Sincroniza manualmente el stock de una película (admin).
    Útil cuando la BD y Redis divergen.
    """
    if _inventory is None:
        raise HTTPException(status_code=503, detail="Service not ready")

    await _inventory.force_set(movie_id, available)
    return {"movie_id": movie_id, "available_tickets": available, "synced": True}
