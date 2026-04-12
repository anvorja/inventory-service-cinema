# app/services/inventory.py
import logging
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

REDIS_PREFIX = "inv:movie:"

# Lua: decrementa atómicamente si hay suficiente stock.
# Returns:
#   -2  → clave no existe (inventario no sembrado)
#   -1  → stock insuficiente
#   >=0 → nuevo stock disponible tras la reserva
_LUA_RESERVE = """
local current = redis.call('GET', KEYS[1])
if not current then return -2 end
current = tonumber(current)
if current < tonumber(ARGV[1]) then return -1 end
return redis.call('DECRBY', KEYS[1], tonumber(ARGV[1]))
"""


class InventoryService:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._script = None

    def _key(self, movie_id: int) -> str:
        return f"{REDIS_PREFIX}{movie_id}"

    async def _get_script(self):
        if self._script is None:
            self._script = self._redis.register_script(_LUA_RESERVE)
        return self._script

    async def seed(self, movie_id: int, available: int) -> None:
        """Siembra el inventario solo si la clave no existe (NX = no overwrite)."""
        set_ok = await self._redis.set(self._key(movie_id), available, nx=True)
        if set_ok:
            logger.info("Seeded | movie_id=%s | available=%s", movie_id, available)
        else:
            logger.debug("Already seeded | movie_id=%s — skipping", movie_id)

    async def force_set(self, movie_id: int, available: int) -> None:
        """Sobrescribe el inventario (para sincronización manual)."""
        await self._redis.set(self._key(movie_id), available)
        logger.info("Force-set | movie_id=%s | available=%s", movie_id, available)

    async def get(self, movie_id: int) -> int | None:
        """Devuelve el stock actual o None si la clave no existe."""
        val = await self._redis.get(self._key(movie_id))
        return int(val) if val is not None else None

    async def reserve(self, movie_id: int, quantity: int) -> int:
        """
        Reserva `quantity` tickets de forma atómica.
        Devuelve el nuevo stock disponible, -1 si insuficiente, -2 si no sembrado.
        """
        script = await self._get_script()
        result = await script(keys=[self._key(movie_id)], args=[quantity])
        return int(result)

    async def release(self, movie_id: int, quantity: int) -> None:
        """Libera tickets reservados (compensación de saga)."""
        await self._redis.incrby(self._key(movie_id), quantity)
        logger.info("Released | movie_id=%s | qty=%s", movie_id, quantity)
