# app/services/inventory.py
import logging
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

REDIS_PREFIX = "inv:movie:"
RESERVATION_PREFIX = "inv:reservation:order:"

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

_LUA_RESERVE_WITH_ORDER = """
if redis.call('EXISTS', KEYS[2]) == 1 then return -3 end
local current = redis.call('GET', KEYS[1])
if not current then return -2 end
current = tonumber(current)
if current < tonumber(ARGV[1]) then return -1 end
local remaining = redis.call('DECRBY', KEYS[1], tonumber(ARGV[1]))
redis.call('SET', KEYS[2], tonumber(ARGV[1]))
return remaining
"""

_LUA_RELEASE_WITH_ORDER = """
local reserved = redis.call('GET', KEYS[2])
if not reserved then return 0 end
local released = tonumber(reserved)
redis.call('INCRBY', KEYS[1], released)
redis.call('DEL', KEYS[2])
return released
"""


class InventoryService:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._reserve_script = None
        self._reserve_order_script = None
        self._release_order_script = None

    def _key(self, movie_id: int) -> str:
        return f"{REDIS_PREFIX}{movie_id}"

    def _reservation_key(self, order_id: int) -> str:
        return f"{RESERVATION_PREFIX}{order_id}"

    async def _get_reserve_script(self):
        if self._reserve_script is None:
            self._reserve_script = self._redis.register_script(_LUA_RESERVE)
        return self._reserve_script

    async def _get_reserve_order_script(self):
        if self._reserve_order_script is None:
            self._reserve_order_script = self._redis.register_script(_LUA_RESERVE_WITH_ORDER)
        return self._reserve_order_script

    async def _get_release_order_script(self):
        if self._release_order_script is None:
            self._release_order_script = self._redis.register_script(_LUA_RELEASE_WITH_ORDER)
        return self._release_order_script

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

    async def reserve(self, movie_id: int, quantity: int, order_id: int | None = None) -> int:
        """
        Reserva `quantity` tickets de forma atómica.
        Devuelve el nuevo stock disponible, -1 si insuficiente, -2 si no sembrado.
        Si `order_id` ya existe, devuelve -3 para deduplicar reintentos.
        """
        if order_id is None:
            script = await self._get_reserve_script()
            result = await script(keys=[self._key(movie_id)], args=[quantity])
            return int(result)

        script = await self._get_reserve_order_script()
        result = await script(
            keys=[self._key(movie_id), self._reservation_key(order_id)],
            args=[quantity],
        )
        return int(result)

    async def release(self, movie_id: int, quantity: int, order_id: int | None = None) -> int:
        """Libera tickets reservados (compensación de saga)."""
        if order_id is not None:
            script = await self._get_release_order_script()
            result = await script(
                keys=[self._key(movie_id), self._reservation_key(order_id)],
                args=[],
            )
            released = int(result)
            if released > 0:
                logger.info("Released by order | movie_id=%s | order_id=%s | qty=%s", movie_id, order_id, released)
            else:
                logger.info("Release ignored | movie_id=%s | order_id=%s | no active reservation", movie_id, order_id)
            return released

        await self._redis.incrby(self._key(movie_id), quantity)
        logger.info("Released | movie_id=%s | qty=%s", movie_id, quantity)
        return quantity
