"""Distributed lock helpers backed by Redis."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from inspect import isawaitable
from typing import TypeVar, cast

from redis.asyncio import Redis

from flowcore.exceptions import LockAcquisitionError

T = TypeVar("T")

_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
"""


@dataclass(slots=True)
class RedisLockManager:
    """Single-Redis distributed lock manager with TTL-based safety."""

    redis: Redis
    namespace: str = "flowcore:locks"
    ttl_seconds: int = 30

    def _key(self, lock_key: str) -> str:
        return f"{self.namespace}:{lock_key}"

    async def acquire(self, lock_key: str, owner: str) -> bool:
        try:
            acquired: object = await _resolve_redis_call(
                self.redis.set(
                    self._key(lock_key),
                    owner,
                    nx=True,
                    ex=self.ttl_seconds,
                )
            )
        except Exception as exc:
            raise LockAcquisitionError("Failed to acquire Redis lock.") from exc
        return bool(acquired)

    async def release(self, lock_key: str, owner: str) -> bool:
        try:
            released: object = await _resolve_redis_call(
                self.redis.eval(_RELEASE_SCRIPT, 1, self._key(lock_key), owner)
            )
        except Exception as exc:
            raise LockAcquisitionError("Failed to release Redis lock.") from exc
        return bool(released)


async def _resolve_redis_call(value: object) -> T:
    if isawaitable(value):
        return await cast(Awaitable[T], value)
    return cast(T, value)
