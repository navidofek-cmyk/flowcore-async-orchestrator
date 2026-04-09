"""Idempotency support for distributed execution."""

from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import Redis

from flowcore.core.serialization import WorkerResult, dumps_json, loads_json, stable_payload_hash
from flowcore.exceptions import IdempotencyStateError


def generate_idempotency_key(
    *,
    dag_run_id: str,
    node_id: str,
    task_identity: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> str:
    """Build a deterministic idempotency key for a logical work item."""

    return stable_payload_hash(
        dag_run_id=dag_run_id,
        node_id=node_id,
        task_identity=task_identity,
        args=args,
        kwargs=kwargs,
    )


@dataclass(slots=True)
class RedisIdempotencyStore:
    """Store successful worker results keyed by idempotency key."""

    redis: Redis
    namespace: str = "flowcore:idempotency"

    def _key(self, idempotency_key: str) -> str:
        return f"{self.namespace}:{idempotency_key}"

    async def get_completed_result(self, idempotency_key: str) -> WorkerResult | None:
        try:
            raw = await self.redis.get(self._key(idempotency_key))
        except Exception as exc:
            raise IdempotencyStateError("Failed to read idempotency state from Redis.") from exc
        if raw is None:
            return None
        payload = loads_json(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        if not isinstance(payload, dict):
            raise IdempotencyStateError("Stored idempotency payload is invalid.")
        return WorkerResult.from_dict(payload)

    async def mark_completed(self, idempotency_key: str, result: WorkerResult) -> None:
        try:
            await self.redis.set(self._key(idempotency_key), dumps_json(result.to_dict()))
        except Exception as exc:
            raise IdempotencyStateError("Failed to persist idempotency state in Redis.") from exc
