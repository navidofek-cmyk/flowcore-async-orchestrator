"""Redis-backed broker for distributed execution."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from inspect import isawaitable
from typing import TypeVar, cast

from redis.asyncio import Redis

from flowcore.brokers.base import BrokerMessage
from flowcore.core.execution import ExecutionResult
from flowcore.core.serialization import (
    JSONValue,
    serialize_value,
    WorkerResult,
    WorkItem,
    dumps_json,
    loads_json,
)
from flowcore.exceptions import BrokerError, SerializationError
from flowcore.observability.metrics import get_metrics
from flowcore.reliability.dlq import DLQEntry

T = TypeVar("T")


@dataclass(slots=True)
class RedisBroker:
    """Redis queue broker with leased processing for at-least-once delivery."""

    redis: Redis
    queue_name: str = "flowcore"
    lease_ttl_seconds: int = 30
    result_queue_name: str = ""
    _processing_queue_name: str = field(init=False, repr=False)
    _dlq_queue_name: str = field(init=False, repr=False)
    _runs_queue_name: str = field(init=False, repr=False)
    _workers_hash_name: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._processing_queue_name = f"{self.queue_name}:processing"
        if not self.result_queue_name:
            self.result_queue_name = f"{self.queue_name}:results"
        self._dlq_queue_name = f"{self.queue_name}:dlq"
        self._runs_queue_name = f"{self.queue_name}:runs"
        self._workers_hash_name = f"{self.queue_name}:workers"

    @classmethod
    def from_url(
        cls,
        redis_url: str,
        *,
        queue_name: str = "flowcore",
        lease_ttl_seconds: int = 30,
    ) -> RedisBroker:
        return cls(
            redis=Redis.from_url(redis_url),
            queue_name=queue_name,
            lease_ttl_seconds=lease_ttl_seconds,
        )

    async def enqueue(self, payload: WorkItem) -> BrokerMessage[WorkItem]:
        envelope = self._work_envelope(payload)
        try:
            await _resolve_redis_call(self.redis.lpush(self.queue_name, envelope))
            await self._refresh_queue_depth()
        except Exception as exc:
            raise BrokerError("Failed to enqueue work item into Redis.") from exc
        token, _ = self._decode_work_envelope(envelope)
        return BrokerMessage(
            token=token,
            payload=payload,
            receipt=envelope,
        )

    async def dequeue(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> BrokerMessage[WorkItem] | None:
        await self.requeue_expired()
        timeout = 0 if timeout_seconds is None else int(timeout_seconds)
        try:
            raw: object = await _resolve_redis_call(
                self.redis.brpoplpush(
                    self.queue_name,
                    self._processing_queue_name,
                    timeout=timeout,
                )
            )
        except Exception as exc:
            raise BrokerError("Failed to dequeue work item from Redis.") from exc
        if raw is None:
            return None
        if not isinstance(raw, bytes | str):
            raise BrokerError("Redis returned an invalid work-item payload.")
        encoded = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        token, payload = self._decode_work_envelope(encoded)
        await self._set_lease(token)
        await self._refresh_queue_depth()
        return BrokerMessage(
            token=token,
            payload=WorkItem.from_dict(payload),
            receipt=encoded,
        )

    async def ack(self, message: BrokerMessage[WorkItem]) -> None:
        try:
            await _resolve_redis_call(
                self.redis.lrem(self._processing_queue_name, 1, message.receipt)
            )
            await _resolve_redis_call(self.redis.delete(self._lease_key(message.token)))
            await self._refresh_queue_depth()
        except Exception as exc:
            raise BrokerError("Failed to acknowledge Redis work item.") from exc

    async def nack(self, message: BrokerMessage[WorkItem]) -> None:
        try:
            await _resolve_redis_call(
                self.redis.lrem(self._processing_queue_name, 1, message.receipt)
            )
            await _resolve_redis_call(self.redis.lpush(self.queue_name, message.receipt))
            await _resolve_redis_call(self.redis.delete(self._lease_key(message.token)))
            await self._refresh_queue_depth()
        except Exception as exc:
            raise BrokerError("Failed to requeue Redis work item.") from exc

    async def depth(self) -> int:
        try:
            return int(await _resolve_redis_call(self.redis.llen(self.queue_name)))
        except Exception as exc:
            raise BrokerError("Failed to read Redis queue depth.") from exc

    async def publish_result(self, result: WorkerResult) -> None:
        try:
            await _resolve_redis_call(
                self.redis.rpush(self.result_queue_name, dumps_json(result.to_dict()))
            )
        except Exception as exc:
            raise BrokerError("Failed to publish worker result to Redis.") from exc

    async def dequeue_result(self, *, timeout_seconds: float = 1.0) -> WorkerResult | None:
        timeout = max(1, int(timeout_seconds))
        try:
            raw: object = await _resolve_redis_call(
                self.redis.brpop([self.result_queue_name], timeout=timeout)
            )
        except Exception as exc:
            raise BrokerError("Failed to dequeue worker result from Redis.") from exc
        if raw is None:
            return None
        if not isinstance(raw, list | tuple) or len(raw) != 2:
            raise BrokerError("Redis returned an invalid worker result payload.")
        _, encoded = raw
        if not isinstance(encoded, bytes | str):
            raise BrokerError("Redis returned an invalid worker result payload.")
        serialized = encoded.decode("utf-8") if isinstance(encoded, bytes) else encoded
        payload = loads_json(serialized)
        if not isinstance(payload, dict):
            raise SerializationError("Worker result payload is invalid.")
        return WorkerResult.from_dict(payload)

    async def requeue_expired(self) -> None:
        try:
            raw_messages: object = await _resolve_redis_call(
                self.redis.lrange(self._processing_queue_name, 0, -1)
            )
        except Exception as exc:
            raise BrokerError("Failed to inspect Redis processing queue.") from exc
        if not isinstance(raw_messages, list):
            raise BrokerError("Redis processing queue returned an invalid payload list.")
        for raw_message in raw_messages:
            if not isinstance(raw_message, bytes | str):
                raise BrokerError("Redis processing queue contained an invalid work item.")
            encoded = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
            token, _ = self._decode_work_envelope(encoded)
            if await _resolve_redis_call(self.redis.exists(self._lease_key(token))):
                continue
            await _resolve_redis_call(self.redis.lrem(self._processing_queue_name, 1, encoded))
            await _resolve_redis_call(self.redis.lpush(self.queue_name, encoded))
        await self._refresh_queue_depth()

    async def processing_depth(self) -> int:
        try:
            return int(await _resolve_redis_call(self.redis.llen(self._processing_queue_name)))
        except Exception as exc:
            raise BrokerError("Failed to read Redis processing depth.") from exc

    async def result_depth(self) -> int:
        try:
            return int(await _resolve_redis_call(self.redis.llen(self.result_queue_name)))
        except Exception as exc:
            raise BrokerError("Failed to read Redis result queue depth.") from exc

    async def dlq_depth(self) -> int:
        try:
            depth = int(await _resolve_redis_call(self.redis.llen(self._dlq_queue_name)))
        except Exception as exc:
            raise BrokerError("Failed to read Redis DLQ depth.") from exc
        get_metrics().set_dlq_size(queue_name=self.queue_name, size=depth)
        return depth

    async def record_dlq_entry(self, entry: DLQEntry, *, work_item: WorkItem | None = None) -> None:
        payload: dict[str, JSONValue] = {
            "entry": cast(dict[str, JSONValue], entry.to_dict()),
        }
        if work_item is not None:
            payload["work_item"] = work_item.to_dict()
        try:
            await _resolve_redis_call(self.redis.lpush(self._dlq_queue_name, dumps_json(payload)))
            await self.dlq_depth()
        except Exception as exc:
            raise BrokerError("Failed to record Redis DLQ entry.") from exc

    async def list_dlq_entries(self, *, limit: int = 20) -> tuple[dict[str, JSONValue], ...]:
        try:
            raw_entries: object = await _resolve_redis_call(
                self.redis.lrange(self._dlq_queue_name, 0, max(0, limit - 1))
            )
        except Exception as exc:
            raise BrokerError("Failed to list Redis DLQ entries.") from exc
        if not isinstance(raw_entries, list):
            raise BrokerError("Redis DLQ returned an invalid payload list.")
        parsed: list[dict[str, JSONValue]] = []
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, bytes | str):
                raise BrokerError("Redis DLQ contained an invalid entry.")
            serialized = raw_entry.decode("utf-8") if isinstance(raw_entry, bytes) else raw_entry
            payload = loads_json(serialized)
            if not isinstance(payload, dict):
                raise SerializationError("Redis DLQ payload is invalid.")
            parsed.append(payload)
        return tuple(parsed)

    async def replay_dlq(self, *, limit: int = 1) -> int:
        replayed = 0
        entries = await self.list_dlq_entries(limit=limit)
        for entry in entries:
            raw_work_item = entry.get("work_item")
            if not isinstance(raw_work_item, dict):
                continue
            await self.enqueue(WorkItem.from_dict(raw_work_item))
            await _resolve_redis_call(self.redis.lrem(self._dlq_queue_name, 1, dumps_json(entry)))
            replayed += 1
        await self.dlq_depth()
        return replayed

    async def record_run_summary(
        self,
        result: ExecutionResult,
        *,
        run_id: str,
        dag_name: str,
        status: str,
        executor: str,
    ) -> None:
        started_at = min(
            (
                metadata.started_at
                for metadata in result.metadata.values()
                if metadata.started_at is not None
            ),
            default=None,
        )
        finished_at = max(
            (
                metadata.finished_at
                for metadata in result.metadata.values()
                if metadata.finished_at is not None
            ),
            default=None,
        )

        payload: dict[str, JSONValue] = {
            "run_id": run_id,
            "dag_name": dag_name,
            "executor": executor,
            "trace_id": result.trace_id,
            "status": status,
            "node_count": len(result.metadata),
            "succeeded": sum(
                1 for metadata in result.metadata.values() if metadata.status.value == "SUCCEEDED"
            ),
            "failed": sum(
                1 for metadata in result.metadata.values() if metadata.status.value == "FAILED"
            ),
            "skipped": sum(
                1 for metadata in result.metadata.values() if metadata.status.value == "SKIPPED"
            ),
            "started_at": started_at.isoformat() if started_at is not None else None,
            "finished_at": finished_at.isoformat() if finished_at is not None else None,
            "duration_seconds": (
                None
                if started_at is None or finished_at is None
                else (finished_at - started_at).total_seconds()
            ),
            "nodes": [
                {
                    "node_id": metadata.node_id,
                    "task_name": metadata.task_name,
                    "status": metadata.status.value,
                    "attempt_count": metadata.attempt_count,
                    "duration_seconds": metadata.duration_seconds,
                    "error_message": metadata.error_message,
                    "trace_id": metadata.trace_id,
                }
                for metadata in sorted(
                    result.metadata.values(),
                    key=lambda item: item.node_id,
                )
            ],
            "outputs": {
                node_id: _safe_json_value(output) for node_id, output in result.outputs.items()
            },
        }
        try:
            await _resolve_redis_call(self.redis.lpush(self._runs_queue_name, dumps_json(payload)))
            await _resolve_redis_call(self.redis.ltrim(self._runs_queue_name, 0, 9))
        except Exception as exc:
            raise BrokerError("Failed to record Redis run summary.") from exc

    async def list_recent_runs(self, *, limit: int = 5) -> tuple[dict[str, JSONValue], ...]:
        try:
            raw_entries: object = await _resolve_redis_call(
                self.redis.lrange(self._runs_queue_name, 0, max(0, limit - 1))
            )
        except Exception as exc:
            raise BrokerError("Failed to list Redis run summaries.") from exc
        if not isinstance(raw_entries, list):
            raise BrokerError("Redis run summaries returned an invalid payload list.")
        parsed: list[dict[str, JSONValue]] = []
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, bytes | str):
                raise BrokerError("Redis run summaries contained an invalid entry.")
            serialized = raw_entry.decode("utf-8") if isinstance(raw_entry, bytes) else raw_entry
            payload = loads_json(serialized)
            if not isinstance(payload, dict):
                raise SerializationError("Redis run summary payload is invalid.")
            parsed.append(payload)
        return tuple(parsed)

    async def get_run_summary(self, run_id: str) -> dict[str, JSONValue] | None:
        for run in await self.list_recent_runs(limit=50):
            if run.get("run_id") == run_id:
                return run
        return None

    async def record_worker_heartbeat(
        self,
        *,
        worker_id: str,
        status: str,
        hostname: str,
        pid: int,
        task_name: str | None = None,
    ) -> None:
        payload: dict[str, JSONValue] = {
            "worker_id": worker_id,
            "status": status,
            "hostname": hostname,
            "pid": pid,
            "queue_name": self.queue_name,
            "task_name": task_name,
            "last_seen_at": self._utcnow().isoformat(),
        }
        try:
            await _resolve_redis_call(
                self.redis.hset(self._workers_hash_name, worker_id, dumps_json(payload))
            )
        except Exception as exc:
            raise BrokerError("Failed to record worker heartbeat.") from exc

    async def list_workers(self, *, active_within_seconds: int = 30) -> tuple[dict[str, JSONValue], ...]:
        try:
            raw_workers: object = await _resolve_redis_call(self.redis.hgetall(self._workers_hash_name))
        except Exception as exc:
            raise BrokerError("Failed to list worker heartbeats.") from exc
        if not isinstance(raw_workers, dict):
            raise BrokerError("Redis worker heartbeats returned an invalid payload.")

        cutoff = self._utcnow().timestamp() - active_within_seconds
        parsed: list[dict[str, JSONValue]] = []
        for raw_payload in raw_workers.values():
            if not isinstance(raw_payload, bytes | str):
                raise BrokerError("Redis worker heartbeats contained an invalid entry.")
            serialized = (
                raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else raw_payload
            )
            payload = loads_json(serialized)
            if not isinstance(payload, dict):
                raise SerializationError("Redis worker heartbeat payload is invalid.")
            last_seen_at = payload.get("last_seen_at")
            if not isinstance(last_seen_at, str):
                continue
            if datetime.fromisoformat(last_seen_at).timestamp() < cutoff:
                continue
            parsed.append(payload)
        parsed.sort(key=lambda item: cast(str, item["worker_id"]))
        return tuple(parsed)

    async def status(self) -> dict[str, object]:
        return {
            "queue_name": self.queue_name,
            "queue_depth": await self.depth(),
            "processing_depth": await self.processing_depth(),
            "result_depth": await self.result_depth(),
            "dlq_depth": await self.dlq_depth(),
            "recent_runs": list(await self.list_recent_runs()),
            "workers": list(await self.list_workers()),
        }

    async def close(self) -> None:
        await self.redis.aclose()

    async def _set_lease(self, token: str) -> None:
        try:
            await _resolve_redis_call(
                self.redis.set(self._lease_key(token), "1", ex=self.lease_ttl_seconds)
            )
        except Exception as exc:
            raise BrokerError("Failed to set Redis work-item lease.") from exc

    def _lease_key(self, token: str) -> str:
        return f"{self.queue_name}:lease:{token}"

    async def _refresh_queue_depth(self) -> None:
        get_metrics().set_queue_depth(queue_name=self.queue_name, depth=await self.depth())

    def _utcnow(self) -> datetime:
        return datetime.now(UTC)

    def _work_envelope(self, payload: WorkItem) -> str:
        token = uuid.uuid4().hex
        return dumps_json(
            {
                "token": token,
                "payload": payload.to_dict(),
            }
        )

    def _decode_work_envelope(self, raw: str) -> tuple[str, dict[str, JSONValue]]:
        payload = loads_json(raw)
        if not isinstance(payload, dict):
            raise SerializationError("Redis work envelope is invalid.")
        token = payload.get("token")
        body = payload.get("payload")
        if not isinstance(token, str) or not isinstance(body, dict):
            raise SerializationError("Redis work envelope is invalid.")
        return token, body


async def _resolve_redis_call(value: object) -> T:
    if isawaitable(value):
        return await cast(Awaitable[T], value)
    return cast(T, value)


def _safe_json_value(value: object) -> JSONValue:
    try:
        return serialize_value(value)
    except SerializationError:
        return repr(value)
