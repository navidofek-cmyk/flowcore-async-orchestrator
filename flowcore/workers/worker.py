"""Distributed worker implementation."""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from flowcore.brokers.base import BrokerMessage
from flowcore.brokers.redis import RedisBroker
from flowcore.core.execution import AttemptMetadata, ExecutionStatus, NodeExecutionMetadata
from flowcore.core.serialization import SerializedError, WorkerResult, WorkItem
from flowcore.core.task import get_task_registry
from flowcore.observability.logging import get_logger
from flowcore.observability.metrics import get_metrics
from flowcore.observability.tracing import bind_execution_context, get_current_trace_id
from flowcore.reliability.dlq import DLQEntry
from flowcore.reliability.idempotency import RedisIdempotencyStore
from flowcore.reliability.locks import RedisLockManager
from flowcore.reliability.retry import RetryPolicy

logger = get_logger(__name__)


@dataclass(slots=True)
class DistributedWorker:
    """Poll Redis for work items and execute registered FlowCore tasks."""

    broker: RedisBroker
    poll_timeout_seconds: float = 1.0
    idle_sleep_seconds: float = 0.05
    lock_ttl_seconds: int = 30
    worker_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    _idempotency_store: RedisIdempotencyStore = field(init=False, repr=False)
    _lock_manager: RedisLockManager = field(init=False, repr=False)
    _hostname: str = field(init=False, repr=False)
    _pid: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._idempotency_store = RedisIdempotencyStore(self.broker.redis)
        self._lock_manager = RedisLockManager(
            self.broker.redis,
            ttl_seconds=self.lock_ttl_seconds,
        )
        self._hostname = socket.gethostname()
        self._pid = os.getpid()

    async def poll_once(self) -> bool:
        message = await self.broker.dequeue(timeout_seconds=self.poll_timeout_seconds)
        if message is None:
            return False
        await self._handle_message(message)
        return True

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        logger.info(
            "worker_started",
            queue_name=self.broker.queue_name,
            worker_id=self.worker_id,
        )
        await self._heartbeat(status="idle")
        while stop_event is None or not stop_event.is_set():
            processed = await self.poll_once()
            await self._heartbeat(status="busy" if processed else "idle")
            if not processed:
                await asyncio.sleep(self.idle_sleep_seconds)
        logger.info(
            "worker_stopped",
            queue_name=self.broker.queue_name,
            worker_id=self.worker_id,
        )

    async def _handle_message(self, message: BrokerMessage[WorkItem]) -> None:
        work_item = message.payload
        await self._heartbeat(status="busy", task_name=work_item.task_name)
        with bind_execution_context(
            trace_id=work_item.trace_id,
            node_id=work_item.node_id,
            task_name=work_item.task_name,
            executor="distributed",
        ):
            cached = await self._idempotency_store.get_completed_result(work_item.idempotency_key)
            if cached is not None:
                logger.info("worker_cache_hit")
                await self.broker.publish_result(cached)
                await self.broker.ack(message)
                return

            owner = uuid.uuid4().hex
            acquired = await self._lock_manager.acquire(work_item.idempotency_key, owner)
            if not acquired:
                logger.warning("worker_lock_contended")
                await self.broker.nack(message)
                return

            try:
                cached = await self._idempotency_store.get_completed_result(
                    work_item.idempotency_key
                )
                if cached is not None:
                    logger.info("worker_cache_hit")
                    await self.broker.publish_result(cached)
                    await self.broker.ack(message)
                    return

                result = await self._execute_work_item(work_item)
                if result.succeeded:
                    await self._idempotency_store.mark_completed(work_item.idempotency_key, result)
                await self.broker.publish_result(result)
                await self.broker.ack(message)
            finally:
                await self._lock_manager.release(work_item.idempotency_key, owner)
                await self._heartbeat(status="idle")

    async def _execute_work_item(self, work_item: WorkItem) -> WorkerResult:
        task = get_task_registry().get(work_item.task_identity)
        retry_policy = task.retry_policy or RetryPolicy()
        started_at: datetime | None = None
        attempts: list[AttemptMetadata] = []
        metrics = get_metrics()
        metrics.record_task_execution(executor="distributed", task_name=work_item.task_name)
        metrics.inc_worker_active_tasks(queue_name=self.broker.queue_name)

        try:
            for attempt_number in range(1, retry_policy.max_attempts + 1):
                attempt_started_at = self._utcnow()
                if started_at is None:
                    started_at = attempt_started_at

                try:
                    output = await task.func(*work_item.args, **dict(work_item.kwargs))
                except Exception as exc:
                    attempt_finished_at = self._utcnow()
                    attempts.append(
                        AttemptMetadata(
                            attempt_number=attempt_number,
                            status=ExecutionStatus.FAILED,
                            started_at=attempt_started_at,
                            finished_at=attempt_finished_at,
                            duration_seconds=(
                                attempt_finished_at - attempt_started_at
                            ).total_seconds(),
                            error_message=str(exc),
                        )
                    )
                    if attempt_number < retry_policy.max_attempts:
                        metrics.record_task_retry(
                            executor="distributed",
                            task_name=work_item.task_name,
                        )
                        logger.warning("task_retrying", attempt_number=attempt_number)
                        delay = retry_policy.compute_delay(attempt_number)
                        if delay > 0:
                            await asyncio.sleep(delay)
                        continue

                    duration_seconds = (attempt_finished_at - started_at).total_seconds()
                    metrics.record_task_failure(
                        executor="distributed",
                        task_name=work_item.task_name,
                        duration_seconds=duration_seconds,
                    )
                    logger.warning("task_failed", attempt_count=attempt_number)
                    metadata = NodeExecutionMetadata(
                        node_id=work_item.node_id,
                        task_name=work_item.task_name,
                        status=ExecutionStatus.FAILED,
                        attempt_count=attempt_number,
                        started_at=started_at,
                        finished_at=attempt_finished_at,
                        duration_seconds=duration_seconds,
                        error_message=str(exc),
                        trace_id=get_current_trace_id(),
                        attempts=tuple(attempts),
                    )
                    return WorkerResult(
                        dag_run_id=work_item.dag_run_id,
                        node_id=work_item.node_id,
                        task_name=work_item.task_name,
                        idempotency_key=work_item.idempotency_key,
                        metadata=metadata,
                        dlq_entry=DLQEntry(
                            node_id=work_item.node_id,
                            task_name=work_item.task_name,
                            attempt_count=attempt_number,
                            exception_type=type(exc).__name__,
                            error_message=str(exc),
                            trace_id=get_current_trace_id(),
                        ),
                        error=SerializedError(
                            exception_type=type(exc).__name__,
                            error_message=str(exc),
                        ),
                    )

                attempt_finished_at = self._utcnow()
                attempts.append(
                    AttemptMetadata(
                        attempt_number=attempt_number,
                        status=ExecutionStatus.SUCCEEDED,
                        started_at=attempt_started_at,
                        finished_at=attempt_finished_at,
                        duration_seconds=(attempt_finished_at - attempt_started_at).total_seconds(),
                        error_message=None,
                    )
                )
                duration_seconds = (attempt_finished_at - started_at).total_seconds()
                metrics.record_task_success(
                    executor="distributed",
                    task_name=work_item.task_name,
                    duration_seconds=duration_seconds,
                )
                logger.info("task_succeeded", attempt_count=attempt_number)
                metadata = NodeExecutionMetadata(
                    node_id=work_item.node_id,
                    task_name=work_item.task_name,
                    status=ExecutionStatus.SUCCEEDED,
                    attempt_count=attempt_number,
                    started_at=started_at,
                    finished_at=attempt_finished_at,
                    duration_seconds=duration_seconds,
                    error_message=None,
                    trace_id=get_current_trace_id(),
                    attempts=tuple(attempts),
                )
                return WorkerResult(
                    dag_run_id=work_item.dag_run_id,
                    node_id=work_item.node_id,
                    task_name=work_item.task_name,
                    idempotency_key=work_item.idempotency_key,
                    metadata=metadata,
                    output=output,
                )
        finally:
            metrics.dec_worker_active_tasks(queue_name=self.broker.queue_name)

        raise AssertionError("Worker retry loop exited unexpectedly.")

    def _utcnow(self) -> datetime:
        return datetime.now(UTC)

    async def _heartbeat(self, *, status: str, task_name: str | None = None) -> None:
        await self.broker.record_worker_heartbeat(
            worker_id=self.worker_id,
            status=status,
            hostname=self._hostname,
            pid=self._pid,
            task_name=task_name,
        )
