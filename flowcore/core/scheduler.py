"""Async local scheduler implementation."""

from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime

from flowcore.brokers.redis import RedisBroker
from flowcore.core.dag import ExecutionPlan, RegisteredNode
from flowcore.core.execution import (
    AttemptMetadata,
    ExecutionResult,
    ExecutionStatus,
    NodeExecutionMetadata,
)
from flowcore.core.serialization import WorkItem
from flowcore.exceptions import (
    DependencyResolutionError,
    DependencySkippedError,
    PermanentTaskExecutionError,
    RetryableTaskExecutionError,
    TaskExecutionError,
)
from flowcore.observability.logging import get_logger
from flowcore.observability.metrics import get_metrics
from flowcore.observability.tracing import bind_execution_context, new_trace_id, trace_context
from flowcore.reliability.dlq import DLQEntry, LocalDeadLetterQueue
from flowcore.reliability.idempotency import generate_idempotency_key
from flowcore.reliability.retry import RetryPolicy

logger = get_logger(__name__)


@dataclass(slots=True)
class _MutableNodeMetadata:
    node_id: str
    task_name: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt_count: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    error_message: str | None = None
    trace_id: str | None = None
    attempts: list[AttemptMetadata] = field(default_factory=list)

    def finalize(self) -> NodeExecutionMetadata:
        return NodeExecutionMetadata(
            node_id=self.node_id,
            task_name=self.task_name,
            status=self.status,
            attempt_count=self.attempt_count,
            started_at=self.started_at,
            finished_at=self.finished_at,
            duration_seconds=self.duration_seconds,
            error_message=self.error_message,
            trace_id=self.trace_id,
            attempts=tuple(self.attempts),
        )


class LocalScheduler:
    """Execute a validated DAG plan in a single asyncio event loop."""

    __slots__ = ("_max_concurrency",)

    def __init__(self, *, max_concurrency: int) -> None:
        self._max_concurrency = max_concurrency

    async def run(self, plan: ExecutionPlan) -> ExecutionResult:
        with (
            trace_context(new_trace_id()) as trace_id,
            bind_execution_context(
                trace_id=trace_id,
                executor=plan.executor,
            ),
        ):
            logger.info("dag_run_started", dag_name=plan.dag_name, node_count=len(plan.nodes))
            nodes_by_id = {node.node_id: node for node in plan.nodes}
            dependents: dict[str, list[str]] = defaultdict(list)
            unresolved_counts: dict[str, int] = {}
            ready = deque[str]()
            skipped: set[str] = set()
            results: dict[str, object] = {}
            task_failures: dict[str, TaskExecutionError] = {}
            dlq = LocalDeadLetterQueue()
            metadata: dict[str, _MutableNodeMetadata] = {
                node.node_id: _MutableNodeMetadata(
                    node_id=node.node_id,
                    task_name=node.task.name,
                    trace_id=trace_id,
                )
                for node in plan.nodes
            }

            for node in plan.nodes:
                unresolved_counts[node.node_id] = len(node.dependency_node_ids)
                if not node.dependency_node_ids:
                    ready.append(node.node_id)
                for dependency_id in node.dependency_node_ids:
                    dependents[dependency_id].append(node.node_id)

            in_flight: dict[str, asyncio.Task[object]] = {}

            while ready or in_flight:
                while ready and len(in_flight) < self._max_concurrency:
                    node_id = ready.popleft()
                    if node_id in skipped:
                        continue
                    in_flight[node_id] = asyncio.create_task(
                        self._execute_node(nodes_by_id[node_id], results, metadata[node_id], dlq),
                        name=f"flowcore:{node_id}",
                    )

                if not in_flight:
                    break

                done, _ = await asyncio.wait(
                    in_flight.values(), return_when=asyncio.FIRST_COMPLETED
                )
                for completed in done:
                    finished_node_id = next(
                        node_id for node_id, task in in_flight.items() if task is completed
                    )
                    del in_flight[finished_node_id]
                    try:
                        results[finished_node_id] = completed.result()
                    except TaskExecutionError as exc:
                        task_failures[finished_node_id] = exc
                        skipped_node_ids = self._collect_downstream(finished_node_id, dependents)
                        skipped.update(skipped_node_ids)
                        for skipped_node_id in skipped_node_ids:
                            self._mark_skipped(
                                metadata[skipped_node_id],
                                finished_node_id,
                                nodes_by_id[finished_node_id].task.name,
                            )
                    for dependent_id in dependents.get(finished_node_id, []):
                        if dependent_id in skipped:
                            continue
                        unresolved_counts[dependent_id] -= 1
                        if unresolved_counts[dependent_id] == 0:
                            ready.append(dependent_id)

            execution_result = ExecutionResult(
                outputs={
                    node.node_id: results[node.node_id]
                    for node in plan.nodes
                    if node.node_id in results and node.node_id not in skipped
                },
                metadata={node.node_id: metadata[node.node_id].finalize() for node in plan.nodes},
                dlq_entries=dlq.entries,
                trace_id=trace_id,
            )

            if task_failures:
                failed_node_id = next(
                    node.node_id for node in plan.nodes if node.node_id in task_failures
                )
                failure = task_failures[failed_node_id]
                failure.execution_result = execution_result
                logger.warning("dag_run_failed", dag_name=plan.dag_name)
                raise failure

            logger.info("dag_run_succeeded", dag_name=plan.dag_name)
            return execution_result

    async def _execute_node(
        self,
        node: RegisteredNode,
        results: dict[str, object],
        metadata: _MutableNodeMetadata,
        dlq: LocalDeadLetterQueue,
    ) -> object:
        policy = node.task.retry_policy or RetryPolicy()
        metrics = get_metrics()
        metrics.record_task_execution(executor="local", task_name=node.task.name)
        try:
            resolved_args = self._resolve_arguments(node, results)
        except DependencyResolutionError as exc:
            return self._raise_permanent_failure(
                node=node,
                metadata=metadata,
                attempt_number=1,
                error=exc,
                dlq=dlq,
            )

        with bind_execution_context(
            trace_id=metadata.trace_id,
            node_id=node.node_id,
            task_name=node.task.name,
            executor="local",
        ):
            for attempt_number in range(1, policy.max_attempts + 1):
                attempt_started_at = self._utcnow()
                if metadata.started_at is None:
                    metadata.started_at = attempt_started_at
                metadata.attempt_count = attempt_number
                metadata.status = ExecutionStatus.RUNNING

                try:
                    result = await node.task.func(*resolved_args[0], **resolved_args[1])
                except Exception as exc:
                    attempt_finished_at = self._utcnow()
                    metadata.error_message = str(exc)
                    metadata.attempts.append(
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
                    if attempt_number < policy.max_attempts:
                        metadata.status = ExecutionStatus.RETRYING
                        metrics.record_task_retry(executor="local", task_name=node.task.name)
                        logger.warning("task_retrying", attempt_number=attempt_number)
                        retry_error = RetryableTaskExecutionError(
                            node.node_id,
                            node.task.name,
                            f"Task {node.task.name!r} failed on attempt "
                            f"{attempt_number} and will retry.",
                            attempt_count=attempt_number,
                            max_attempts=policy.max_attempts,
                        )
                        delay = policy.compute_delay(attempt_number)
                        if delay > 0:
                            await asyncio.sleep(delay)
                        _ = retry_error
                        continue

                    return self._raise_permanent_failure(
                        node=node,
                        metadata=metadata,
                        attempt_number=attempt_number,
                        error=exc,
                        dlq=dlq,
                        finished_at=attempt_finished_at,
                    )

                attempt_finished_at = self._utcnow()
                metadata.status = ExecutionStatus.SUCCEEDED
                metadata.finished_at = attempt_finished_at
                metadata.duration_seconds = self._duration(metadata.started_at, attempt_finished_at)
                metadata.error_message = None
                metadata.attempts.append(
                    AttemptMetadata(
                        attempt_number=attempt_number,
                        status=ExecutionStatus.SUCCEEDED,
                        started_at=attempt_started_at,
                        finished_at=attempt_finished_at,
                        duration_seconds=(attempt_finished_at - attempt_started_at).total_seconds(),
                        error_message=None,
                    )
                )
                metrics.record_task_success(
                    executor="local",
                    task_name=node.task.name,
                    duration_seconds=metadata.duration_seconds,
                )
                logger.info("task_succeeded", attempt_count=attempt_number)
                return result

        raise AssertionError("Retry loop exited unexpectedly.")

    def _resolve_arguments(
        self, node: RegisteredNode, results: dict[str, object]
    ) -> tuple[tuple[object, ...], dict[str, object]]:
        dependency_values = tuple(
            results[dependency_id] for dependency_id in node.dependency_node_ids
        )
        args = tuple(node.invocation.args) + dependency_values
        kwargs = dict(node.invocation.kwargs)

        try:
            node.task.signature.bind(*args, **kwargs)
        except TypeError as exc:
            raise DependencyResolutionError(
                f"Could not resolve arguments for task {node.task.name!r}: {exc}"
            ) from exc

        return args, kwargs

    def _mark_skipped(
        self,
        metadata: _MutableNodeMetadata,
        upstream_node_id: str,
        upstream_task_name: str,
    ) -> None:
        if metadata.status is ExecutionStatus.SKIPPED:
            return
        skipped_at = self._utcnow()
        skipped_error = DependencySkippedError(
            f"Task {metadata.task_name!r} skipped because dependency "
            f"{upstream_task_name!r} ({upstream_node_id}) failed permanently."
        )
        metadata.status = ExecutionStatus.SKIPPED
        metadata.finished_at = skipped_at
        metadata.error_message = str(skipped_error)

    def _raise_permanent_failure(
        self,
        *,
        node: RegisteredNode,
        metadata: _MutableNodeMetadata,
        attempt_number: int,
        error: Exception,
        dlq: LocalDeadLetterQueue,
        finished_at: datetime | None = None,
    ) -> object:
        finalized_at = finished_at or self._utcnow()
        if metadata.started_at is None:
            metadata.started_at = finalized_at
        metadata.status = ExecutionStatus.FAILED
        metadata.attempt_count = attempt_number
        metadata.finished_at = finalized_at
        metadata.duration_seconds = self._duration(metadata.started_at, finalized_at)
        metadata.error_message = str(error)
        metadata.trace_id = metadata.trace_id

        dlq.record(
            DLQEntry(
                node_id=node.node_id,
                task_name=node.task.name,
                attempt_count=attempt_number,
                exception_type=type(error).__name__,
                error_message=str(error),
                trace_id=metadata.trace_id,
            )
        )
        get_metrics().record_task_failure(
            executor="local",
            task_name=node.task.name,
            duration_seconds=metadata.duration_seconds,
        )
        logger.warning("task_failed", attempt_count=attempt_number, error_message=str(error))

        raise PermanentTaskExecutionError(
            node.node_id,
            node.task.name,
            f"Task {node.task.name!r} failed permanently after {attempt_number} attempt(s).",
            attempt_count=attempt_number,
        ) from error

    def _collect_downstream(self, node_id: str, dependents: dict[str, list[str]]) -> set[str]:
        skipped: set[str] = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            for dependent_id in dependents.get(current, []):
                if dependent_id in skipped:
                    continue
                skipped.add(dependent_id)
                queue.append(dependent_id)

        return skipped

    def _duration(self, started_at: datetime | None, finished_at: datetime) -> float | None:
        if started_at is None:
            return None
        return (finished_at - started_at).total_seconds()

    def _utcnow(self) -> datetime:
        return datetime.now(UTC)


class DistributedScheduler(LocalScheduler):
    """Coordinate distributed execution through a Redis broker."""

    __slots__ = ("_broker", "_result_poll_interval")

    def __init__(
        self,
        *,
        broker: RedisBroker,
        max_concurrency: int,
        result_poll_interval: float = 1.0,
    ) -> None:
        super().__init__(max_concurrency=max_concurrency)
        self._broker = broker
        self._result_poll_interval = result_poll_interval

    async def run(self, plan: ExecutionPlan) -> ExecutionResult:
        dag_run_id = f"{plan.dag_name}:{uuid.uuid4()}"
        with (
            trace_context(new_trace_id()) as trace_id,
            bind_execution_context(
                trace_id=trace_id,
                executor=plan.executor,
            ),
        ):
            logger.info("dag_run_started", dag_name=plan.dag_name, node_count=len(plan.nodes))
            nodes_by_id = {node.node_id: node for node in plan.nodes}
            dependents: dict[str, list[str]] = defaultdict(list)
            unresolved_counts: dict[str, int] = {}
            ready = deque[str]()
            running: set[str] = set()
            skipped: set[str] = set()
            results: dict[str, object] = {}
            enqueued_work_items: dict[str, WorkItem] = {}
            task_failures: dict[str, TaskExecutionError] = {}
            dlq = LocalDeadLetterQueue()
            metadata: dict[str, _MutableNodeMetadata] = {
                node.node_id: _MutableNodeMetadata(
                    node_id=node.node_id,
                    task_name=node.task.name,
                    trace_id=trace_id,
                )
                for node in plan.nodes
            }

            for node in plan.nodes:
                unresolved_counts[node.node_id] = len(node.dependency_node_ids)
                if not node.dependency_node_ids:
                    ready.append(node.node_id)
                for dependency_id in node.dependency_node_ids:
                    dependents[dependency_id].append(node.node_id)

            try:
                while ready or running:
                    while ready and len(running) < self._max_concurrency:
                        node_id = ready.popleft()
                        if node_id in skipped:
                            continue
                        node = nodes_by_id[node_id]
                        try:
                            args, kwargs = self._resolve_arguments(node, results)
                        except DependencyResolutionError as exc:
                            self._handle_local_failure(
                                node=node,
                                metadata=metadata[node_id],
                                task_failures=task_failures,
                                dlq=dlq,
                                dependents=dependents,
                                skipped=skipped,
                                metadata_by_id=metadata,
                                error=exc,
                            )
                            continue

                        idempotency_key = generate_idempotency_key(
                            dag_run_id=dag_run_id,
                            node_id=node.node_id,
                            task_identity=node.task.identity,
                            args=args,
                            kwargs=kwargs,
                        )
                        work_item = WorkItem(
                            dag_run_id=dag_run_id,
                            node_id=node.node_id,
                            task_name=node.task.name,
                            task_identity=node.task.identity,
                            idempotency_key=idempotency_key,
                            trace_id=trace_id,
                            args=args,
                            kwargs=kwargs,
                        )
                        await self._broker.enqueue(work_item)
                        enqueued_work_items[node_id] = work_item
                        running.add(node.node_id)

                    if not running:
                        break

                    result = await self._broker.dequeue_result(
                        timeout_seconds=self._result_poll_interval
                    )
                    if result is None or result.node_id not in running:
                        continue

                    running.remove(result.node_id)
                    metadata[result.node_id] = _MutableNodeMetadata(
                        node_id=result.metadata.node_id,
                        task_name=result.metadata.task_name,
                        status=result.metadata.status,
                        attempt_count=result.metadata.attempt_count,
                        started_at=result.metadata.started_at,
                        finished_at=result.metadata.finished_at,
                        duration_seconds=result.metadata.duration_seconds,
                        error_message=result.metadata.error_message,
                        trace_id=result.metadata.trace_id,
                        attempts=list(result.metadata.attempts),
                    )

                    if result.succeeded:
                        results[result.node_id] = result.output
                        for dependent_id in dependents.get(result.node_id, []):
                            if dependent_id in skipped:
                                continue
                            unresolved_counts[dependent_id] -= 1
                            if unresolved_counts[dependent_id] == 0:
                                ready.append(dependent_id)
                        continue

                    if result.dlq_entry is not None:
                        dlq.record(result.dlq_entry)
                        await self._broker.record_dlq_entry(
                            result.dlq_entry,
                            work_item=enqueued_work_items.get(result.node_id),
                        )
                    task_failures[result.node_id] = PermanentTaskExecutionError(
                        result.node_id,
                        result.task_name,
                        f"Task {result.task_name!r} failed permanently after "
                        f"{result.metadata.attempt_count} attempt(s).",
                        attempt_count=result.metadata.attempt_count,
                    )
                    skipped_node_ids = self._collect_downstream(result.node_id, dependents)
                    skipped.update(skipped_node_ids)
                    for skipped_node_id in skipped_node_ids:
                        self._mark_skipped(
                            metadata[skipped_node_id],
                            result.node_id,
                            result.task_name,
                        )

                execution_result = ExecutionResult(
                    outputs={
                        node.node_id: results[node.node_id]
                        for node in plan.nodes
                        if node.node_id in results and node.node_id not in skipped
                    },
                    metadata={
                        node.node_id: metadata[node.node_id].finalize() for node in plan.nodes
                    },
                    dlq_entries=dlq.entries,
                    trace_id=trace_id,
                )

                await self._broker.record_run_summary(
                    execution_result,
                    run_id=dag_run_id,
                    dag_name=plan.dag_name,
                    status="failed" if task_failures else "succeeded",
                    executor=plan.executor,
                )

                if task_failures:
                    failed_node_id = next(
                        node.node_id for node in plan.nodes if node.node_id in task_failures
                    )
                    failure = task_failures[failed_node_id]
                    failure.execution_result = execution_result
                    logger.warning("dag_run_failed", dag_name=plan.dag_name)
                    raise failure

                logger.info("dag_run_succeeded", dag_name=plan.dag_name)
                return execution_result
            finally:
                await self._broker.close()

    def _handle_local_failure(
        self,
        *,
        node: RegisteredNode,
        metadata: _MutableNodeMetadata,
        task_failures: dict[str, TaskExecutionError],
        dlq: LocalDeadLetterQueue,
        dependents: dict[str, list[str]],
        skipped: set[str],
        metadata_by_id: dict[str, _MutableNodeMetadata],
        error: Exception,
    ) -> None:
        finalized_at = self._utcnow()
        metadata.started_at = finalized_at
        metadata.finished_at = finalized_at
        metadata.status = ExecutionStatus.FAILED
        metadata.attempt_count = 1
        metadata.duration_seconds = 0.0
        metadata.error_message = str(error)
        dlq.record(
            DLQEntry(
                node_id=node.node_id,
                task_name=node.task.name,
                attempt_count=1,
                exception_type=type(error).__name__,
                error_message=str(error),
            )
        )
        task_failures[node.node_id] = PermanentTaskExecutionError(
            node.node_id,
            node.task.name,
            f"Task {node.task.name!r} failed permanently after 1 attempt(s).",
            attempt_count=1,
        )
        skipped_node_ids = self._collect_downstream(node.node_id, dependents)
        skipped.update(skipped_node_ids)
        for skipped_node_id in skipped_node_ids:
            self._mark_skipped(
                metadata_by_id[skipped_node_id],
                node.node_id,
                node.task.name,
            )
