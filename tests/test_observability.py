from __future__ import annotations

import pytest

from flowcore.brokers.redis import RedisBroker
from flowcore.core.dag import DAG
from flowcore.core.serialization import WorkItem
from flowcore.core.task import task
from flowcore.exceptions import PermanentTaskExecutionError
from flowcore.observability.metrics import get_metrics
from flowcore.observability.tracing import get_current_trace_id
from flowcore.reliability.idempotency import generate_idempotency_key
from flowcore.reliability.retry import RetryPolicy
from flowcore.workers.worker import DistributedWorker


@pytest.mark.asyncio
async def test_local_dag_run_generates_trace_id() -> None:
    @task
    async def simple() -> str:
        return "ok"

    async with DAG("trace-local") as dag:
        dag.add(simple)
        result = await dag.run()

    assert result.trace_id is not None
    assert result.metadata["simple:0"].trace_id == result.trace_id


def test_work_item_roundtrip_preserves_trace_id() -> None:
    work_item = WorkItem(
        dag_run_id="run-1",
        node_id="node-1",
        task_name="task",
        task_identity="tests:task",
        idempotency_key="idemp-1",
        trace_id="trace-123",
        args=("value",),
        kwargs={"count": 2},
    )

    restored = WorkItem.from_dict(work_item.to_dict())

    assert restored == work_item
    assert restored.trace_id == "trace-123"


@pytest.mark.asyncio
async def test_worker_restores_trace_context(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    worker = DistributedWorker(broker=broker)

    @task(name=f"trace_worker_{redis_queue_name}")
    async def trace_task() -> str | None:
        return get_current_trace_id()

    work_item = WorkItem(
        dag_run_id="run-trace-worker",
        node_id="trace:0",
        task_name=trace_task.name,
        task_identity=trace_task.identity,
        idempotency_key=generate_idempotency_key(
            dag_run_id="run-trace-worker",
            node_id="trace:0",
            task_identity=trace_task.identity,
            args=(),
            kwargs={},
        ),
        trace_id="trace-worker-123",
    )

    await broker.enqueue(work_item)
    await worker.poll_once()
    result = await broker.dequeue_result(timeout_seconds=1.0)

    assert result is not None
    assert result.output == "trace-worker-123"
    assert result.metadata.trace_id == "trace-worker-123"
    await broker.close()


@pytest.mark.asyncio
async def test_metrics_capture_success_failure_and_retry() -> None:
    attempts = 0

    @task
    async def success() -> str:
        return "ok"

    @task(retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0))
    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("retry")
        return "recovered"

    @task
    async def broken() -> str:
        raise RuntimeError("boom")

    async with DAG("metrics-success") as dag:
        dag.add(success)
        await dag.run()

    async with DAG("metrics-retry") as dag:
        dag.add(flaky)
        await dag.run()

    async with DAG("metrics-failure") as dag:
        dag.add(broken)
        with pytest.raises(PermanentTaskExecutionError):
            await dag.run()

    metrics_text = get_metrics().render()

    assert 'flowcore_task_success_total{executor="local",task_name="success"} 1.0' in metrics_text
    assert 'flowcore_task_success_total{executor="local",task_name="flaky"} 1.0' in metrics_text
    assert 'flowcore_task_failure_total{executor="local",task_name="broken"} 1.0' in metrics_text
    assert 'flowcore_task_retry_total{executor="local",task_name="flaky"} 1.0' in metrics_text
    assert 'flowcore_task_duration_seconds_count{executor="local",task_name="success"} 1.0' in (
        metrics_text
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_queue_depth_metric_changes(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    work_item = WorkItem(
        dag_run_id="run-queue",
        node_id="node-queue",
        task_name="task",
        task_identity="tests:queue",
        idempotency_key="idemp-queue",
        trace_id="trace-queue",
    )

    await broker.enqueue(work_item)
    metrics_text = get_metrics().render()

    assert f'flowcore_queue_depth{{queue_name="{redis_queue_name}"}} 1.0' in metrics_text
    await broker.close()
