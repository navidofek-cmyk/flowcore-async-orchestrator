from __future__ import annotations

import asyncio

import pytest

from flowcore.brokers.redis import RedisBroker
from flowcore.core.serialization import WorkItem
from flowcore.core.task import task
from flowcore.reliability.idempotency import generate_idempotency_key
from flowcore.reliability.retry import RetryPolicy
from flowcore.workers.worker import DistributedWorker


@pytest.mark.asyncio
@pytest.mark.integration
async def test_worker_executes_simple_task(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    worker = DistributedWorker(broker=broker)

    @task(name=f"worker_simple_{redis_queue_name}")
    async def simple() -> str:
        return "done"

    work_item = WorkItem(
        dag_run_id="run-worker-1",
        node_id="simple:0",
        task_name=simple.name,
        task_identity=simple.identity,
        idempotency_key=generate_idempotency_key(
            dag_run_id="run-worker-1",
            node_id="simple:0",
            task_identity=simple.identity,
            args=(),
            kwargs={},
        ),
        trace_id="trace-worker-1",
    )

    await broker.enqueue(work_item)
    processed = await worker.poll_once()
    result = await broker.dequeue_result(timeout_seconds=1.0)

    assert processed is True
    assert result is not None
    assert result.output == "done"
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_worker_handles_failure_and_retry(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    worker = DistributedWorker(broker=broker)
    attempts = 0

    @task(
        name=f"worker_flaky_{redis_queue_name}",
        retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0),
    )
    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    work_item = WorkItem(
        dag_run_id="run-worker-2",
        node_id="flaky:0",
        task_name=flaky.name,
        task_identity=flaky.identity,
        idempotency_key=generate_idempotency_key(
            dag_run_id="run-worker-2",
            node_id="flaky:0",
            task_identity=flaky.identity,
            args=(),
            kwargs={},
        ),
        trace_id="trace-worker-2",
    )

    await broker.enqueue(work_item)
    await worker.poll_once()
    result = await broker.dequeue_result(timeout_seconds=1.0)

    assert result is not None
    assert result.succeeded is False
    assert result.metadata.attempt_count == 2
    assert attempts == 2
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_worker_respects_idempotency_after_success(
    redis_url: str,
    redis_queue_name: str,
) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    worker = DistributedWorker(broker=broker)
    executions = 0

    @task(name=f"worker_idempotent_{redis_queue_name}")
    async def idempotent() -> str:
        nonlocal executions
        executions += 1
        return "ok"

    idempotency_key = generate_idempotency_key(
        dag_run_id="run-worker-3",
        node_id="idempotent:0",
        task_identity=idempotent.identity,
        args=(),
        kwargs={},
    )
    work_item = WorkItem(
        dag_run_id="run-worker-3",
        node_id="idempotent:0",
        task_name=idempotent.name,
        task_identity=idempotent.identity,
        idempotency_key=idempotency_key,
        trace_id="trace-worker-3",
    )

    await broker.enqueue(work_item)
    await worker.poll_once()
    await broker.dequeue_result(timeout_seconds=1.0)

    await broker.enqueue(work_item)
    await worker.poll_once()
    second_result = await broker.dequeue_result(timeout_seconds=1.0)

    assert second_result is not None
    assert second_result.output == "ok"
    assert executions == 1
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_lock_prevents_simultaneous_duplicate_execution(
    redis_url: str,
    redis_queue_name: str,
) -> None:
    broker_one = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    broker_two = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    worker_one = DistributedWorker(broker=broker_one)
    worker_two = DistributedWorker(broker=broker_two)
    executions = 0
    allow_finish = asyncio.Event()

    @task(name=f"worker_lock_{redis_queue_name}")
    async def guarded() -> str:
        nonlocal executions
        executions += 1
        await allow_finish.wait()
        return "guarded"

    idempotency_key = generate_idempotency_key(
        dag_run_id="run-worker-4",
        node_id="guarded:0",
        task_identity=guarded.identity,
        args=(),
        kwargs={},
    )
    work_item = WorkItem(
        dag_run_id="run-worker-4",
        node_id="guarded:0",
        task_name=guarded.name,
        task_identity=guarded.identity,
        idempotency_key=idempotency_key,
        trace_id="trace-worker-4",
    )

    await broker_one.enqueue(work_item)
    await broker_one.enqueue(work_item)

    first = asyncio.create_task(worker_one.poll_once())
    second = asyncio.create_task(worker_two.poll_once())
    await asyncio.sleep(0.1)
    allow_finish.set()
    await asyncio.gather(first, second)

    follow_up = await worker_two.poll_once()
    assert follow_up is True
    assert executions == 1
    await broker_one.close()
    await broker_two.close()
