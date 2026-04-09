from __future__ import annotations

import asyncio

import pytest

from flowcore.brokers.redis import RedisBroker
from flowcore.core.dag import DAG
from flowcore.core.execution import ExecutionStatus
from flowcore.core.task import task
from flowcore.exceptions import PermanentTaskExecutionError
from flowcore.reliability.retry import RetryPolicy
from flowcore.workers.worker import DistributedWorker


async def _start_workers(
    redis_url: str,
    queue_name: str,
    *,
    count: int = 1,
) -> tuple[asyncio.Event, list[asyncio.Task[None]], list[RedisBroker]]:
    stop_event = asyncio.Event()
    brokers = [RedisBroker.from_url(redis_url, queue_name=queue_name) for _ in range(count)]
    tasks = [
        asyncio.create_task(DistributedWorker(broker=broker).run(stop_event)) for broker in brokers
    ]
    await asyncio.sleep(0.1)
    return stop_event, tasks, brokers


async def _stop_workers(
    stop_event: asyncio.Event,
    tasks: list[asyncio.Task[None]],
    brokers: list[RedisBroker],
) -> None:
    stop_event.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
    for broker in brokers:
        await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_distributed_linear_pipeline(redis_url: str, redis_queue_name: str) -> None:
    stop_event, worker_tasks, brokers = await _start_workers(redis_url, redis_queue_name)

    @task(name=f"distributed_source_{redis_queue_name}")
    async def source() -> int:
        return 3

    @task(depends_on=[source], name=f"distributed_consumer_{redis_queue_name}")
    async def consumer(value: int) -> int:
        return value * 4

    async with DAG("distributed-linear") as dag:
        dag.add(source)
        dag.add(consumer)
        result = await dag.run_with_executor(
            executor="distributed",
            redis_url=redis_url,
            queue_name=redis_queue_name,
        )

    assert result.outputs["distributed_consumer_" + redis_queue_name + ":1"] == 12
    await _stop_workers(stop_event, worker_tasks, brokers)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_distributed_branching_pipeline(redis_url: str, redis_queue_name: str) -> None:
    stop_event, worker_tasks, brokers = await _start_workers(
        redis_url,
        redis_queue_name,
        count=2,
    )

    @task(name=f"dist_root_{redis_queue_name}")
    async def root() -> int:
        return 2

    @task(depends_on=[root], name=f"dist_left_{redis_queue_name}")
    async def left(value: int) -> int:
        return value + 1

    @task(depends_on=[root], name=f"dist_right_{redis_queue_name}")
    async def right(value: int) -> int:
        return value + 2

    @task(depends_on=[left, right], name=f"dist_total_{redis_queue_name}")
    async def total(a: int, b: int) -> int:
        return a + b

    async with DAG("distributed-branch") as dag:
        dag.add(root)
        dag.add(left)
        dag.add(right)
        dag.add(total)
        result = await dag.run_with_executor(
            executor="distributed",
            redis_url=redis_url,
            queue_name=redis_queue_name,
        )

    assert result.outputs["dist_total_" + redis_queue_name + ":3"] == 7
    await _stop_workers(stop_event, worker_tasks, brokers)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_distributed_retry_and_dlq_behavior(redis_url: str, redis_queue_name: str) -> None:
    stop_event, worker_tasks, brokers = await _start_workers(redis_url, redis_queue_name)

    @task(
        name=f"dist_fail_{redis_queue_name}",
        retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0),
    )
    async def fail() -> int:
        raise RuntimeError("boom")

    @task(depends_on=[fail], name=f"dist_never_{redis_queue_name}")
    async def never(value: int) -> int:
        return value

    async with DAG("distributed-failure") as dag:
        dag.add(fail)
        dag.add(never)
        with pytest.raises(PermanentTaskExecutionError) as exc_info:
            await dag.run_with_executor(
                executor="distributed",
                redis_url=redis_url,
                queue_name=redis_queue_name,
            )

    execution_result = exc_info.value.execution_result
    assert execution_result is not None
    assert execution_result.metadata["dist_fail_" + redis_queue_name + ":0"].attempt_count == 2
    assert (
        execution_result.metadata["dist_never_" + redis_queue_name + ":1"].status
        is ExecutionStatus.SKIPPED
    )
    assert len(execution_result.dlq_entries) == 1
    await _stop_workers(stop_event, worker_tasks, brokers)
