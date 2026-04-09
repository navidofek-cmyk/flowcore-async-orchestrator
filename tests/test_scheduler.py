from __future__ import annotations

import asyncio

import pytest

from flowcore.core.dag import DAG
from flowcore.core.execution import ExecutionStatus
from flowcore.core.task import task
from flowcore.exceptions import PermanentTaskExecutionError, TaskExecutionError
from flowcore.reliability.retry import RetryPolicy


@pytest.mark.asyncio
async def test_simple_linear_pipeline() -> None:
    @task
    async def a() -> int:
        return 5

    @task(depends_on=[a])
    async def b(value: int) -> int:
        return value * 2

    async with DAG("linear") as dag:
        dag.add(a)
        dag.add(b)
        results = await dag.run()

    assert results.outputs == {"a:0": 5, "b:1": 10}
    assert results.metadata["b:1"].status is ExecutionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_branching_dag() -> None:
    @task
    async def root() -> int:
        return 2

    @task(depends_on=[root])
    async def left(value: int) -> int:
        return value + 1

    @task(depends_on=[root])
    async def right(value: int) -> int:
        return value + 3

    @task(depends_on=[left, right])
    async def total(a: int, b: int) -> int:
        return a + b

    async with DAG("branching") as dag:
        dag.add(root)
        dag.add(left)
        dag.add(right)
        dag.add(total)
        results = await dag.run()

    assert results.outputs["total:3"] == 8


@pytest.mark.asyncio
async def test_parallel_independent_tasks() -> None:
    started: list[str] = []
    gate = asyncio.Event()
    gate_released = asyncio.Event()

    @task
    async def left() -> str:
        started.append("left")
        if len(started) == 2:
            gate.set()
        await gate_released.wait()
        return "left"

    @task
    async def right() -> str:
        started.append("right")
        if len(started) == 2:
            gate.set()
        await gate_released.wait()
        return "right"

    async with DAG("parallel", max_concurrency=2) as dag:
        dag.add(left)
        dag.add(right)
        run_task = asyncio.create_task(dag.run())
        await asyncio.wait_for(gate.wait(), timeout=1)
        gate_released.set()
        results = await run_task

    assert sorted(started) == ["left", "right"]
    assert results.outputs == {"left:0": "left", "right:1": "right"}


@pytest.mark.asyncio
async def test_dependency_injection_from_upstream_result() -> None:
    @task
    async def a() -> int:
        return 5

    @task(depends_on=[a])
    async def b(value: int) -> int:
        return value * 3

    async with DAG("injection") as dag:
        dag.add(a)
        dag.add(b)
        results = await dag.run()

    assert results.outputs["b:1"] == 15


@pytest.mark.asyncio
async def test_explicit_args_plus_dependency_output_combination() -> None:
    @task
    async def a() -> int:
        return 7

    @task(depends_on=[a])
    async def combine(prefix: str, value: int) -> str:
        return f"{prefix}:{value}"

    async with DAG("args-plus-deps") as dag:
        dag.add(a)
        dag.add(combine("item"))
        results = await dag.run()

    assert results.outputs["combine:1"] == "item:7"


@pytest.mark.asyncio
async def test_upstream_failure_prevents_downstream_execution() -> None:
    executed = False

    @task
    async def broken() -> int:
        raise RuntimeError("boom")

    @task(depends_on=[broken])
    async def never_runs(value: int) -> int:
        nonlocal executed
        executed = True
        return value

    async with DAG("failure") as dag:
        dag.add(broken)
        dag.add(never_runs)
        with pytest.raises(TaskExecutionError):
            await dag.run()

    assert executed is False


@pytest.mark.asyncio
async def test_invalid_dependency_arity_raises_clear_error() -> None:
    @task
    async def left() -> int:
        return 1

    @task
    async def right() -> int:
        return 2

    @task(depends_on=[left, right])
    async def invalid(value: int) -> int:
        return value

    async with DAG("invalid-arity") as dag:
        dag.add(left)
        dag.add(right)
        dag.add(invalid)
        with pytest.raises(PermanentTaskExecutionError):
            await dag.run()


@pytest.mark.asyncio
async def test_task_succeeds_on_third_attempt_with_retries() -> None:
    attempts = 0

    @task(retry=RetryPolicy(max_attempts=3, backoff="fixed", base_delay_seconds=0.0))
    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("not yet")
        return "ok"

    async with DAG("retry-success") as dag:
        dag.add(flaky)
        results = await dag.run()

    assert results.outputs["flaky:0"] == "ok"
    assert results.metadata["flaky:0"].attempt_count == 3
    assert results.metadata["flaky:0"].status is ExecutionStatus.SUCCEEDED
    assert len(results.metadata["flaky:0"].attempts) == 3


@pytest.mark.asyncio
async def test_retries_stop_after_max_attempts_and_record_dlq() -> None:
    @task(retry=RetryPolicy(max_attempts=3, backoff="fixed", base_delay_seconds=0.0))
    async def always_fail() -> int:
        raise RuntimeError("boom")

    async with DAG("retry-fail") as dag:
        dag.add(always_fail)
        with pytest.raises(PermanentTaskExecutionError) as exc_info:
            await dag.run()

    execution_result = exc_info.value.execution_result
    assert execution_result is not None
    assert execution_result.metadata["always_fail:0"].attempt_count == 3
    assert execution_result.metadata["always_fail:0"].status is ExecutionStatus.FAILED
    assert execution_result.dlq_entries[0].task_name == "always_fail"
    assert execution_result.dlq_entries[0].attempt_count == 3


@pytest.mark.asyncio
async def test_downstream_runs_after_upstream_eventually_succeeds() -> None:
    attempts = 0

    @task(retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0))
    async def flaky() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("retry once")
        return 10

    @task(depends_on=[flaky])
    async def dependent(value: int) -> int:
        return value + 5

    async with DAG("retry-upstream-success") as dag:
        dag.add(flaky)
        dag.add(dependent)
        results = await dag.run()

    assert results.outputs["dependent:1"] == 15
    assert results.metadata["flaky:0"].attempt_count == 2
    assert results.metadata["dependent:1"].status is ExecutionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_downstream_is_skipped_after_upstream_permanent_failure() -> None:
    @task(retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0))
    async def broken() -> int:
        raise RuntimeError("boom")

    @task(depends_on=[broken])
    async def dependent(value: int) -> int:
        return value + 1

    async with DAG("retry-upstream-failure") as dag:
        dag.add(broken)
        dag.add(dependent)
        with pytest.raises(PermanentTaskExecutionError) as exc_info:
            await dag.run()

    execution_result = exc_info.value.execution_result
    assert execution_result is not None
    assert execution_result.metadata["broken:0"].status is ExecutionStatus.FAILED
    assert execution_result.metadata["dependent:1"].status is ExecutionStatus.SKIPPED
    assert "failed permanently" in (execution_result.metadata["dependent:1"].error_message or "")


@pytest.mark.asyncio
async def test_successful_task_metadata_contains_duration() -> None:
    @task
    async def simple() -> str:
        return "done"

    async with DAG("metadata-success") as dag:
        dag.add(simple)
        results = await dag.run()

    metadata = results.metadata["simple:0"]
    assert metadata.status is ExecutionStatus.SUCCEEDED
    assert metadata.duration_seconds is not None
    assert metadata.finished_at is not None
