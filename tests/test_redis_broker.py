from __future__ import annotations

import pytest

from flowcore.brokers.redis import RedisBroker
from flowcore.core.execution import ExecutionResult, ExecutionStatus, NodeExecutionMetadata
from flowcore.core.serialization import WorkerResult, WorkItem


@pytest.mark.asyncio
@pytest.mark.integration
async def test_enqueue_dequeue_roundtrip(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    work_item = WorkItem(
        dag_run_id="run-1",
        node_id="node-1",
        task_name="task",
        task_identity="tests:task",
        idempotency_key="idemp-1",
        trace_id="trace-1",
        args=("value",),
        kwargs={},
    )

    await broker.enqueue(work_item)
    message = await broker.dequeue(timeout_seconds=1.0)

    assert message is not None
    assert message.payload == work_item
    await broker.ack(message)
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ack_nack_and_queue_depth(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    work_item = WorkItem(
        dag_run_id="run-2",
        node_id="node-2",
        task_name="task",
        task_identity="tests:task",
        idempotency_key="idemp-2",
        trace_id="trace-2",
    )

    await broker.enqueue(work_item)
    assert await broker.depth() == 1

    message = await broker.dequeue(timeout_seconds=1.0)
    assert message is not None
    assert await broker.depth() == 0

    await broker.nack(message)
    assert await broker.depth() == 1

    message = await broker.dequeue(timeout_seconds=1.0)
    assert message is not None
    await broker.ack(message)
    assert await broker.depth() == 0
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_result_publication_roundtrip(redis_url: str, redis_queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    result = WorkerResult(
        dag_run_id="run-3",
        node_id="node-3",
        task_name="task",
        idempotency_key="idemp-3",
        metadata=NodeExecutionMetadata(
            node_id="node-3",
            task_name="task",
            status=ExecutionStatus.SUCCEEDED,
            attempt_count=1,
            started_at=None,
            finished_at=None,
            duration_seconds=None,
            error_message=None,
        ),
        output={"value": "ok"},
    )

    await broker.publish_result(result)
    dequeued = await broker.dequeue_result(timeout_seconds=1.0)

    assert dequeued == result
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_summary_roundtrip_includes_dashboard_fields(
    redis_url: str,
    redis_queue_name: str,
) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
    metadata = NodeExecutionMetadata(
            node_id="node-dashboard",
            task_name="task",
            status=ExecutionStatus.SUCCEEDED,
            attempt_count=1,
            started_at=None,
            finished_at=None,
            duration_seconds=None,
            error_message=None,
            trace_id="trace-dashboard",
        )

    await broker.record_run_summary(
        result=ExecutionResult(
            outputs={"node-dashboard": {"value": "ok"}},
            metadata={"node-dashboard": metadata},
            trace_id="trace-dashboard",
        ),
        run_id="run-dashboard",
        dag_name="dashboard-demo",
        status="succeeded",
        executor="distributed",
    )

    summary = await broker.get_run_summary("run-dashboard")

    assert summary is not None
    assert summary["executor"] == "distributed"
    assert summary["run_id"] == "run-dashboard"
    assert summary["nodes"][0]["task_name"] == "task"
    assert summary["outputs"]["node-dashboard"]["value"] == "ok"
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_worker_heartbeat_is_reported_in_status(
    redis_url: str,
    redis_queue_name: str,
) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)

    await broker.record_worker_heartbeat(
        worker_id="worker-1",
        status="idle",
        hostname="n1",
        pid=1234,
        task_name=None,
    )
    status = await broker.status()

    assert status["workers"][0]["worker_id"] == "worker-1"
    assert status["workers"][0]["hostname"] == "n1"
    await broker.close()
