"""Command line interface for FlowCore."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from time import sleep
from typing import cast

import typer

from flowcore.brokers.redis import RedisBroker
from flowcore.core.dag import DAG
from flowcore.core.execution import ExecutionResult, ExecutionStatus, NodeExecutionMetadata
from flowcore.core.task import TaskDefinition, TaskInvocation, task
from flowcore.exceptions import PermanentTaskExecutionError
from flowcore.observability.dashboard import (
    DashboardDataProvider,
    DashboardHTTPServer,
    serve_dashboard_forever,
)
from flowcore.observability.logging import configure_logging
from flowcore.observability.metrics import MetricsHTTPServer, get_metrics
from flowcore.reliability.dlq import DLQEntry
from flowcore.reliability.retry import RetryPolicy
from flowcore.workers.pool import WorkerPool
from flowcore.workers.worker import DistributedWorker

app = typer.Typer(help="FlowCore demo CLI.")
_QUALITY_CHECK_ATTEMPTS: set[str] = set()
_TREE_RIGHT_ATTEMPTS: set[str] = set()


@task(name="fetch_message")
async def _fetch_message() -> str:
    return "flowcore"


@task(
    depends_on=[cast(TaskDefinition[..., object], _fetch_message)],
    name="format_message",
    retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0),
)
async def _format_message(prefix: str, message: str) -> str:
    return f"{prefix}:{message}"


async def _build_demo_dag() -> DAG:
    dag = DAG("demo_pipeline")
    dag.add(cast(TaskDefinition[..., object], _fetch_message))
    dag.add(cast(TaskInvocation[..., object], _format_message("demo")))  # type: ignore[call-arg]
    return dag


@task(name="find_parts")
async def _find_parts(delay_seconds: float) -> list[str]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return ["wheels", "body", "paint"]


@task(depends_on=[cast(TaskDefinition[..., object], _find_parts)], name="build_frame")
async def _build_frame(delay_seconds: float, parts: list[str]) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"frame-from-{len(parts)}-parts"


@task(depends_on=[cast(TaskDefinition[..., object], _build_frame)], name="paint_shell")
async def _paint_shell(color: str, delay_seconds: float, frame: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"{color}-{frame}"


@task(
    depends_on=[cast(TaskDefinition[..., object], _paint_shell)],
    name="quality_check",
    retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0),
)
async def _quality_check(run_token: str, delay_seconds: float, painted_shell: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    if run_token not in _QUALITY_CHECK_ATTEMPTS:
        _QUALITY_CHECK_ATTEMPTS.add(run_token)
        raise RuntimeError("paint not dry yet")
    return f"checked-{painted_shell}"


@task(depends_on=[cast(TaskDefinition[..., object], _quality_check)], name="pack_toy")
async def _pack_toy(delay_seconds: float, checked_toy: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"boxed-{checked_toy}"


@task(depends_on=[cast(TaskDefinition[..., object], _find_parts)], name="mold_wheels")
async def _mold_wheels(delay_seconds: float, parts: list[str]) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"wheels-from-{len(parts)}-parts"


@task(
    depends_on=[
        cast(TaskDefinition[..., object], _build_frame),
        cast(TaskDefinition[..., object], _mold_wheels),
    ],
    name="mount_wheels",
)
async def _mount_wheels(delay_seconds: float, frame: str, wheels: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"{frame}-with-{wheels}"


@task(
    depends_on=[
        cast(TaskDefinition[..., object], _mount_wheels),
        cast(TaskDefinition[..., object], _paint_shell),
    ],
    name="safety_review",
)
async def _safety_review(delay_seconds: float, rolling_chassis: str, painted_shell: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"reviewed-{rolling_chassis}-{painted_shell}"


@task(
    depends_on=[
        cast(TaskDefinition[..., object], _pack_toy),
        cast(TaskDefinition[..., object], _safety_review),
    ],
    name="label_shipment",
)
async def _label_shipment(delay_seconds: float, boxed_toy: str, reviewed_toy: str) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return f"shipment-{boxed_toy}-{reviewed_toy}"


@task(name="scan_root")
async def _scan_root(delay_seconds: float) -> list[str]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return ["left", "right"]


@task(depends_on=[cast(TaskDefinition[..., object], _scan_root)], name="inspect_left_branch")
async def _inspect_left_branch(delay_seconds: float, branches: list[str]) -> dict[str, object]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return {"branch": branches[0], "clues": 2, "risk": 1}


@task(depends_on=[cast(TaskDefinition[..., object], _scan_root)], name="inspect_right_branch")
async def _inspect_right_branch(delay_seconds: float, branches: list[str]) -> dict[str, object]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return {"branch": branches[1], "clues": 4, "risk": 2}


@task(depends_on=[cast(TaskDefinition[..., object], _inspect_left_branch)], name="search_left_leaf")
async def _search_left_leaf(delay_seconds: float, branch_info: dict[str, object]) -> dict[str, object]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return {
        "branch": branch_info["branch"],
        "score": 7,
        "finding": "small treasure chest",
    }


@task(
    depends_on=[cast(TaskDefinition[..., object], _inspect_right_branch)],
    name="search_right_leaf",
    retry=RetryPolicy(max_attempts=2, backoff="fixed", base_delay_seconds=0.0),
)
async def _search_right_leaf(
    run_token: str,
    delay_seconds: float,
    branch_info: dict[str, object],
) -> dict[str, object]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    if run_token not in _TREE_RIGHT_ATTEMPTS:
        _TREE_RIGHT_ATTEMPTS.add(run_token)
        raise RuntimeError("thick fog blocked the right branch")
    return {
        "branch": branch_info["branch"],
        "score": 11,
        "finding": "big glowing crystal",
    }


@task(
    depends_on=[
        cast(TaskDefinition[..., object], _search_left_leaf),
        cast(TaskDefinition[..., object], _search_right_leaf),
    ],
    name="choose_best_path",
)
async def _choose_best_path(
    delay_seconds: float,
    left_result: dict[str, object],
    right_result: dict[str, object],
) -> dict[str, object]:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    best = left_result if int(left_result["score"]) >= int(right_result["score"]) else right_result
    return {
        "winner": best["branch"],
        "score": best["score"],
        "finding": best["finding"],
    }


@task(depends_on=[cast(TaskDefinition[..., object], _choose_best_path)], name="write_story")
async def _write_story(delay_seconds: float, best_path: dict[str, object]) -> str:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    return (
        f"path={best_path['winner']} score={best_path['score']} "
        f"prize={best_path['finding']}"
    )


async def _describe_demo() -> dict[str, object]:
    dag = await _build_demo_dag()
    return dag.describe()


async def _build_factory_demo_dag(
    *,
    color: str,
    delay_scale: float,
    run_token: str,
    variant: str,
) -> DAG:
    dag = DAG("toy_factory_pipeline")
    dag.add(cast(TaskInvocation[..., object], _find_parts(4.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _build_frame(6.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _paint_shell(color, 5.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _quality_check(run_token, 4.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _pack_toy(3.0 * delay_scale)))
    if variant == "extended":
        dag.add(cast(TaskInvocation[..., object], _mold_wheels(4.0 * delay_scale)))
        dag.add(cast(TaskInvocation[..., object], _mount_wheels(5.0 * delay_scale)))
        dag.add(cast(TaskInvocation[..., object], _safety_review(4.0 * delay_scale)))
        dag.add(cast(TaskInvocation[..., object], _label_shipment(3.0 * delay_scale)))
    return dag


async def _run_demo() -> ExecutionResult:
    dag = await _build_demo_dag()
    try:
        return await dag.run()
    except PermanentTaskExecutionError as exc:
        if exc.execution_result is None:
            raise
        return exc.execution_result


async def _run_distributed_demo(
    redis_url: str,
    queue_name: str,
    workers: int,
) -> ExecutionResult:
    dag = await _build_demo_dag()
    pool = WorkerPool(worker_count=workers, redis_url=redis_url, queue_name=queue_name)
    if workers > 0:
        await pool.start()
    try:
        return await dag.run_with_executor(
            executor="distributed",
            redis_url=redis_url,
            queue_name=queue_name,
        )
    except PermanentTaskExecutionError as exc:
        if exc.execution_result is None:
            raise
        return exc.execution_result
    finally:
        if workers > 0:
            await pool.stop()


async def _run_factory_demo(
    *,
    executor: str,
    color: str,
    delay_scale: float,
    variant: str,
    redis_url: str,
    queue_name: str,
    workers: int,
) -> ExecutionResult:
    run_token = f"{color}:{datetime.now(UTC).isoformat()}"
    dag = await _build_factory_demo_dag(
        color=color,
        delay_scale=delay_scale,
        run_token=run_token,
        variant=variant,
    )
    try:
        if executor == "local":
            return await dag.run()

        pool = WorkerPool(worker_count=workers, redis_url=redis_url, queue_name=queue_name)
        if workers > 0:
            await pool.start()
        try:
            return await dag.run_with_executor(
                executor="distributed",
                redis_url=redis_url,
                queue_name=queue_name,
            )
        finally:
            if workers > 0:
                await pool.stop()
    except PermanentTaskExecutionError as exc:
        if exc.execution_result is None:
            raise
        return exc.execution_result


async def _run_assembly_line_demo(
    *,
    executor: str,
    cycles: int,
    pause_seconds: float,
    delay_scale: float,
    variant: str,
    redis_url: str,
    queue_name: str,
    workers: int,
) -> dict[str, object]:
    colors = ("blue", "red", "green", "yellow", "orange", "silver")
    completed_runs: list[dict[str, object]] = []
    pool: WorkerPool | None = None

    if executor == "distributed" and workers > 0:
        pool = WorkerPool(worker_count=workers, redis_url=redis_url, queue_name=queue_name)
        await pool.start()

    try:
        for index in range(cycles):
            color = colors[index % len(colors)]
            run_token = f"assembly-line:{index}:{datetime.now(UTC).isoformat()}"
            dag = await _build_factory_demo_dag(
                color=color,
                delay_scale=delay_scale,
                run_token=run_token,
                variant=variant,
            )
            try:
                if executor == "local":
                    result = await dag.run()
                else:
                    result = await dag.run_with_executor(
                        executor="distributed",
                        redis_url=redis_url,
                        queue_name=queue_name,
                    )
            except PermanentTaskExecutionError as exc:
                if exc.execution_result is None:
                    raise
                result = exc.execution_result

            statuses = {item.status for item in result.metadata.values()}
            if ExecutionStatus.FAILED in statuses:
                overall_status = ExecutionStatus.FAILED.value
            elif ExecutionStatus.RETRYING in statuses:
                overall_status = ExecutionStatus.RETRYING.value
            elif ExecutionStatus.RUNNING in statuses:
                overall_status = ExecutionStatus.RUNNING.value
            else:
                overall_status = ExecutionStatus.SUCCEEDED.value

            completed_runs.append(
                {
                    "cycle": index + 1,
                    "color": color,
                    "status": overall_status,
                    "trace_id": result.trace_id,
                    "outputs": dict(result.outputs),
                }
            )

            if pause_seconds > 0 and index < cycles - 1:
                await asyncio.sleep(pause_seconds)
    finally:
        if pool is not None:
            await pool.stop()

    return {
        "cycles": cycles,
        "delay_scale": delay_scale,
        "executor": executor,
        "pause_seconds": pause_seconds,
        "queue_name": queue_name if executor == "distributed" else None,
        "runs": completed_runs,
        "variant": variant,
    }


async def _build_tree_demo_dag(*, delay_scale: float, run_token: str) -> DAG:
    dag = DAG("tree_search_pipeline")
    dag.add(cast(TaskInvocation[..., object], _scan_root(3.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _inspect_left_branch(4.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _inspect_right_branch(4.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _search_left_leaf(3.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _search_right_leaf(run_token, 3.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _choose_best_path(2.0 * delay_scale)))
    dag.add(cast(TaskInvocation[..., object], _write_story(1.0 * delay_scale)))
    return dag


async def _run_tree_demo(
    *,
    executor: str,
    delay_scale: float,
    redis_url: str,
    queue_name: str,
    workers: int,
) -> ExecutionResult:
    run_token = f"tree-search:{datetime.now(UTC).isoformat()}"
    dag = await _build_tree_demo_dag(delay_scale=delay_scale, run_token=run_token)
    try:
        if executor == "local":
            return await dag.run()

        pool = WorkerPool(worker_count=workers, redis_url=redis_url, queue_name=queue_name)
        if workers > 0:
            await pool.start()
        try:
            return await dag.run_with_executor(
                executor="distributed",
                redis_url=redis_url,
                queue_name=queue_name,
            )
        finally:
            if workers > 0:
                await pool.stop()
    except PermanentTaskExecutionError as exc:
        if exc.execution_result is None:
            raise
        return exc.execution_result


async def _read_status(redis_url: str, queue_name: str) -> dict[str, object]:
    broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
    try:
        return await broker.status()
    finally:
        await broker.close()


async def _read_dlq(redis_url: str, queue_name: str, limit: int) -> list[dict[str, object]]:
    broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
    try:
        return [dict(item) for item in await broker.list_dlq_entries(limit=limit)]
    finally:
        await broker.close()


async def _replay_dlq(redis_url: str, queue_name: str, limit: int) -> int:
    broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
    try:
        return await broker.replay_dlq(limit=limit)
    finally:
        await broker.close()


async def _refresh_broker_metrics(redis_url: str, queue_name: str) -> None:
    broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
    try:
        await broker.status()
    finally:
        await broker.close()


async def _seed_demo_data(
    redis_url: str,
    queue_name: str,
    workers: int,
    runs: int,
    dlq_entries: int,
    *,
    include_failures: bool = True,
) -> dict[str, int]:
    broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
    try:
        now = datetime.now(UTC)

        for index in range(workers):
            await broker.record_worker_heartbeat(
                worker_id=f"demo-worker-{index + 1}",
                status="busy" if index % 2 == 0 else "idle",
                hostname="demo-host",
                pid=4100 + index,
                task_name="format_message" if index % 2 == 0 else None,
            )

        for index in range(runs):
            started_at = now - timedelta(minutes=(index + 1) * 3)
            finished_at = started_at + timedelta(seconds=2 + index)
            root_status = ExecutionStatus.SUCCEEDED
            if include_failures:
                leaf_status = (
                    ExecutionStatus.FAILED
                    if index == 0
                    else (ExecutionStatus.SKIPPED if index == 1 else ExecutionStatus.SUCCEEDED)
                )
            else:
                leaf_status = ExecutionStatus.SUCCEEDED
            metadata = {
                f"fetch_message:{index}": NodeExecutionMetadata(
                    node_id=f"fetch_message:{index}",
                    task_name="fetch_message",
                    status=root_status,
                    attempt_count=1,
                    started_at=started_at,
                    finished_at=started_at + timedelta(seconds=1),
                    duration_seconds=1.0,
                    trace_id=f"demo-trace-{index}",
                ),
                f"format_message:{index}": NodeExecutionMetadata(
                    node_id=f"format_message:{index}",
                    task_name="format_message",
                    status=leaf_status,
                    attempt_count=2 if leaf_status is ExecutionStatus.FAILED else 1,
                    started_at=started_at + timedelta(seconds=1),
                    finished_at=finished_at,
                    duration_seconds=(finished_at - (started_at + timedelta(seconds=1))).total_seconds(),
                    error_message=(
                        "Demo permanent failure"
                        if leaf_status is ExecutionStatus.FAILED
                        else (
                            "Skipped because upstream task failed"
                            if leaf_status is ExecutionStatus.SKIPPED
                            else None
                        )
                    ),
                    trace_id=f"demo-trace-{index}",
                ),
            }
            await broker.record_run_summary(
                ExecutionResult(
                    outputs=(
                        {f"fetch_message:{index}": "flowcore", f"format_message:{index}": "demo:flowcore"}
                        if leaf_status is not ExecutionStatus.FAILED
                        else {f"fetch_message:{index}": "flowcore"}
                    ),
                    metadata=metadata,
                    trace_id=f"demo-trace-{index}",
                ),
                run_id=f"demo-run-{index + 1}",
                dag_name="demo_pipeline",
                status="failed" if leaf_status is ExecutionStatus.FAILED else "succeeded",
                executor="distributed",
            )

        for index in range(dlq_entries):
            if include_failures:
                await broker.record_dlq_entry(
                    DLQEntry(
                        node_id=f"format_message:dlq:{index}",
                        task_name="format_message",
                        attempt_count=2,
                        exception_type="RuntimeError",
                        error_message=f"Demo DLQ failure #{index + 1}",
                        trace_id=f"demo-dlq-trace-{index}",
                    )
                )

        return {
            "workers": workers,
            "runs": runs,
            "dlq_entries": dlq_entries if include_failures else 0,
        }
    finally:
        await broker.close()


async def _run_workers(redis_url: str, queue_name: str, concurrency: int) -> None:
    workers = [
        DistributedWorker(broker=RedisBroker.from_url(redis_url, queue_name=queue_name))
        for _ in range(concurrency)
    ]
    try:
        await asyncio.gather(*(worker.run() for worker in workers))
    finally:
        await asyncio.gather(*(worker.broker.close() for worker in workers))


@app.command()
def describe() -> None:
    """Print the built-in demo DAG as JSON."""

    typer.echo(json.dumps(asyncio.run(_describe_demo()), indent=2, sort_keys=True))


@app.command()
def run(
    executor: str = typer.Option("local", "--executor"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    workers: int = typer.Option(0, "--workers"),
) -> None:
    """Run the built-in demo DAG and print results as JSON."""

    if executor == "local":
        result = asyncio.run(_run_demo())
    elif executor == "distributed":
        result = asyncio.run(_run_distributed_demo(redis_url, queue_name, workers))
    else:
        raise typer.BadParameter("executor must be either 'local' or 'distributed'.")
    typer.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))


@app.command("factory-demo")
def factory_demo(
    executor: str = typer.Option("local", "--executor"),
    color: str = typer.Option("blue", "--color"),
    delay_scale: float = typer.Option(1.0, "--delay-scale", min=0.0),
    variant: str = typer.Option("basic", "--variant"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    workers: int = typer.Option(0, "--workers"),
) -> None:
    """Run a longer toy-factory workflow with visible step timing and one retry."""

    if executor not in {"local", "distributed"}:
        raise typer.BadParameter("executor must be either 'local' or 'distributed'.")
    if variant not in {"basic", "extended"}:
        raise typer.BadParameter("variant must be either 'basic' or 'extended'.")
    result = asyncio.run(
        _run_factory_demo(
            executor=executor,
            color=color,
            delay_scale=delay_scale,
            variant=variant,
            redis_url=redis_url,
            queue_name=queue_name,
            workers=workers,
        )
    )
    typer.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))


@app.command("assembly-line-demo")
def assembly_line_demo(
    executor: str = typer.Option("local", "--executor"),
    cycles: int = typer.Option(3, "--cycles", min=1),
    pause_seconds: float = typer.Option(2.0, "--pause-seconds", min=0.0),
    delay_scale: float = typer.Option(1.0, "--delay-scale", min=0.0),
    variant: str = typer.Option("extended", "--variant"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    workers: int = typer.Option(0, "--workers"),
) -> None:
    """Run several toy-factory cycles in a row so the dashboard stays busy longer."""

    if executor not in {"local", "distributed"}:
        raise typer.BadParameter("executor must be either 'local' or 'distributed'.")
    if variant not in {"basic", "extended"}:
        raise typer.BadParameter("variant must be either 'basic' or 'extended'.")
    payload = asyncio.run(
        _run_assembly_line_demo(
            executor=executor,
            cycles=cycles,
            pause_seconds=pause_seconds,
            delay_scale=delay_scale,
            variant=variant,
            redis_url=redis_url,
            queue_name=queue_name,
            workers=workers,
        )
    )
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@app.command("tree-demo")
def tree_demo(
    executor: str = typer.Option("local", "--executor"),
    delay_scale: float = typer.Option(1.0, "--delay-scale", min=0.0),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    workers: int = typer.Option(0, "--workers"),
) -> None:
    """Run a tree-search story with parallel branches and one visible retry."""

    if executor not in {"local", "distributed"}:
        raise typer.BadParameter("executor must be either 'local' or 'distributed'.")
    result = asyncio.run(
        _run_tree_demo(
            executor=executor,
            delay_scale=delay_scale,
            redis_url=redis_url,
            queue_name=queue_name,
            workers=workers,
        )
    )
    typer.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))


@app.command()
def worker(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    concurrency: int = typer.Option(1, "--concurrency", min=1),
    metrics_host: str = typer.Option("0.0.0.0", "--metrics-host"),
    metrics_port: int | None = typer.Option(None, "--metrics-port"),
) -> None:
    """Start one or more distributed FlowCore workers."""

    server: MetricsHTTPServer | None = None
    if metrics_port is not None:
        server = MetricsHTTPServer(get_metrics(), host=metrics_host, port=metrics_port)
        server.start()
    try:
        asyncio.run(_run_workers(redis_url, queue_name, concurrency))
    finally:
        if server is not None:
            server.stop()


@app.command()
def status(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
) -> None:
    """Show queue state and recent execution summary for Redis-backed mode."""

    typer.echo(
        json.dumps(asyncio.run(_read_status(redis_url, queue_name)), indent=2, sort_keys=True)
    )


@app.command()
def dlq(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    limit: int = typer.Option(20, "--limit", min=1),
) -> None:
    """List Redis-backed distributed DLQ entries."""

    typer.echo(
        json.dumps(asyncio.run(_read_dlq(redis_url, queue_name, limit)), indent=2, sort_keys=True)
    )


@app.command("replay-dlq")
def replay_dlq(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    limit: int = typer.Option(1, "--limit", min=1),
) -> None:
    """Replay DLQ work items by re-enqueueing stored distributed payloads."""

    replayed = asyncio.run(_replay_dlq(redis_url, queue_name, limit))
    typer.echo(
        json.dumps({"queue_name": queue_name, "replayed": replayed}, indent=2, sort_keys=True)
    )


@app.command()
def metrics(
    redis_url: str | None = typer.Option(None, "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    serve: bool = typer.Option(False, "--serve"),
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(9000, "--port"),
    poll_interval_seconds: float = typer.Option(1.0, "--poll-interval-seconds"),
) -> None:
    """Print or serve Prometheus metrics for the current process."""

    if redis_url is not None:
        asyncio.run(_refresh_broker_metrics(redis_url, queue_name))
    if not serve:
        typer.echo(get_metrics().render())
        return

    server = MetricsHTTPServer(get_metrics(), host=host, port=port)
    server.start()
    try:
        while True:
            if redis_url is not None:
                asyncio.run(_refresh_broker_metrics(redis_url, queue_name))
            sleep(poll_interval_seconds)
    except KeyboardInterrupt:
        server.stop()


@app.command()
def dashboard(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(9100, "--port"),
) -> None:
    """Serve a browser dashboard for Redis-backed FlowCore runs."""

    server = DashboardHTTPServer(
        DashboardDataProvider(redis_url=redis_url, queue_name=queue_name),
        host=host,
        port=port,
    )
    serve_dashboard_forever(server)


@app.command("seed-demo")
def seed_demo(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url"),
    queue_name: str = typer.Option("flowcore", "--queue-name"),
    workers: int = typer.Option(3, "--workers", min=1),
    runs: int = typer.Option(4, "--runs", min=1),
    dlq_entries: int = typer.Option(2, "--dlq-entries", min=0),
    happy_path: bool = typer.Option(False, "--happy-path"),
) -> None:
    """Seed Redis with demo workers and runs for the dashboard."""

    typer.echo(
        json.dumps(
            asyncio.run(
                _seed_demo_data(
                    redis_url,
                    queue_name,
                    workers,
                    runs,
                    dlq_entries,
                    include_failures=not happy_path,
                )
            ),
            indent=2,
            sort_keys=True,
        )
    )


def main() -> None:
    """Run the Typer application."""

    configure_logging(logging.INFO)
    app()


if __name__ == "__main__":
    main()
