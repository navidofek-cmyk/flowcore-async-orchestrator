from __future__ import annotations

import asyncio
import json

import pytest
from typer.testing import CliRunner

from flowcore.brokers.redis import RedisBroker
from flowcore.cli import app
from flowcore.core.execution import ExecutionResult, ExecutionStatus, NodeExecutionMetadata
from flowcore.core.serialization import WorkItem
from flowcore.reliability.dlq import DLQEntry

runner = CliRunner()


def test_describe_command_works() -> None:
    result = runner.invoke(app, ["describe"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["name"] == "demo_pipeline"
    assert len(payload["nodes"]) == 2


def test_run_command_works() -> None:
    result = runner.invoke(app, ["run", "--executor", "local"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["fetch_message:0"] == "flowcore"
    assert payload["outputs"]["format_message:1"] == "demo:flowcore"
    assert payload["metadata"]["fetch_message:0"]["status"] == "SUCCEEDED"
    assert payload["dlq"] == []


def test_factory_demo_command_works() -> None:
    result = runner.invoke(app, ["factory-demo", "--delay-scale", "0"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["pack_toy:4"] == "boxed-checked-blue-frame-from-3-parts"
    assert payload["metadata"]["quality_check:3"]["attempt_count"] == 2
    assert payload["metadata"]["quality_check:3"]["status"] == "SUCCEEDED"


def test_factory_demo_extended_command_works() -> None:
    result = runner.invoke(
        app,
        ["factory-demo", "--delay-scale", "0", "--variant", "extended"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["label_shipment:8"] == (
        "shipment-boxed-checked-blue-frame-from-3-parts-"
        "reviewed-frame-from-3-parts-with-wheels-from-3-parts-blue-frame-from-3-parts"
    )
    assert payload["metadata"]["quality_check:3"]["attempt_count"] == 2
    assert payload["metadata"]["label_shipment:8"]["status"] == "SUCCEEDED"


def test_assembly_line_demo_command_works() -> None:
    result = runner.invoke(
        app,
        [
            "assembly-line-demo",
            "--cycles",
            "2",
            "--delay-scale",
            "0",
            "--pause-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["cycles"] == 2
    assert payload["variant"] == "extended"
    assert len(payload["runs"]) == 2
    assert payload["runs"][0]["color"] == "blue"
    assert payload["runs"][1]["color"] == "red"
    assert payload["runs"][0]["status"] == "SUCCEEDED"


def test_assembly_line_demo_help_works() -> None:
    result = runner.invoke(app, ["assembly-line-demo", "--help"])

    assert result.exit_code == 0
    assert "dashboard stays busy longer" in result.stdout


def test_tree_demo_command_works() -> None:
    result = runner.invoke(app, ["tree-demo", "--delay-scale", "0"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["write_story:6"] == "path=right score=11 prize=big glowing crystal"
    assert payload["metadata"]["search_right_leaf:4"]["attempt_count"] == 2
    assert payload["metadata"]["choose_best_path:5"]["status"] == "SUCCEEDED"


def test_tree_demo_help_works() -> None:
    result = runner.invoke(app, ["tree-demo", "--help"])

    assert result.exit_code == 0
    assert "tree-search story" in result.stdout


def test_worker_help_works() -> None:
    result = runner.invoke(app, ["worker", "--help"])

    assert result.exit_code == 0
    assert "Start one or more distributed FlowCore workers" in result.stdout


def test_dashboard_help_works() -> None:
    result = runner.invoke(app, ["dashboard", "--help"])

    assert result.exit_code == 0
    assert "Serve a browser dashboard for Redis-backed FlowCore runs" in result.stdout


def test_seed_demo_help_works() -> None:
    result = runner.invoke(app, ["seed-demo", "--help"])

    assert result.exit_code == 0
    assert "Seed Redis with demo workers and runs for the dashboard" in result.stdout


def test_factory_demo_help_works() -> None:
    result = runner.invoke(app, ["factory-demo", "--help"])

    assert result.exit_code == 0
    assert "Run a longer toy-factory workflow with visible step timing and one retry" in result.stdout


def test_metrics_command_works() -> None:
    result = runner.invoke(app, ["metrics"])

    assert result.exit_code == 0
    assert "flowcore_task_execution_total" in result.stdout


@pytest.mark.integration
def test_run_distributed_command_works(redis_url: str, redis_queue_name: str) -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--executor",
            "distributed",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["fetch_message:0"] == "flowcore"


@pytest.mark.integration
def test_factory_demo_distributed_command_works(redis_url: str, redis_queue_name: str) -> None:
    result = runner.invoke(
        app,
        [
            "factory-demo",
            "--executor",
            "distributed",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "1",
            "--delay-scale",
            "0",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["pack_toy:4"] == "boxed-checked-blue-frame-from-3-parts"
    assert payload["metadata"]["quality_check:3"]["attempt_count"] == 2


@pytest.mark.integration
def test_factory_demo_extended_distributed_command_works(
    redis_url: str, redis_queue_name: str
) -> None:
    result = runner.invoke(
        app,
        [
            "factory-demo",
            "--executor",
            "distributed",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "1",
            "--delay-scale",
            "0",
            "--variant",
            "extended",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["label_shipment:8"] == (
        "shipment-boxed-checked-blue-frame-from-3-parts-"
        "reviewed-frame-from-3-parts-with-wheels-from-3-parts-blue-frame-from-3-parts"
    )
    assert payload["metadata"]["quality_check:3"]["attempt_count"] == 2
    assert payload["metadata"]["label_shipment:8"]["status"] == "SUCCEEDED"


@pytest.mark.integration
def test_assembly_line_demo_distributed_command_works(
    redis_url: str, redis_queue_name: str
) -> None:
    result = runner.invoke(
        app,
        [
            "assembly-line-demo",
            "--executor",
            "distributed",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "1",
            "--cycles",
            "2",
            "--delay-scale",
            "0",
            "--pause-seconds",
            "0",
            "--variant",
            "basic",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["executor"] == "distributed"
    assert payload["queue_name"] == redis_queue_name
    assert len(payload["runs"]) == 2
    assert payload["runs"][0]["status"] == "SUCCEEDED"


@pytest.mark.integration
def test_tree_demo_distributed_command_works(redis_url: str, redis_queue_name: str) -> None:
    result = runner.invoke(
        app,
        [
            "tree-demo",
            "--executor",
            "distributed",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "1",
            "--delay-scale",
            "0",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["outputs"]["write_story:6"] == "path=right score=11 prize=big glowing crystal"
    assert payload["metadata"]["search_right_leaf:4"]["attempt_count"] == 2


@pytest.mark.integration
def test_status_command_works(redis_url: str, redis_queue_name: str) -> None:
    async def seed() -> None:
        broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
        try:
            await broker.record_run_summary(
                ExecutionResult(
                    outputs={},
                    metadata={
                        "node:0": NodeExecutionMetadata(
                            node_id="node:0",
                            task_name="task",
                            status=ExecutionStatus.SUCCEEDED,
                            attempt_count=1,
                            started_at=None,
                            finished_at=None,
                            duration_seconds=None,
                            trace_id="trace-status",
                        )
                    },
                    trace_id="trace-status",
                ),
                run_id="run-status",
                dag_name="demo",
                status="succeeded",
                executor="distributed",
            )
        finally:
            await broker.close()

    asyncio.run(seed())
    result = runner.invoke(
        app,
        ["status", "--redis-url", redis_url, "--queue-name", redis_queue_name],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["queue_name"] == redis_queue_name
    assert payload["recent_runs"][0]["run_id"] == "run-status"
    assert payload["recent_runs"][0]["trace_id"] == "trace-status"


@pytest.mark.integration
def test_dlq_and_replay_dlq_commands_work(redis_url: str, redis_queue_name: str) -> None:
    async def seed() -> None:
        broker = RedisBroker.from_url(redis_url, queue_name=redis_queue_name)
        try:
            work_item = WorkItem(
                dag_run_id="run-cli-dlq",
                node_id="node:0",
                task_name="task",
                task_identity="tests:task",
                idempotency_key="idemp-cli-dlq",
                trace_id="trace-cli-dlq",
            )
            await broker.record_dlq_entry(
                DLQEntry(
                    node_id="node:0",
                    task_name="task",
                    attempt_count=1,
                    exception_type="RuntimeError",
                    error_message="boom",
                    trace_id="trace-cli-dlq",
                ),
                work_item=work_item,
            )
        finally:
            await broker.close()

    asyncio.run(seed())

    dlq_result = runner.invoke(
        app,
        ["dlq", "--redis-url", redis_url, "--queue-name", redis_queue_name],
    )
    assert dlq_result.exit_code == 0
    dlq_payload = json.loads(dlq_result.stdout)
    assert dlq_payload[0]["entry"]["trace_id"] == "trace-cli-dlq"

    replay_result = runner.invoke(
        app,
        ["replay-dlq", "--redis-url", redis_url, "--queue-name", redis_queue_name],
    )
    assert replay_result.exit_code == 0
    replay_payload = json.loads(replay_result.stdout)
    assert replay_payload["replayed"] == 1


@pytest.mark.integration
def test_seed_demo_command_populates_dashboard_data(redis_url: str, redis_queue_name: str) -> None:
    result = runner.invoke(
        app,
        [
            "seed-demo",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "2",
            "--runs",
            "2",
            "--dlq-entries",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {"dlq_entries": 1, "runs": 2, "workers": 2}

    status_result = runner.invoke(
        app,
        ["status", "--redis-url", redis_url, "--queue-name", redis_queue_name],
    )
    status_payload = json.loads(status_result.stdout)
    assert len(status_payload["workers"]) == 2
    assert len(status_payload["recent_runs"]) == 2


@pytest.mark.integration
def test_seed_demo_happy_path_avoids_failures(redis_url: str, redis_queue_name: str) -> None:
    result = runner.invoke(
        app,
        [
            "seed-demo",
            "--redis-url",
            redis_url,
            "--queue-name",
            redis_queue_name,
            "--workers",
            "2",
            "--runs",
            "2",
            "--dlq-entries",
            "2",
            "--happy-path",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {"dlq_entries": 0, "runs": 2, "workers": 2}

    status_result = runner.invoke(
        app,
        ["status", "--redis-url", redis_url, "--queue-name", redis_queue_name],
    )
    status_payload = json.loads(status_result.stdout)
    assert status_payload["dlq_depth"] == 0
    assert all(run["failed"] == 0 for run in status_payload["recent_runs"])
