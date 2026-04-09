from __future__ import annotations

import pytest

from flowcore.core.dag import DAG
from flowcore.core.task import task
from flowcore.exceptions import CycleDetectedError, DAGValidationError


@task
async def source() -> int:
    return 1


@task(depends_on=[source])
async def dependent(value: int) -> int:
    return value + 1


@task
async def extra() -> int:
    return 5


@task(depends_on=[source, extra])
async def combine(left: int, right: int) -> int:
    return left + right


@pytest.mark.asyncio
async def test_add_task_invocation() -> None:
    async with DAG("invocation") as dag:
        node_id = dag.add(source())

    assert node_id == "source:0"


@pytest.mark.asyncio
async def test_add_task_definition() -> None:
    async with DAG("definition") as dag:
        dag.add(source)
        dag.add(dependent)
        description = dag.describe()

    assert [node["task_name"] for node in description["nodes"]] == ["source", "dependent"]


@pytest.mark.asyncio
async def test_cycle_detection() -> None:
    @task()
    async def first() -> int:
        return 1

    @task(depends_on=[first])
    async def second(value: int) -> int:
        return value + 1

    object.__setattr__(first.metadata, "depends_on", (second,))

    async with DAG("cyclic") as dag:
        dag.add(first)
        dag.add(second)
        with pytest.raises(CycleDetectedError):
            dag.describe()


@pytest.mark.asyncio
async def test_deterministic_topological_order() -> None:
    async with DAG("ordering") as dag:
        dag.add(source)
        dag.add(extra)
        dag.add(combine)
        description = dag.describe()

    assert [node["task_name"] for node in description["nodes"]] == ["source", "extra", "combine"]


@pytest.mark.asyncio
async def test_duplicate_definition_registration_is_rejected() -> None:
    async with DAG("duplicates") as dag:
        dag.add(source)
        with pytest.raises(DAGValidationError):
            dag.add(source)


@pytest.mark.asyncio
async def test_describe_output_shape() -> None:
    async with DAG("describe") as dag:
        dag.add(source)
        dag.add(dependent)
        description = dag.describe()

    assert description["name"] == "describe"
    assert description["executor"] == "local"
    assert isinstance(description["nodes"], list)
    assert description["nodes"][1]["dependencies"] == ["source:0"]
