from __future__ import annotations

import pytest

from flowcore.core.task import TaskDefinition, TaskInvocation, task
from flowcore.exceptions import TaskDefinitionError
from flowcore.reliability.retry import RetryPolicy


@task
async def sample_task(value: int) -> int:
    return value + 1


def test_task_decorator_without_parentheses_preserves_task_type() -> None:
    assert isinstance(sample_task, TaskDefinition)
    assert sample_task.name == "sample_task"


def test_task_decorator_with_parentheses_creates_definition() -> None:
    @task()
    async def decorated(value: str) -> str:
        return value.upper()

    assert isinstance(decorated, TaskDefinition)


def test_task_invocation_creation() -> None:
    invocation = sample_task(3)

    assert isinstance(invocation, TaskInvocation)
    assert invocation.args == (3,)
    assert dict(invocation.kwargs) == {}


def test_dependency_validation_rejects_non_flowcore_tasks() -> None:
    async def plain_dependency() -> int:
        return 1

    with pytest.raises(TaskDefinitionError):
        task(depends_on=[plain_dependency])  # type: ignore[list-item]


def test_sync_function_rejected() -> None:
    with pytest.raises(TaskDefinitionError):

        @task
        def invalid() -> int:
            return 1


def test_task_retry_policy_is_attached() -> None:
    retry_policy = RetryPolicy(max_attempts=3, backoff="exponential", base_delay_seconds=0.1)

    @task(retry=retry_policy)
    async def retried(value: int) -> int:
        return value

    assert retried.retry_policy == retry_policy
