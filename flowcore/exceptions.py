"""Project-specific exceptions for FlowCore."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowcore.core.execution import ExecutionResult


class FlowCoreError(Exception):
    """Base exception for all FlowCore errors."""


class TaskDefinitionError(FlowCoreError):
    """Raised when a task definition is invalid."""


class TaskInvocationError(FlowCoreError):
    """Raised when a task invocation is invalid."""


class DAGValidationError(FlowCoreError):
    """Raised when DAG validation fails."""


class CycleDetectedError(DAGValidationError):
    """Raised when the DAG contains a cycle."""


class SchedulerError(FlowCoreError):
    """Raised when the scheduler cannot complete execution."""


class TaskExecutionError(SchedulerError):
    """Raised when task execution fails."""

    def __init__(
        self,
        node_id: str,
        task_name: str,
        message: str,
        *,
        attempt_count: int | None = None,
        execution_result: ExecutionResult | None = None,
    ) -> None:
        super().__init__(message)
        self.node_id = node_id
        self.task_name = task_name
        self.attempt_count = attempt_count
        self.execution_result = execution_result


class DependencyResolutionError(SchedulerError):
    """Raised when dependency injection cannot be resolved."""


class InvalidRetryConfigurationError(FlowCoreError):
    """Raised when retry policy configuration is invalid."""


class RetryableTaskExecutionError(TaskExecutionError):
    """Raised for an intermediate task failure that will be retried."""

    def __init__(
        self,
        node_id: str,
        task_name: str,
        message: str,
        *,
        attempt_count: int,
        max_attempts: int,
    ) -> None:
        super().__init__(node_id, task_name, message, attempt_count=attempt_count)
        self.max_attempts = max_attempts


class PermanentTaskExecutionError(TaskExecutionError):
    """Raised when a task has failed permanently."""


class DependencySkippedError(SchedulerError):
    """Raised when a task is skipped because an upstream dependency failed."""


class TaskRegistryError(FlowCoreError):
    """Raised when task registry operations fail."""


class BrokerError(FlowCoreError):
    """Raised when broker operations fail."""


class SerializationError(FlowCoreError):
    """Raised when FlowCore payload serialization fails."""


class WorkerError(FlowCoreError):
    """Raised when a worker cannot process a work item."""


class LockAcquisitionError(FlowCoreError):
    """Raised when distributed lock handling fails."""


class IdempotencyStateError(FlowCoreError):
    """Raised when idempotency state cannot be read or written."""
