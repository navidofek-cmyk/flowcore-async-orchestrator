"""Execution result and metadata models."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from flowcore.reliability.dlq import DLQEntry


class ExecutionStatus(StrEnum):
    """Execution lifecycle status for a node."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    RETRYING = "RETRYING"


@dataclass(frozen=True, slots=True)
class AttemptMetadata:
    """One execution attempt for a task node."""

    attempt_number: int
    status: ExecutionStatus
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error_message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "attempt_number": self.attempt_number,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> AttemptMetadata:
        attempt_number = payload.get("attempt_number")
        status = payload.get("status")
        started_at = payload.get("started_at")
        finished_at = payload.get("finished_at")
        duration_seconds = payload.get("duration_seconds")
        error_message = payload.get("error_message")

        if not (
            isinstance(attempt_number, int)
            and isinstance(status, str)
            and isinstance(started_at, str)
            and isinstance(finished_at, str)
            and isinstance(duration_seconds, int | float)
            and (isinstance(error_message, str) or error_message is None)
        ):
            raise ValueError("Attempt metadata payload is invalid.")

        return cls(
            attempt_number=attempt_number,
            status=ExecutionStatus(status),
            started_at=datetime.fromisoformat(started_at),
            finished_at=datetime.fromisoformat(finished_at),
            duration_seconds=float(duration_seconds),
            error_message=error_message,
        )


@dataclass(frozen=True, slots=True)
class NodeExecutionMetadata:
    """Final execution metadata for a task node."""

    node_id: str
    task_name: str
    status: ExecutionStatus
    attempt_count: int
    started_at: datetime | None
    finished_at: datetime | None
    duration_seconds: float | None
    error_message: str | None = None
    trace_id: str | None = None
    attempts: tuple[AttemptMetadata, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "task_name": self.task_name,
            "status": self.status.value,
            "attempt_count": self.attempt_count,
            "started_at": self.started_at.isoformat() if self.started_at is not None else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at is not None else None,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "trace_id": self.trace_id,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> NodeExecutionMetadata:
        node_id = payload.get("node_id")
        task_name = payload.get("task_name")
        status = payload.get("status")
        attempt_count = payload.get("attempt_count")
        started_at = payload.get("started_at")
        finished_at = payload.get("finished_at")
        duration_seconds = payload.get("duration_seconds")
        error_message = payload.get("error_message")
        trace_id = payload.get("trace_id")
        raw_attempts = payload.get("attempts")

        if not (
            isinstance(node_id, str)
            and isinstance(task_name, str)
            and isinstance(status, str)
            and isinstance(attempt_count, int)
            and (isinstance(started_at, str) or started_at is None)
            and (isinstance(finished_at, str) or finished_at is None)
            and (isinstance(duration_seconds, int | float) or duration_seconds is None)
            and (isinstance(error_message, str) or error_message is None)
            and (isinstance(trace_id, str) or trace_id is None)
            and isinstance(raw_attempts, list)
        ):
            raise ValueError("Node execution metadata payload is invalid.")

        return cls(
            node_id=node_id,
            task_name=task_name,
            status=ExecutionStatus(status),
            attempt_count=attempt_count,
            started_at=None if started_at is None else datetime.fromisoformat(started_at),
            finished_at=None if finished_at is None else datetime.fromisoformat(finished_at),
            duration_seconds=None if duration_seconds is None else float(duration_seconds),
            error_message=error_message,
            trace_id=trace_id,
            attempts=tuple(AttemptMetadata.from_dict(attempt) for attempt in raw_attempts),
        )


@dataclass(frozen=True, slots=True, eq=False)
class ExecutionResult(Mapping[str, object]):
    """Structured result returned by ``dag.run()``."""

    outputs: Mapping[str, object]
    metadata: Mapping[str, NodeExecutionMetadata]
    dlq_entries: tuple[DLQEntry, ...] = ()
    trace_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "outputs", MappingProxyType(dict(self.outputs)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def __getitem__(self, key: str) -> object:
        return self.outputs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.outputs)

    def __len__(self) -> int:
        return len(self.outputs)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ExecutionResult):
            return (
                dict(self.outputs) == dict(other.outputs)
                and dict(self.metadata) == dict(other.metadata)
                and self.dlq_entries == other.dlq_entries
            )
        if isinstance(other, Mapping):
            return dict(self.outputs) == dict(other)
        return False

    def to_dict(self) -> dict[str, object]:
        return {
            "outputs": dict(self.outputs),
            "metadata": {node_id: details.to_dict() for node_id, details in self.metadata.items()},
            "dlq": [entry.to_dict() for entry in self.dlq_entries],
            "trace_id": self.trace_id,
        }
