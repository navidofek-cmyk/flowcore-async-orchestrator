"""Dead letter queue primitives for permanently failed tasks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from flowcore.exceptions import SerializationError


@dataclass(frozen=True, slots=True)
class DLQEntry:
    """A permanently failed task recorded for later inspection."""

    node_id: str
    task_name: str
    attempt_count: int
    exception_type: str
    error_message: str
    trace_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "task_name": self.task_name,
            "attempt_count": self.attempt_count,
            "exception_type": self.exception_type,
            "error_message": self.error_message,
            "trace_id": self.trace_id,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DLQEntry:
        node_id = payload.get("node_id")
        task_name = payload.get("task_name")
        attempt_count = payload.get("attempt_count")
        exception_type = payload.get("exception_type")
        error_message = payload.get("error_message")
        trace_id = payload.get("trace_id")
        if not (
            isinstance(node_id, str)
            and isinstance(task_name, str)
            and isinstance(attempt_count, int)
            and isinstance(exception_type, str)
            and isinstance(error_message, str)
            and (isinstance(trace_id, str) or trace_id is None)
        ):
            raise SerializationError("DLQ payload is invalid.")
        return cls(
            node_id=node_id,
            task_name=task_name,
            attempt_count=attempt_count,
            exception_type=exception_type,
            error_message=error_message,
            trace_id=trace_id,
        )


@dataclass(slots=True)
class LocalDeadLetterQueue:
    """In-memory DLQ for local Phase 02 execution."""

    _entries: list[DLQEntry] = field(default_factory=list)
    _queue_name: str = "local"

    def record(self, entry: DLQEntry) -> None:
        self._entries.append(entry)

    @property
    def entries(self) -> tuple[DLQEntry, ...]:
        return tuple(self._entries)
