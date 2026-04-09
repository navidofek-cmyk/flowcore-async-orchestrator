"""Serialization helpers and typed broker payloads."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from types import MappingProxyType
from typing import cast

from flowcore.core.execution import ExecutionStatus, NodeExecutionMetadata
from flowcore.exceptions import SerializationError
from flowcore.reliability.dlq import DLQEntry

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]

_TYPE_KEY = "__flowcore_type__"


def serialize_value(value: object) -> JSONValue:
    """Convert a Python value into a JSON-safe representation."""

    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, bytes):
        return {
            _TYPE_KEY: "bytes",
            "data": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, tuple):
        return {
            _TYPE_KEY: "tuple",
            "items": [serialize_value(item) for item in value],
        }
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, Mapping):
        serialized: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise SerializationError("Only string keys are supported for JSON serialization.")
            serialized[key] = serialize_value(item)
        return serialized
    raise SerializationError(f"Unsupported value type for serialization: {type(value)!r}")


def deserialize_value(value: JSONValue) -> object:
    """Restore a Python value from a JSON-safe representation."""

    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list):
        return [deserialize_value(item) for item in value]
    type_tag = value.get(_TYPE_KEY)
    if type_tag == "bytes":
        raw_data = value.get("data")
        if not isinstance(raw_data, str):
            raise SerializationError("Serialized bytes payload is invalid.")
        return base64.b64decode(raw_data.encode("ascii"))
    if type_tag == "tuple":
        raw_items = value.get("items")
        if not isinstance(raw_items, list):
            raise SerializationError("Serialized tuple payload is invalid.")
        return tuple(deserialize_value(item) for item in raw_items)
    return {key: deserialize_value(item) for key, item in value.items() if key != _TYPE_KEY}


def dumps_json(value: JSONValue) -> str:
    """Encode a JSON-safe value using deterministic formatting."""

    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def loads_json(value: str) -> JSONValue:
    """Decode a JSON string into a JSON-safe value."""

    decoded = json.loads(value)
    if not isinstance(decoded, dict | list | str | int | float | bool) and decoded is not None:
        raise SerializationError("Decoded JSON payload is not supported.")
    return decoded


@dataclass(frozen=True, slots=True)
class SerializedError:
    """Safe structured error data for transport across the broker."""

    exception_type: str
    error_message: str

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "exception_type": self.exception_type,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, JSONValue]) -> SerializedError:
        exception_type = payload.get("exception_type")
        error_message = payload.get("error_message")
        if not isinstance(exception_type, str) or not isinstance(error_message, str):
            raise SerializationError("Serialized error payload is invalid.")
        return cls(exception_type=exception_type, error_message=error_message)


@dataclass(frozen=True, slots=True)
class WorkItem:
    """A serialized task execution request for distributed workers."""

    dag_run_id: str
    node_id: str
    task_name: str
    task_identity: str
    idempotency_key: str
    trace_id: str
    args: tuple[object, ...] = ()
    kwargs: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kwargs", MappingProxyType(dict(self.kwargs)))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "dag_run_id": self.dag_run_id,
            "node_id": self.node_id,
            "task_name": self.task_name,
            "task_identity": self.task_identity,
            "idempotency_key": self.idempotency_key,
            "trace_id": self.trace_id,
            "args": [serialize_value(value) for value in self.args],
            "kwargs": {key: serialize_value(value) for key, value in self.kwargs.items()},
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, JSONValue]) -> WorkItem:
        dag_run_id = payload.get("dag_run_id")
        node_id = payload.get("node_id")
        task_name = payload.get("task_name")
        task_identity = payload.get("task_identity")
        idempotency_key = payload.get("idempotency_key")
        trace_id = payload.get("trace_id")
        raw_args = payload.get("args")
        raw_kwargs = payload.get("kwargs")

        if not (
            isinstance(dag_run_id, str)
            and isinstance(node_id, str)
            and isinstance(task_name, str)
            and isinstance(task_identity, str)
            and isinstance(idempotency_key, str)
            and isinstance(trace_id, str)
            and isinstance(raw_args, list)
            and isinstance(raw_kwargs, dict)
        ):
            raise SerializationError("Work item payload is invalid.")

        kwargs: dict[str, object] = {}
        for key, value in raw_kwargs.items():
            kwargs[key] = deserialize_value(value)

        return cls(
            dag_run_id=dag_run_id,
            node_id=node_id,
            task_name=task_name,
            task_identity=task_identity,
            idempotency_key=idempotency_key,
            trace_id=trace_id,
            args=tuple(deserialize_value(item) for item in raw_args),
            kwargs=kwargs,
        )


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """A worker-computed node result returned to the scheduler."""

    dag_run_id: str
    node_id: str
    task_name: str
    idempotency_key: str
    metadata: NodeExecutionMetadata
    output: object | None = None
    dlq_entry: DLQEntry | None = None
    error: SerializedError | None = None

    @property
    def succeeded(self) -> bool:
        return self.metadata.status is ExecutionStatus.SUCCEEDED

    def to_dict(self) -> dict[str, JSONValue]:
        metadata_payload = cast(dict[str, JSONValue], self.metadata.to_dict())
        dlq_payload = (
            None if self.dlq_entry is None else cast(dict[str, JSONValue], self.dlq_entry.to_dict())
        )
        return {
            "dag_run_id": self.dag_run_id,
            "node_id": self.node_id,
            "task_name": self.task_name,
            "idempotency_key": self.idempotency_key,
            "metadata": metadata_payload,
            "output": serialize_value(self.output),
            "dlq_entry": dlq_payload,
            "error": None if self.error is None else self.error.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, JSONValue]) -> WorkerResult:
        dag_run_id = payload.get("dag_run_id")
        node_id = payload.get("node_id")
        task_name = payload.get("task_name")
        idempotency_key = payload.get("idempotency_key")
        raw_metadata = payload.get("metadata")
        raw_output = payload.get("output")
        raw_dlq = payload.get("dlq_entry")
        raw_error = payload.get("error")

        if not (
            isinstance(dag_run_id, str)
            and isinstance(node_id, str)
            and isinstance(task_name, str)
            and isinstance(idempotency_key, str)
            and isinstance(raw_metadata, dict)
        ):
            raise SerializationError("Worker result payload is invalid.")

        dlq_entry = None
        if isinstance(raw_dlq, dict):
            dlq_entry = DLQEntry.from_dict(raw_dlq)
        elif raw_dlq is not None:
            raise SerializationError("Worker result DLQ payload is invalid.")

        error = None
        if isinstance(raw_error, dict):
            error = SerializedError.from_dict(raw_error)
        elif raw_error is not None:
            raise SerializationError("Worker result error payload is invalid.")

        return cls(
            dag_run_id=dag_run_id,
            node_id=node_id,
            task_name=task_name,
            idempotency_key=idempotency_key,
            metadata=NodeExecutionMetadata.from_dict(raw_metadata),
            output=deserialize_value(raw_output),
            dlq_entry=dlq_entry,
            error=error,
        )


def stable_payload_hash(
    *,
    dag_run_id: str,
    node_id: str,
    task_identity: str,
    args: tuple[object, ...],
    kwargs: Mapping[str, object],
) -> str:
    """Generate a deterministic idempotency hash for a work item."""

    payload: JSONValue = {
        "dag_run_id": dag_run_id,
        "node_id": node_id,
        "task_identity": task_identity,
        "args": [serialize_value(value) for value in args],
        "kwargs": {key: serialize_value(value) for key, value in kwargs.items()},
    }
    return sha256(dumps_json(payload).encode("utf-8")).hexdigest()
