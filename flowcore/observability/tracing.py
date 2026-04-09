"""Lightweight trace context propagation for FlowCore."""

from __future__ import annotations

import contextvars
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import Token

_TRACE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "flowcore_trace_id",
    default=None,
)
_NODE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "flowcore_node_id",
    default=None,
)
_TASK_NAME: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "flowcore_task_name",
    default=None,
)
_EXECUTOR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "flowcore_executor",
    default=None,
)


def new_trace_id() -> str:
    """Create a new trace identifier for one DAG run."""

    return uuid.uuid4().hex


def get_current_trace_id() -> str | None:
    """Read the currently bound trace identifier."""

    return _TRACE_ID.get()


def ensure_trace_id() -> str:
    """Return the current trace id, creating one when missing."""

    trace_id = get_current_trace_id()
    if trace_id is None:
        trace_id = new_trace_id()
        _TRACE_ID.set(trace_id)
    return trace_id


def current_context() -> dict[str, str]:
    """Return the current execution context for logging."""

    context: dict[str, str] = {}
    trace_id = _TRACE_ID.get()
    node_id = _NODE_ID.get()
    task_name = _TASK_NAME.get()
    executor = _EXECUTOR.get()
    if trace_id is not None:
        context["trace_id"] = trace_id
    if node_id is not None:
        context["node_id"] = node_id
    if task_name is not None:
        context["task_name"] = task_name
    if executor is not None:
        context["executor"] = executor
    return context


@contextmanager
def trace_context(trace_id: str | None = None) -> Iterator[str]:
    """Bind a trace id for the duration of a logical run."""

    resolved_trace_id = trace_id or new_trace_id()
    token = _TRACE_ID.set(resolved_trace_id)
    try:
        yield resolved_trace_id
    finally:
        _TRACE_ID.reset(token)


@contextmanager
def bind_execution_context(
    *,
    trace_id: str | None = None,
    node_id: str | None = None,
    task_name: str | None = None,
    executor: str | None = None,
) -> Iterator[None]:
    """Bind execution context fields using context variables."""

    tokens: list[tuple[contextvars.ContextVar[str | None], Token[str | None]]] = []
    if trace_id is not None:
        tokens.append((_TRACE_ID, _TRACE_ID.set(trace_id)))
    if node_id is not None:
        tokens.append((_NODE_ID, _NODE_ID.set(node_id)))
    if task_name is not None:
        tokens.append((_TASK_NAME, _TASK_NAME.set(task_name)))
    if executor is not None:
        tokens.append((_EXECUTOR, _EXECUTOR.set(executor)))
    try:
        yield None
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)
