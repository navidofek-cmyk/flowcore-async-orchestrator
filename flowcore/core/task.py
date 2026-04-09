"""Task definition and invocation primitives."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Generic, ParamSpec, TypeVar, cast, overload

from flowcore.exceptions import TaskDefinitionError, TaskRegistryError
from flowcore.reliability.retry import RetryPolicy

P = ParamSpec("P")
R = TypeVar("R")


class TaskRegistry:
    """Explicit registry for resolving task identities in distributed execution."""

    __slots__ = ("_tasks",)

    def __init__(self) -> None:
        self._tasks: dict[str, TaskDefinition[..., object]] = {}

    def register(self, task: TaskDefinition[..., object]) -> None:
        existing = self._tasks.get(task.identity)
        if existing is not None and existing.func is not task.func:
            raise TaskRegistryError(
                f"Duplicate task identity registration detected for {task.identity!r}."
            )
        self._tasks[task.identity] = task

    def get(self, task_identity: str) -> TaskDefinition[..., object]:
        try:
            return self._tasks[task_identity]
        except KeyError as exc:
            raise TaskRegistryError(f"Task identity {task_identity!r} is not registered.") from exc

    def clear(self) -> None:
        self._tasks.clear()


_TASK_REGISTRY = TaskRegistry()


@dataclass(frozen=True, slots=True)
class TaskMetadata:
    """Metadata attached to each task definition."""

    depends_on: tuple[TaskDefinition[..., object], ...] = ()
    retry: RetryPolicy | None = None


@dataclass(frozen=True, slots=True)
class TaskInvocation(Generic[P, R]):
    """A bound task call that has not been executed yet."""

    task: TaskDefinition[P, R]
    args: tuple[object, ...] = ()
    kwargs: Mapping[str, object] = field(default_factory=dict)
    _owner_dag_id: str | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kwargs", MappingProxyType(dict(self.kwargs)))

    @property
    def owner_dag_id(self) -> str | None:
        return self._owner_dag_id

    def assign_owner(self, dag_id: str) -> None:
        object.__setattr__(self, "_owner_dag_id", dag_id)

    def __repr__(self) -> str:
        return (
            f"TaskInvocation(task={self.task.name!r}, args={self.args!r}, "
            f"kwargs={dict(self.kwargs)!r})"
        )


@dataclass(frozen=True, slots=True, eq=False)
class TaskDefinition(Generic[P, R]):
    """A declarative FlowCore task."""

    func: Callable[P, Awaitable[R]]
    metadata: TaskMetadata = field(default_factory=TaskMetadata)
    name: str = ""
    _signature: inspect.Signature = field(init=False, repr=False, compare=False)
    _identity: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not inspect.iscoroutinefunction(self.func):
            raise TaskDefinitionError("FlowCore tasks must wrap async functions.")

        if self.name:
            task_name = self.name
        else:
            task_name = self.func.__name__
            object.__setattr__(self, "name", task_name)

        signature = inspect.signature(self.func)
        object.__setattr__(self, "_signature", signature)
        object.__setattr__(self, "_identity", f"{self.func.__module__}:{self.func.__qualname__}")
        _TASK_REGISTRY.register(cast(TaskDefinition[..., object], self))

    @property
    def signature(self) -> inspect.Signature:
        return self._signature

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def depends_on(self) -> tuple[TaskDefinition[..., object], ...]:
        return self.metadata.depends_on

    @property
    def retry_policy(self) -> RetryPolicy | None:
        return self.metadata.retry

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> TaskInvocation[P, R]:
        return TaskInvocation(task=self, args=tuple(args), kwargs=dict(kwargs))

    def __repr__(self) -> str:
        dependency_names = [dependency.name for dependency in self.depends_on]
        return (
            f"TaskDefinition(name={self.name!r}, identity={self.identity!r}, "
            f"depends_on={dependency_names!r})"
        )

    def __hash__(self) -> int:
        return hash(self.identity)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskDefinition):
            return NotImplemented
        return self.identity == other.identity


def _normalize_dependencies(
    depends_on: Sequence[TaskDefinition[..., object]] | None,
) -> tuple[TaskDefinition[..., object], ...]:
    if depends_on is None:
        return ()

    normalized: list[TaskDefinition[..., object]] = []
    for dependency in depends_on:
        if not isinstance(dependency, TaskDefinition):
            raise TaskDefinitionError("Task dependencies must be FlowCore task definitions.")
        normalized.append(dependency)
    return tuple(normalized)


def _normalize_retry(retry: RetryPolicy | None) -> RetryPolicy | None:
    if retry is None:
        return None
    if not isinstance(retry, RetryPolicy):
        raise TaskDefinitionError("Task retry configuration must be a RetryPolicy.")
    return retry


@overload
def task(func: Callable[P, Awaitable[R]], /) -> TaskDefinition[P, R]: ...


@overload
def task(
    *,
    depends_on: Sequence[TaskDefinition[..., object]] | None = None,
    name: str | None = None,
    retry: RetryPolicy | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], TaskDefinition[P, R]]: ...


def task(
    func: Callable[P, Awaitable[R]] | None = None,
    /,
    *,
    depends_on: Sequence[TaskDefinition[..., object]] | None = None,
    name: str | None = None,
    retry: RetryPolicy | None = None,
) -> TaskDefinition[P, R] | Callable[[Callable[P, Awaitable[R]]], TaskDefinition[P, R]]:
    """Decorate an async callable as a FlowCore task."""

    dependencies = _normalize_dependencies(depends_on)
    retry_policy = _normalize_retry(retry)

    def decorator(inner: Callable[P, Awaitable[R]]) -> TaskDefinition[P, R]:
        return TaskDefinition(
            func=inner,
            metadata=TaskMetadata(depends_on=dependencies, retry=retry_policy),
            name=name or "",
        )

    if func is None:
        return decorator
    return decorator(func)


def get_task_registry() -> TaskRegistry:
    """Expose the global task registry explicitly."""

    return _TASK_REGISTRY
