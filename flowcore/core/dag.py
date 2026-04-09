"""DAG registration, validation, and graph description."""

from __future__ import annotations

import heapq
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Literal

from flowcore.brokers.redis import RedisBroker
from flowcore.core.execution import ExecutionResult
from flowcore.core.task import TaskDefinition, TaskInvocation
from flowcore.exceptions import CycleDetectedError, DAGValidationError, DependencyResolutionError


@dataclass(frozen=True, slots=True)
class RegisteredNode:
    """A resolved executable node within a DAG."""

    node_id: str
    order: int
    invocation: TaskInvocation[..., object]
    dependency_node_ids: tuple[str, ...]

    @property
    def task(self) -> TaskDefinition[..., object]:
        return self.invocation.task


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """Validated execution plan emitted by the DAG."""

    dag_name: str
    executor: str
    max_concurrency: int
    nodes: tuple[RegisteredNode, ...]


@dataclass(frozen=True, slots=True)
class _PendingNode:
    node_id: str
    order: int
    invocation: TaskInvocation[..., object]

    @property
    def task(self) -> TaskDefinition[..., object]:
        return self.invocation.task


class DAG:
    """A single-process DAG composed of FlowCore tasks."""

    __slots__ = (
        "_dag_id",
        "_implicit_definitions",
        "_invocation_ids",
        "_max_concurrency",
        "_name",
        "_nodes",
        "_running",
    )

    def __init__(self, name: str, *, max_concurrency: int = 10) -> None:
        if max_concurrency < 1:
            raise DAGValidationError("max_concurrency must be at least 1.")
        self._dag_id = str(uuid.uuid4())
        self._name = name
        self._max_concurrency = max_concurrency
        self._nodes: list[_PendingNode] = []
        self._running = False
        self._implicit_definitions: set[TaskDefinition[..., object]] = set()
        self._invocation_ids: set[int] = set()

    async def __aenter__(self) -> DAG:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    @property
    def name(self) -> str:
        return self._name

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    def add(self, item: TaskDefinition[..., object] | TaskInvocation[..., object]) -> str:
        if self._running:
            raise DAGValidationError("Cannot add tasks after execution has started.")

        invocation = self._coerce_invocation(item)
        self._validate_invocation_ownership(invocation)

        if isinstance(item, TaskDefinition) and item in self._implicit_definitions:
            raise DAGValidationError(
                f"Task definition {item.name!r} has already been added implicitly to this DAG."
            )

        invocation_id = id(invocation)
        if invocation_id in self._invocation_ids:
            raise DAGValidationError("The same task invocation cannot be added twice.")

        node_id = f"{invocation.task.name}:{len(self._nodes)}"
        pending_node = _PendingNode(node_id=node_id, order=len(self._nodes), invocation=invocation)
        self._nodes.append(pending_node)
        self._invocation_ids.add(invocation_id)

        if isinstance(item, TaskDefinition):
            self._implicit_definitions.add(item)

        return node_id

    def describe(self) -> dict[str, object]:
        plan = self._build_execution_plan()
        return {
            "name": plan.dag_name,
            "executor": plan.executor,
            "max_concurrency": plan.max_concurrency,
            "nodes": [
                {
                    "node_id": node.node_id,
                    "task_name": node.task.name,
                    "task_identity": node.task.identity,
                    "args": list(node.invocation.args),
                    "kwargs": dict(node.invocation.kwargs),
                    "dependencies": list(node.dependency_node_ids),
                }
                for node in plan.nodes
            ],
        }

    async def run(self) -> ExecutionResult:
        return await self.run_with_executor()

    async def run_with_executor(
        self,
        *,
        executor: Literal["local", "distributed"] = "local",
        redis_url: str | None = None,
        queue_name: str = "flowcore",
    ) -> ExecutionResult:
        from flowcore.core.scheduler import DistributedScheduler, LocalScheduler

        self._running = True
        try:
            if executor == "local":
                scheduler = LocalScheduler(max_concurrency=self._max_concurrency)
                return await scheduler.run(self._build_execution_plan(executor=executor))
            if redis_url is None:
                raise DAGValidationError("redis_url is required for distributed execution.")
            broker = RedisBroker.from_url(redis_url, queue_name=queue_name)
            scheduler = DistributedScheduler(
                broker=broker,
                max_concurrency=self._max_concurrency,
            )
            return await scheduler.run(self._build_execution_plan(executor=executor))
        finally:
            self._running = False

    def _build_execution_plan(
        self,
        *,
        executor: Literal["local", "distributed"] = "local",
    ) -> ExecutionPlan:
        dependency_index = self._index_nodes_by_task()
        resolved_dependencies = self._resolve_dependencies(dependency_index)
        ordered_nodes = self._topological_sort(resolved_dependencies)
        return ExecutionPlan(
            dag_name=self._name,
            executor=executor,
            max_concurrency=self._max_concurrency,
            nodes=ordered_nodes,
        )

    def _coerce_invocation(
        self, item: TaskDefinition[..., object] | TaskInvocation[..., object]
    ) -> TaskInvocation[..., object]:
        if isinstance(item, TaskDefinition):
            return item()
        if isinstance(item, TaskInvocation):
            return item
        raise DAGValidationError("Only task definitions or task invocations can be added to a DAG.")

    def _validate_invocation_ownership(self, invocation: TaskInvocation[..., object]) -> None:
        owner = invocation.owner_dag_id
        if owner is not None and owner != self._dag_id:
            raise DAGValidationError("Task invocations cannot be shared across DAG instances.")
        if owner is None:
            invocation.assign_owner(self._dag_id)

    def _index_nodes_by_task(self) -> dict[TaskDefinition[..., object], tuple[_PendingNode, ...]]:
        indexed: dict[TaskDefinition[..., object], list[_PendingNode]] = defaultdict(list)
        for node in self._nodes:
            indexed[node.task].append(node)
        return {task: tuple(nodes) for task, nodes in indexed.items()}

    def _resolve_dependencies(
        self,
        dependency_index: dict[TaskDefinition[..., object], tuple[_PendingNode, ...]],
    ) -> dict[str, tuple[str, ...]]:
        resolved: dict[str, tuple[str, ...]] = {}
        for node in self._nodes:
            dependency_ids: list[str] = []
            for dependency_task in node.task.depends_on:
                candidates = dependency_index.get(dependency_task, ())
                if not candidates:
                    raise DependencyResolutionError(
                        f"Task {node.task.name!r} depends on {dependency_task.name!r}, "
                        "but no matching node was added to the DAG."
                    )
                if len(candidates) > 1:
                    raise DAGValidationError(
                        f"Task {node.task.name!r} depends on {dependency_task.name!r}, "
                        "but multiple matching nodes make the dependency ambiguous."
                    )
                dependency_ids.append(candidates[0].node_id)
            resolved[node.node_id] = tuple(dependency_ids)
        return resolved

    def _topological_sort(
        self, resolved_dependencies: dict[str, tuple[str, ...]]
    ) -> tuple[RegisteredNode, ...]:
        pending_by_id = {node.node_id: node for node in self._nodes}
        in_degree = {node.node_id: len(resolved_dependencies[node.node_id]) for node in self._nodes}
        dependents: dict[str, list[str]] = {node.node_id: [] for node in self._nodes}

        for node_id, dependency_ids in resolved_dependencies.items():
            for dependency_id in dependency_ids:
                dependents[dependency_id].append(node_id)

        ready_heap: list[tuple[int, str]] = [
            (pending_by_id[node_id].order, node_id)
            for node_id, degree in in_degree.items()
            if degree == 0
        ]
        heapq.heapify(ready_heap)

        ordered: list[RegisteredNode] = []
        while ready_heap:
            _, node_id = heapq.heappop(ready_heap)
            pending = pending_by_id[node_id]
            ordered.append(
                RegisteredNode(
                    node_id=pending.node_id,
                    order=pending.order,
                    invocation=pending.invocation,
                    dependency_node_ids=resolved_dependencies[node_id],
                )
            )

            for dependent_id in sorted(
                dependents[node_id],
                key=lambda item: pending_by_id[item].order,
            ):
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    heapq.heappush(ready_heap, (pending_by_id[dependent_id].order, dependent_id))

        if len(ordered) != len(self._nodes):
            remaining = sorted(
                (node_id for node_id, degree in in_degree.items() if degree > 0),
                key=lambda item: pending_by_id[item].order,
            )
            raise CycleDetectedError(f"Cycle detected in DAG {self._name!r}: {remaining!r}")

        return tuple(ordered)
