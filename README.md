# FlowCore — Async DAG Task Engine

Python library for dependency-aware task orchestration. Runs locally in a single process or distributes work across multiple workers via Redis.

Built to automate multi-step engineering pipelines where tasks have explicit dependencies and each step must complete before downstream work can proceed.

---

## Engineering Context

Simulation workflows follow a natural DAG structure: geometry generation feeds mesh generation, which feeds solver setup, which feeds the solver run, which feeds post-processing. Running these steps manually is error-prone and does not scale across parameter sweeps or design-of-experiments studies.

FlowCore handles the coordination — dependency resolution, parallel execution where tasks are independent, retry logic for transient failures, and structured logging so failures are traceable.

**Typical applications:**
- Parametric CFD studies — sweep geometry or boundary condition parameters across a batch of solver runs
- Pre/post-processing pipelines — automate meshing → solving → result extraction in sequence
- Multi-solver workflows — chain incompressible pre-run with RANS initialisation and full solve
- Distributed compute — distribute independent simulation cases across available worker processes

---

## How It Works

```
Task definitions → DAG (dependency graph) → Scheduler → Broker → Worker → Result
```

In **local mode**, the full DAG runs inside one process. In **distributed mode**, the scheduler publishes tasks to Redis; worker processes pull, execute, and return results; the scheduler assembles the final run outcome.

Tasks that share no dependencies execute in parallel. Tasks with upstream dependencies wait for their inputs before starting. Permanent failures block downstream tasks and are recorded in a dead-letter queue for inspection.

---

## Architecture

| Component | Role |
|---|---|
| `flowcore.core.task` | Task definitions, retry metadata, registry identity |
| `flowcore.core.dag` | Graph validation, local or distributed execution selection |
| `flowcore.core.scheduler` | Dependency release, result assembly |
| `flowcore.brokers.redis` | Work queue, result queue, DLQ, status |
| `flowcore.workers.worker` | Distributed task execution, trace context restoration |
| `flowcore.observability` | Structured logging, trace correlation, Prometheus metrics, dashboard |

---

## Running

**Local execution:**

```bash
flowcore run --executor local
```

**Distributed execution:**

```bash
# Start worker(s)
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore

# Submit DAG run
flowcore run --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore
```

**Docker (Redis + worker in one step):**

```bash
docker compose up --build
docker compose exec app flowcore run --executor distributed --redis-url redis://redis:6379/0 --queue-name flowcore
```

---

## Observability

Structured JSON logging via `structlog`, per-run trace ID propagation, and Prometheus metrics (task counts, durations, queue depth, DLQ size).

```bash
# Print current metrics
flowcore metrics

# Serve metrics endpoint
flowcore metrics --serve --port 9000
```

A lightweight browser dashboard for Redis-backed runs:

```bash
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
```

Shows active runs, per-node status, worker heartbeats, and DLQ entries.

---

## Reliability

| Property | Behaviour |
|---|---|
| Delivery semantics | At-least-once in distributed mode |
| Idempotency | Practical deduplication via Redis-backed locks |
| Retry policy | Fixed or exponential backoff, configurable per task |
| Permanent failures | Stored in DLQ; downstream tasks marked SKIPPED |
| Redis topology | Single-instance; no cluster or consensus locking |

---

## Install

```bash
python -m pip install -e .[dev]
```

Tests and validation:

```bash
pytest -q
mypy flowcore --strict
ruff check .
```

Integration tests require Docker (via `testcontainers-python`).

---

## CLI Reference

```bash
flowcore describe
flowcore run --executor [local|distributed]
flowcore worker --concurrency N
flowcore status
flowcore dlq
flowcore replay-dlq
flowcore metrics
flowcore dashboard
```
