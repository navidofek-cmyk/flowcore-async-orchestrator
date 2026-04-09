# flowcore-async-orchestrator

![FlowCore Dashboard](./docs/dashboard.png)

`FlowCore` is the Python package inside the `flowcore-async-orchestrator` repository. It is a small async-first DAG task engine for Python with local and Redis-backed distributed execution.

- Async task definitions, dependency-aware DAG execution, and a clean Python API.
- Practical distributed execution with retries, DLQ handling, idempotency, locks, and worker processes.
- Built-in operational surface: structured logs, trace correlation, Prometheus metrics, and a lightweight dashboard.

## Why FlowCore

FlowCore exists to make task orchestration easy to understand, easy to run, and easy to inspect.

It solves the problem of coordinating small dependency-based workflows without pulling in a heavyweight platform. You define async tasks, connect them into a DAG, and run them locally or through Redis-backed workers.

FlowCore is not:
- an Airflow-scale platform
- a full workflow control plane
- an exactly-once execution system
- a large observability stack with external infrastructure requirements

## What It Supports

FlowCore supports a focused set of workflow and operational features:

- local single-process DAG execution
- Redis-backed distributed execution
- distributed workers with explicit task registry resolution
- retry policies with fixed or exponential backoff
- dead letter queue entries for permanent failures
- structured execution metadata
- practical idempotency for duplicate distributed deliveries
- Redis-backed locks for best-effort duplicate suppression
- structured logging, trace correlation, and Prometheus metrics

## High-Level Flow

Task -> DAG -> Scheduler -> Broker -> Worker -> Result

In local mode, FlowCore executes the DAG inside one process. In distributed mode, the scheduler publishes work into Redis, workers pull tasks, execute them, publish results back, and FlowCore assembles the final run outcome.

## Architecture Summary

At a high level, FlowCore is split into task definitions, graph validation, scheduling, broker-backed distribution, and operational tooling.

- `flowcore.core.task` defines tasks, invocations, retry metadata, and registry identity.
- `flowcore.core.dag` validates the graph and selects local or distributed execution.
- `flowcore.core.scheduler` coordinates dependency release and final result assembly.
- `flowcore.brokers.redis` provides the Redis work queue, result queue, DLQ storage, and status surfaces.
- `flowcore.workers.worker` executes distributed work items and restores trace context.
- `flowcore.observability` contains centralized logging, tracing, metrics, and dashboard helpers.

## Execution Modes

FlowCore supports two execution modes.

Local mode keeps execution inside one process:

```bash
flowcore run --executor local
```

Distributed mode uses Redis plus one or more workers:

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore run --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore
```

For a quick local demo, FlowCore can auto-launch workers for the built-in pipeline:

```bash
flowcore run --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore --workers 1
```

## Observability

FlowCore includes a lightweight observability layer so runs are easier to debug and operate without requiring a heavyweight platform.

Logs:
- structured JSON logging via `structlog`

Tracing:
- per-run `trace_id` propagation via `contextvars`
- trace propagation across scheduler, serialized work items, and workers

Metrics:
- Prometheus metrics via `prometheus_client`

Metrics include:

- task execution count
- task success count
- task failure count
- task retry count
- task duration histogram
- queue depth
- DLQ size
- worker active task count

Print metrics from the current process:

```bash
flowcore metrics
```

Serve a metrics endpoint:

```bash
flowcore metrics --serve --port 9000
```

Workers can expose their own metrics endpoint:

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore --metrics-port 9000
```

## Operational CLI

FlowCore includes a small operational CLI for inspecting and running the system.

- `flowcore describe`
- `flowcore run --executor local`
- `flowcore run --executor distributed`
- `flowcore worker --concurrency 1`
- `flowcore dashboard --redis-url ... --queue-name ...`
- `flowcore seed-demo --redis-url ... --queue-name ...`
- `flowcore assembly-line-demo --executor distributed --redis-url ... --queue-name ...`
- `flowcore tree-demo --executor distributed --redis-url ... --queue-name ...`
- `flowcore status --redis-url ... --queue-name ...`
- `flowcore dlq --redis-url ... --queue-name ...`
- `flowcore replay-dlq --redis-url ... --queue-name ...`
- `flowcore metrics`

`status` reports queue state and recent execution summary where available. `dlq` lists stored distributed DLQ entries. `replay-dlq` re-enqueues stored distributed work items when replay data is present.

## Dashboard

The dashboard gives you a browser view of recent runs, workers, queue state, and permanent failures.

FlowCore also ships with a lightweight browser dashboard for the Redis-backed mode.

Start workers plus the dashboard:

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
```

Then open `http://localhost:9100` to inspect:

- recent DAG runs in a live-refreshing table
- per-run node status and retry detail
- active worker heartbeats
- DLQ entries for permanent failures

Seed demo workers and synthetic runs when you want a quick UI preview:

```bash
flowcore seed-demo --redis-url redis://localhost:6379/0 --queue-name flowcore
```

Run a longer, easier-to-follow toy factory story when you want to watch real work move through the system for a while:

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
flowcore assembly-line-demo --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore --cycles 4 --delay-scale 1.0 --pause-seconds 2
```

This demo keeps the dashboard active longer by running several toy-production cycles in a row. You can watch queued work, retries, finished runs, and the final outputs accumulate over time.

If you want something easier to explain visually, run the tree-search demo:

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
flowcore tree-demo --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore --workers 1 --delay-scale 1.0
```

This scenario scans a root node, explores a left and right branch in parallel, retries the right side once because of temporary fog, and then chooses the best path. It is meant to be easier to follow on the dashboard than a plain synthetic run.

If you want a completely separate teaching UI with a visible worker card, buttons, and a simpler story page, see [demo_fastapi/README.md](./demo_fastapi/README.md). That demo runs on its own port and does not replace the main FlowCore dashboard.

### What The Dashboard Watches

The dashboard watches the Redis-backed operational state of FlowCore.

- `Redis`: the shared message box that stores queued work, results, DLQ entries, and worker heartbeats
- `worker` processes: helpers that pull work from Redis, execute tasks, and publish results back
- `dashboard` process: a read-only web view that loads recent runs, workers, queue depth, and DLQ data from Redis
- `run summaries`: recent DAG execution snapshots written by the distributed scheduler
- `DLQ`: permanently failed task entries stored for inspection or replay
- `worker heartbeats`: small status records that tell the dashboard whether a worker is idle or busy

The web UI does not connect directly to workers. It connects to Redis-backed state that workers and the scheduler already publish.

The flow looks like this:

```text
FlowCore scheduler -> Redis <- Worker
                         |
                         v
                     Dashboard
```

In practice:

1. the scheduler enqueues work into Redis
2. a worker picks the job up from Redis
3. the worker executes the task and pushes the result back
4. the scheduler writes a run summary
5. workers write heartbeat updates
6. the dashboard reads those Redis records and turns them into the web view

## Retry And DLQ Behavior

Retries and permanent failures are handled at the task level.

- retries happen per task according to `RetryPolicy`
- permanent failures are recorded in the execution result DLQ
- distributed failures are also stored in Redis for operational inspection
- downstream nodes are marked `SKIPPED` when an upstream dependency fails permanently

<!-- ## Guarantees And Limits -->
## Guarantees (Important)

FlowCore aims for practical correctness and debuggability, not exactly-once execution.

- delivery semantics are at-least-once in distributed mode
- idempotency is practical deduplication, not exactly-once semantics
- Redis assumptions are intentionally single-instance
- no Redis clustering or consensus locking is provided
- no web UI or heavy tracing stack is included

## Docker Quick Start

The Docker setup gives you a ready-to-run local environment with Redis plus FlowCore containers.

Build and start Redis plus a worker:

```bash
docker compose up --build
```

Run commands from the app container:

```bash
docker compose exec app flowcore run --executor distributed --redis-url redis://redis:6379/0 --queue-name flowcore
docker compose exec app flowcore status --redis-url redis://redis:6379/0 --queue-name flowcore
```

The worker container exposes metrics on `http://localhost:9000/metrics`.

## Development

Install dependencies:

```bash
python -m pip install -e .[dev]
```

Validation:

```bash
pytest -q
mypy flowcore --strict
ruff check .
ruff format --check .
```

Integration tests use `testcontainers-python`, so Docker must be available locally.
