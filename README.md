*[English](#english) · [Čeština](#čeština)*

---

## English

# FlowCore — Async DAG Task Engine

Python library for dependency-aware task orchestration. Runs locally in a single process or distributes work across multiple workers via Redis.

Built to automate multi-step engineering pipelines where tasks have explicit dependencies and each step must complete before downstream work can proceed.

### Engineering Context

Simulation workflows follow a natural DAG structure: geometry generation feeds mesh generation, which feeds solver setup, which feeds the solver run, which feeds post-processing. Running these steps manually is error-prone and does not scale across parameter sweeps or design-of-experiments studies.

FlowCore handles the coordination — dependency resolution, parallel execution where tasks are independent, retry logic for transient failures, and structured logging so failures are traceable.

**Typical applications:**
- Parametric CFD studies — sweep geometry or boundary condition parameters across a batch of solver runs
- Pre/post-processing pipelines — automate meshing → solving → result extraction in sequence
- Multi-solver workflows — chain incompressible pre-run with RANS initialisation and full solve
- Distributed compute — distribute independent simulation cases across available worker processes

### How It Works

```
Task definitions → DAG (dependency graph) → Scheduler → Broker → Worker → Result
```

In **local mode**, the full DAG runs inside one process. In **distributed mode**, the scheduler publishes tasks to Redis; worker processes pull, execute, and return results; the scheduler assembles the final run outcome.

Tasks that share no dependencies execute in parallel. Tasks with upstream dependencies wait for their inputs before starting. Permanent failures block downstream tasks and are stored in a dead-letter queue for inspection.

### Architecture

| Component | Role |
|---|---|
| `flowcore.core.task` | Task definitions, retry metadata, registry identity |
| `flowcore.core.dag` | Graph validation, execution mode selection |
| `flowcore.core.scheduler` | Dependency release, result assembly |
| `flowcore.brokers.redis` | Work queue, result queue, DLQ, status |
| `flowcore.workers.worker` | Distributed task execution, trace context |
| `flowcore.observability` | Structured logging, Prometheus metrics, dashboard |

### Running

**Local:**

```bash
flowcore run --executor local
```

**Distributed:**

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore run --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore
```

**Docker:**

```bash
docker compose up --build
docker compose exec app flowcore run --executor distributed --redis-url redis://redis:6379/0 --queue-name flowcore
```

### Observability

Structured JSON logging via `structlog`, per-run trace ID propagation, Prometheus metrics (task counts, durations, queue depth, DLQ size).

```bash
flowcore metrics                          # print current metrics
flowcore metrics --serve --port 9000      # serve metrics endpoint
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
```

### Reliability

| Property | Behaviour |
|---|---|
| Delivery semantics | At-least-once in distributed mode |
| Idempotency | Practical deduplication via Redis-backed locks |
| Retry policy | Fixed or exponential backoff, configurable per task |
| Permanent failures | Stored in DLQ; downstream tasks marked SKIPPED |
| Redis topology | Single-instance; no cluster or consensus locking |

### Install

```bash
python -m pip install -e .[dev]
pytest -q && mypy flowcore --strict && ruff check .
```

Integration tests require Docker (via `testcontainers-python`).

### CLI Reference

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

---

## Čeština

# FlowCore — asynchronní DAG task engine

Python knihovna pro orchestraci úloh se závislostmi. Běží lokálně v jednom procesu nebo distribuuje práci přes více workerů pomocí Redis.

Navrženo pro automatizaci vícekrokových inženýrských pipeline, kde mají úlohy explicitní závislosti a každý krok musí být dokončen, než může začít navazující práce.

### Inženýrský kontext

Simulační workflow mají přirozenou DAG strukturu: generování geometrie → tvorba sítě → nastavení solveru → výpočet → post-processing. Ruční spouštění těchto kroků je náchylné k chybám a nefunguje pro parametrické studie nebo design-of-experiments.

FlowCore řeší koordinaci — rozlišení závislostí, paralelní spuštění nezávislých úloh, logiku opakování pro přechodné chyby a strukturované logování pro dohledatelnost selhání.

**Typické použití:**
- Parametrické CFD studie — sweep geometrie nebo okrajových podmínek přes dávku výpočtů
- Pre/post-processing pipeline — automatizace meshing → výpočet → extrakce výsledků
- Vícesolver workflow — zřetězení nestlačitelného pre-runu s RANS inicializací a plným výpočtem
- Distribuovaný výpočet — rozdělení nezávislých simulačních případů mezi dostupné worker procesy

### Jak to funguje

```
Definice úloh → DAG (graf závislostí) → Scheduler → Broker → Worker → Výsledek
```

V **lokálním režimu** běží celý DAG v jednom procesu. V **distribuovaném režimu** scheduler publikuje úlohy do Redis; worker procesy je přebírají, spouštějí a vrací výsledky; scheduler sestavuje finální výsledek celého běhu.

Úlohy bez vzájemných závislostí se spouštějí paralelně. Úlohy s upstream závislostmi čekají na vstupy. Trvalá selhání blokují navazující úlohy a jsou uložena v dead-letter queue.

### Architektura

| Komponenta | Role |
|---|---|
| `flowcore.core.task` | Definice úloh, metadata pro retry, registr |
| `flowcore.core.dag` | Validace grafu, volba režimu spuštění |
| `flowcore.core.scheduler` | Uvolňování závislostí, sestavení výsledků |
| `flowcore.brokers.redis` | Fronta práce, fronta výsledků, DLQ, status |
| `flowcore.workers.worker` | Distribuované spuštění úloh, trace kontext |
| `flowcore.observability` | Strukturované logování, Prometheus metriky, dashboard |

### Spuštění

**Lokálně:**

```bash
flowcore run --executor local
```

**Distribuovaně:**

```bash
flowcore worker --redis-url redis://localhost:6379/0 --queue-name flowcore
flowcore run --executor distributed --redis-url redis://localhost:6379/0 --queue-name flowcore
```

**Docker:**

```bash
docker compose up --build
docker compose exec app flowcore run --executor distributed --redis-url redis://redis:6379/0 --queue-name flowcore
```

### Observabilita

Strukturované JSON logování přes `structlog`, propagace trace ID pro každý běh, Prometheus metriky (počty úloh, doby trvání, hloubka fronty, velikost DLQ).

```bash
flowcore metrics                          # výpis aktuálních metrik
flowcore metrics --serve --port 9000      # metrics endpoint
flowcore dashboard --redis-url redis://localhost:6379/0 --queue-name flowcore --port 9100
```

### Spolehlivost

| Vlastnost | Chování |
|---|---|
| Sémantika doručení | At-least-once v distribuovaném režimu |
| Idempotence | Praktická deduplikace přes Redis zámky |
| Retry politika | Pevný nebo exponenciální backoff, konfigurovatelný per úloha |
| Trvalá selhání | Uložena v DLQ; navazující úlohy označeny SKIPPED |
| Redis topologie | Single-instance; bez clusteringu ani consensus locking |

### Instalace

```bash
python -m pip install -e .[dev]
pytest -q && mypy flowcore --strict && ruff check .
```

Integrační testy vyžadují Docker (přes `testcontainers-python`).

### CLI přehled

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
