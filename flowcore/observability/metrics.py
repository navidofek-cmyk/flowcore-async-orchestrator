"""Prometheus metrics for FlowCore."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import sleep
from typing import Final

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_METRICS_CONTENT_TYPE: Final[str] = "text/plain; version=0.0.4; charset=utf-8"


@dataclass(slots=True)
class FlowCoreMetrics:
    """Registry-aware metrics collector used by FlowCore code paths."""

    registry: CollectorRegistry = field(default_factory=CollectorRegistry)
    task_execution_total: Counter = field(init=False)
    task_success_total: Counter = field(init=False)
    task_failure_total: Counter = field(init=False)
    task_retry_total: Counter = field(init=False)
    task_duration_seconds: Histogram = field(init=False)
    queue_depth: Gauge = field(init=False)
    dlq_size: Gauge = field(init=False)
    worker_active_tasks: Gauge = field(init=False)

    def __post_init__(self) -> None:
        self.task_execution_total = Counter(
            "flowcore_task_execution_total",
            "Count of task executions started.",
            ("executor", "task_name"),
            registry=self.registry,
        )
        self.task_success_total = Counter(
            "flowcore_task_success_total",
            "Count of task executions completed successfully.",
            ("executor", "task_name"),
            registry=self.registry,
        )
        self.task_failure_total = Counter(
            "flowcore_task_failure_total",
            "Count of task executions completed with failure.",
            ("executor", "task_name"),
            registry=self.registry,
        )
        self.task_retry_total = Counter(
            "flowcore_task_retry_total",
            "Count of task retries triggered.",
            ("executor", "task_name"),
            registry=self.registry,
        )
        self.task_duration_seconds = Histogram(
            "flowcore_task_duration_seconds",
            "Observed task execution durations.",
            ("executor", "task_name"),
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "flowcore_queue_depth",
            "Current broker queue depth.",
            ("queue_name",),
            registry=self.registry,
        )
        self.dlq_size = Gauge(
            "flowcore_dlq_size",
            "Current DLQ size.",
            ("queue_name",),
            registry=self.registry,
        )
        self.worker_active_tasks = Gauge(
            "flowcore_worker_active_tasks",
            "Currently active worker tasks.",
            ("queue_name",),
            registry=self.registry,
        )

    def record_task_execution(self, *, executor: str, task_name: str) -> None:
        self.task_execution_total.labels(executor=executor, task_name=task_name).inc()

    def record_task_success(
        self,
        *,
        executor: str,
        task_name: str,
        duration_seconds: float | None,
    ) -> None:
        self.task_success_total.labels(executor=executor, task_name=task_name).inc()
        if duration_seconds is not None:
            self.task_duration_seconds.labels(executor=executor, task_name=task_name).observe(
                duration_seconds
            )

    def record_task_failure(
        self,
        *,
        executor: str,
        task_name: str,
        duration_seconds: float | None,
    ) -> None:
        self.task_failure_total.labels(executor=executor, task_name=task_name).inc()
        if duration_seconds is not None:
            self.task_duration_seconds.labels(executor=executor, task_name=task_name).observe(
                duration_seconds
            )

    def record_task_retry(self, *, executor: str, task_name: str) -> None:
        self.task_retry_total.labels(executor=executor, task_name=task_name).inc()

    def set_queue_depth(self, *, queue_name: str, depth: int) -> None:
        self.queue_depth.labels(queue_name=queue_name).set(depth)

    def set_dlq_size(self, *, queue_name: str, size: int) -> None:
        self.dlq_size.labels(queue_name=queue_name).set(size)

    def inc_worker_active_tasks(self, *, queue_name: str) -> None:
        self.worker_active_tasks.labels(queue_name=queue_name).inc()

    def dec_worker_active_tasks(self, *, queue_name: str) -> None:
        self.worker_active_tasks.labels(queue_name=queue_name).dec()

    def render(self) -> str:
        payload = generate_latest(self.registry)
        return payload.decode("utf-8") if isinstance(payload, bytes) else payload


@dataclass(slots=True)
class MetricsHTTPServer:
    """Tiny HTTP server that exposes a Prometheus metrics registry."""

    metrics: FlowCoreMetrics
    host: str = "0.0.0.0"
    port: int = 9000
    _server: ThreadingHTTPServer | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        registry = self.metrics.registry

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/":
                    payload = b"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FlowCore Metrics</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f4ec;
      --panel: #fffaf0;
      --ink: #1f2933;
      --accent: #b45309;
      --border: #e5d3b3;
    }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: linear-gradient(135deg, #f3efe4, #fbf7ef);
      color: var(--ink);
    }
    main {
      max-width: 820px;
      margin: 48px auto;
      padding: 24px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 12px 32px rgba(84, 63, 25, 0.08);
    }
    h1 {
      margin-top: 0;
      font-size: 2rem;
    }
    p {
      line-height: 1.6;
    }
    a, button {
      color: white;
      background: var(--accent);
      border: none;
      border-radius: 999px;
      padding: 10px 16px;
      font-size: 0.95rem;
      text-decoration: none;
      cursor: pointer;
    }
    .actions {
      display: flex;
      gap: 12px;
      margin: 20px 0;
      flex-wrap: wrap;
    }
    pre {
      margin: 0;
      padding: 16px;
      background: #24170b;
      color: #f8f3ea;
      border-radius: 12px;
      overflow: auto;
      min-height: 220px;
      font-size: 0.88rem;
    }
    .hint {
      color: #6b7280;
      font-size: 0.92rem;
    }
  </style>
</head>
<body>
  <main>
    <section class="panel">
      <h1>FlowCore Metrics</h1>
      <p>
        This demo endpoint exposes Prometheus-compatible metrics for the current
        FlowCore process.
      </p>
      <div class="actions">
        <a href="/metrics" target="_blank" rel="noopener noreferrer">Open Raw Metrics</a>
        <button type="button" onclick="loadMetrics()">Refresh Preview</button>
      </div>
      <p class="hint">Preview below shows the first lines returned by <code>/metrics</code>.</p>
      <pre id="metrics-preview">Loading metrics preview...</pre>
    </section>
  </main>
  <script>
    async function loadMetrics() {
      const target = document.getElementById("metrics-preview");
      target.textContent = "Loading metrics preview...";
      try {
        const response = await fetch("/metrics");
        if (!response.ok) {
          target.textContent = "Failed to load /metrics: HTTP " + response.status;
          return;
        }
        const text = await response.text();
        target.textContent = text.split("\\n").slice(0, 40).join("\\n");
      } catch (error) {
        target.textContent = "Failed to load /metrics: " + error;
      }
    }
    loadMetrics();
  </script>
</body>
</html>
"""
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if self.path != "/metrics":
                    self.send_response(404)
                    self.end_headers()
                    return
                payload = generate_latest(registry)
                self.send_response(200)
                self.send_header("Content-Type", _METRICS_CONTENT_TYPE)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, _format: str, *_args: object) -> None:
                return None

        self._server = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)


_DEFAULT_METRICS = FlowCoreMetrics()


def get_metrics() -> FlowCoreMetrics:
    """Return the process-local default metrics collector."""

    return _DEFAULT_METRICS


def configure_metrics(metrics: FlowCoreMetrics) -> None:
    """Replace the process-local default metrics collector."""

    global _DEFAULT_METRICS
    _DEFAULT_METRICS = metrics


def serve_metrics_forever(server: MetricsHTTPServer, *, poll_interval_seconds: float = 1.0) -> None:
    """Block forever after starting a metrics server."""

    server.start()
    try:
        while True:
            sleep(poll_interval_seconds)
    except KeyboardInterrupt:
        server.stop()
