"""HTTP dashboard for Redis-backed FlowCore runs."""

from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Final
from urllib.parse import urlparse

from flowcore.brokers.redis import RedisBroker

_JSON_CONTENT_TYPE: Final[str] = "application/json; charset=utf-8"
_HTML_CONTENT_TYPE: Final[str] = "text/html; charset=utf-8"


@dataclass(slots=True)
class DashboardDataProvider:
    """Collect Redis-backed operational data for the dashboard."""

    redis_url: str
    queue_name: str

    async def overview(self) -> dict[str, object]:
        broker = RedisBroker.from_url(self.redis_url, queue_name=self.queue_name)
        try:
            status = await broker.status()
            runs = list(await broker.list_recent_runs(limit=10))
            return {
                "queue_name": self.queue_name,
                "status": status,
                "runs": runs,
                "summary": {
                    "queued": status["queue_depth"],
                    "processing": status["processing_depth"],
                    "failed": sum(
                        int(run.get("failed", 0))
                        for run in runs
                        if isinstance(run.get("failed"), int)
                    ),
                    "dlq": status["dlq_depth"],
                },
            }
        finally:
            await broker.close()

    async def run_detail(self, run_id: str) -> dict[str, object] | None:
        broker = RedisBroker.from_url(self.redis_url, queue_name=self.queue_name)
        try:
            result = await broker.get_run_summary(run_id)
            if result is None:
                return None
            return dict(result)
        finally:
            await broker.close()

    async def dlq_entries(self, *, limit: int = 20) -> list[dict[str, object]]:
        broker = RedisBroker.from_url(self.redis_url, queue_name=self.queue_name)
        try:
            return [dict(item) for item in await broker.list_dlq_entries(limit=limit)]
        finally:
            await broker.close()


@dataclass(slots=True)
class DashboardHTTPServer:
    """Tiny dashboard server with built-in HTML, CSS, and JSON APIs."""

    provider: DashboardDataProvider
    host: str = "0.0.0.0"
    port: int = 9100
    _server: ThreadingHTTPServer | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        provider = self.provider

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/":
                    self._write_html(_render_dashboard_html(provider.queue_name))
                    return
                if parsed.path == "/api/overview":
                    self._write_json(asyncio.run(provider.overview()))
                    return
                if parsed.path == "/api/dlq":
                    self._write_json(asyncio.run(provider.dlq_entries()))
                    return
                if parsed.path.startswith("/api/runs/"):
                    run_id = parsed.path.removeprefix("/api/runs/")
                    payload = asyncio.run(provider.run_detail(run_id))
                    if payload is None:
                        self.send_response(HTTPStatus.NOT_FOUND)
                        self.end_headers()
                        return
                    self._write_json(payload)
                    return
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()

            def log_message(self, _format: str, *_args: object) -> None:
                return None

            def _write_html(self, payload: str) -> None:
                encoded = payload.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", _HTML_CONTENT_TYPE)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def _write_json(self, payload: object) -> None:
                encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", _JSON_CONTENT_TYPE)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        self._server = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)


def serve_dashboard_forever(server: DashboardHTTPServer) -> None:
    """Block forever after starting a dashboard server."""

    server.start()
    try:
        while True:
            threading.Event().wait(1.0)
    except KeyboardInterrupt:
        server.stop()


def _render_dashboard_html(queue_name: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FlowCore Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255, 250, 242, 0.92);
      --panel-strong: #fffdf8;
      --ink: #1f2933;
      --muted: #6b7280;
      --line: #dfd1bd;
      --shadow: rgba(61, 42, 18, 0.12);
      --brand: #b45309;
      --brand-soft: #fde7c7;
      --ok: #166534;
      --ok-bg: #dcfce7;
      --warn: #a16207;
      --warn-bg: #fef3c7;
      --bad: #991b1b;
      --bad-bg: #fee2e2;
      --idle: #475569;
      --idle-bg: #e2e8f0;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(180, 83, 9, 0.16), transparent 24rem),
        radial-gradient(circle at bottom right, rgba(127, 29, 29, 0.10), transparent 22rem),
        linear-gradient(180deg, #f8f4ed 0%, #efe8dd 100%);
    }}
    .shell {{
      display: grid;
      grid-template-columns: 16rem minmax(0, 1fr);
      min-height: 100vh;
    }}
    .sidebar {{
      padding: 2rem 1.25rem;
      border-right: 1px solid rgba(102, 77, 46, 0.12);
      background: rgba(255, 248, 240, 0.7);
      backdrop-filter: blur(16px);
    }}
    .brand {{
      margin-bottom: 2rem;
    }}
    .eyebrow {{
      display: inline-block;
      padding: 0.25rem 0.6rem;
      border-radius: 999px;
      background: var(--brand-soft);
      color: var(--brand);
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 0.8rem 0 0.35rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
      font-size: 1.95rem;
      line-height: 1;
    }}
    .sidebar p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }}
    .nav {{
      display: grid;
      gap: 0.55rem;
      margin-top: 2rem;
    }}
    .nav a {{
      display: block;
      padding: 0.85rem 1rem;
      border: 1px solid transparent;
      border-radius: 1rem;
      color: var(--ink);
      text-decoration: none;
      font-weight: 600;
    }}
    .nav a:hover {{
      background: rgba(255, 255, 255, 0.56);
      border-color: rgba(180, 83, 9, 0.16);
    }}
    .content {{
      padding: 2rem;
      display: grid;
      gap: 1.5rem;
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: flex-start;
      padding: 1.6rem 1.8rem;
      border: 1px solid rgba(102, 77, 46, 0.12);
      border-radius: 1.5rem;
      background: linear-gradient(135deg, rgba(255,255,255,0.78), rgba(255,244,230,0.92));
      box-shadow: 0 22px 60px var(--shadow);
    }}
    .hero h2 {{
      margin: 0 0 0.5rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
      font-size: 1.8rem;
    }}
    .hero p {{
      margin: 0;
      max-width: 46rem;
      color: var(--muted);
      line-height: 1.6;
    }}
    .live {{
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.6rem 0.95rem;
      border-radius: 999px;
      background: rgba(255,255,255,0.8);
      border: 1px solid rgba(22, 101, 52, 0.16);
      font-weight: 700;
      white-space: nowrap;
    }}
    .live-dot {{
      width: 0.65rem;
      height: 0.65rem;
      border-radius: 50%;
      background: #22c55e;
      box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.4);
      animation: pulse 1.8s infinite;
    }}
    @keyframes pulse {{
      0% {{ box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.35); }}
      70% {{ box-shadow: 0 0 0 12px rgba(34, 197, 94, 0.0); }}
      100% {{ box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.0); }}
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 1rem;
    }}
    .card, .panel {{
      border: 1px solid rgba(102, 77, 46, 0.12);
      border-radius: 1.35rem;
      background: var(--panel);
      backdrop-filter: blur(14px);
      box-shadow: 0 16px 44px var(--shadow);
    }}
    .card {{
      padding: 1.15rem 1.2rem;
    }}
    .card label {{
      display: block;
      color: var(--muted);
      font-size: 0.86rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .card strong {{
      display: block;
      margin-top: 0.45rem;
      font-size: 2rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
    }}
    .grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(18rem, 0.9fr);
      gap: 1rem;
    }}
    .panel {{
      overflow: hidden;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
      padding: 1rem 1.2rem;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 253, 248, 0.7);
    }}
    .panel-head h3 {{
      margin: 0;
      font-size: 1rem;
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
    }}
    .panel-body {{
      padding: 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 0.9rem 1rem;
      text-align: left;
      border-bottom: 1px solid rgba(223, 209, 189, 0.7);
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      background: rgba(255, 253, 248, 0.56);
    }}
    tbody tr {{
      cursor: pointer;
      transition: background 150ms ease, transform 150ms ease;
    }}
    tbody tr:hover {{
      background: rgba(255, 248, 237, 0.8);
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      padding: 0.28rem 0.6rem;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
    }}
    .status-succeeded {{ color: var(--ok); background: var(--ok-bg); }}
    .status-running, .status-retrying, .status-busy {{ color: var(--warn); background: var(--warn-bg); }}
    .status-failed {{ color: var(--bad); background: var(--bad-bg); }}
    .status-skipped, .status-pending, .status-idle {{ color: var(--idle); background: var(--idle-bg); }}
    .mono {{
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      font-size: 0.86rem;
    }}
    .muted {{
      color: var(--muted);
    }}
    .stack {{
      display: grid;
      gap: 0.8rem;
      padding: 1rem 1.1rem 1.1rem;
    }}
    .worker {{
      padding: 0.95rem;
      border-radius: 1rem;
      background: rgba(255, 255, 255, 0.7);
      border: 1px solid rgba(223, 209, 189, 0.72);
    }}
    .worker strong {{
      display: block;
      margin-bottom: 0.3rem;
    }}
    .detail {{
      padding: 1rem 1.2rem 1.2rem;
      display: grid;
      gap: 1rem;
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0.75rem;
    }}
    .detail-card {{
      padding: 0.85rem 0.9rem;
      border-radius: 1rem;
      background: rgba(255,255,255,0.72);
      border: 1px solid rgba(223, 209, 189, 0.72);
    }}
    .detail-card label {{
      display: block;
      margin-bottom: 0.35rem;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .empty {{
      padding: 1.1rem 1.2rem 1.3rem;
      color: var(--muted);
      line-height: 1.6;
    }}
    @media (max-width: 1080px) {{
      .shell {{
        grid-template-columns: 1fr;
      }}
      .sidebar {{
        border-right: 0;
        border-bottom: 1px solid rgba(102, 77, 46, 0.12);
      }}
      .cards, .detail-grid, .grid {{
        grid-template-columns: 1fr;
      }}
      .hero {{
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div class="brand">
        <span class="eyebrow">FlowCore</span>
        <h1>Operations</h1>
        <p>Live dashboard for queue <span class="mono">{queue_name}</span>. Runs, workers, retries, and DLQ all in one place.</p>
      </div>
      <nav class="nav">
        <a href="#runs">Runs</a>
        <a href="#workers">Workers</a>
        <a href="#detail">Run Detail</a>
        <a href="#dlq">DLQ</a>
      </nav>
    </aside>
    <main class="content">
      <section class="hero">
        <div>
          <span class="eyebrow">Redis-backed Dashboard</span>
          <h2>FlowCore run table, worker pulse, and failure inbox.</h2>
          <p>Inspired by Celery-style monitoring, but tailored to FlowCore: recent DAG runs, task states, active workers, and dead-letter entries with a clean live-refreshing table surface.</p>
        </div>
        <div class="live"><span class="live-dot"></span><span id="live-label">Refreshing every 3s</span></div>
      </section>

      <section class="cards">
        <article class="card"><label>Queued</label><strong id="queued-count">0</strong></article>
        <article class="card"><label>Processing</label><strong id="processing-count">0</strong></article>
        <article class="card"><label>Failed Nodes</label><strong id="failed-count">0</strong></article>
        <article class="card"><label>DLQ Entries</label><strong id="dlq-count">0</strong></article>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h3>How It Works</h3>
          <span class="muted">Simple explanation</span>
        </div>
        <div class="detail">
          <div class="detail-grid">
            <div class="detail-card">
              <label>Redis</label>
              <div>Redis is a very fast message box. FlowCore puts jobs there and workers pick them up.</div>
            </div>
            <div class="detail-card">
              <label>Docker</label>
              <div>Docker is a box for programs. It runs Redis, workers, and the dashboard the same way on every machine.</div>
            </div>
            <div class="detail-card">
              <label>Worker</label>
              <div>A worker is a helper that takes one job, does it, and sends the result back.</div>
            </div>
            <div class="detail-card">
              <label>FlowCore</label>
              <div>FlowCore is the boss that decides what should happen first, next, and what to retry if something fails.</div>
            </div>
          </div>
        </div>
      </section>

      <section class="grid">
        <article class="panel" id="runs">
          <div class="panel-head">
            <h3>Recent Runs</h3>
            <span class="muted mono" id="queue-name">{queue_name}</span>
          </div>
          <div class="panel-body">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>DAG</th>
                  <th>Status</th>
                  <th>Tasks</th>
                  <th>Started</th>
                  <th>Duration</th>
                </tr>
              </thead>
              <tbody id="runs-body"></tbody>
            </table>
          </div>
        </article>

        <article class="panel" id="workers">
          <div class="panel-head">
            <h3>Workers</h3>
            <span class="muted" id="worker-count">0 active</span>
          </div>
          <div class="stack" id="workers-body"></div>
        </article>
      </section>

      <section class="panel" id="detail">
        <div class="panel-head">
          <h3>Run Detail</h3>
          <span class="muted" id="detail-title">Select a run</span>
        </div>
        <div class="detail" id="detail-body">
          <div class="empty">Pick any run from the table above to inspect its node-by-node execution story.</div>
        </div>
      </section>

      <section class="panel" id="dlq">
        <div class="panel-head">
          <h3>Dead Letter Queue</h3>
          <span class="muted">Permanent failures waiting for replay or inspection</span>
        </div>
        <div class="panel-body">
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Node</th>
                <th>Attempts</th>
                <th>Exception</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody id="dlq-body"></tbody>
          </table>
        </div>
      </section>
    </main>
  </div>

  <script>
    const REFRESH_MS = 3000;
    let selectedRunId = null;

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    function statusClass(value) {{
      return "status-" + String(value || "pending").toLowerCase();
    }}

    function statusPill(value) {{
      const label = String(value || "pending");
      return `<span class="pill ${{statusClass(label)}}">${{escapeHtml(label)}}</span>`;
    }}

    function formatWhen(value) {{
      if (!value) {{
        return '<span class="muted">n/a</span>';
      }}
      const date = new Date(value);
      return `<span title="${{escapeHtml(date.toISOString())}}">${{escapeHtml(date.toLocaleString())}}</span>`;
    }}

    function formatDuration(value) {{
      if (value === null || value === undefined) {{
        return '<span class="muted">n/a</span>';
      }}
      return `${{Number(value).toFixed(2)}}s`;
    }}

    function renderRuns(runs) {{
      const body = document.getElementById("runs-body");
      if (!runs.length) {{
        body.innerHTML = `<tr><td colspan="6" class="empty">No runs recorded yet. Start a distributed FlowCore DAG to populate this table.</td></tr>`;
        return;
      }}
      body.innerHTML = runs.map((run) => {{
        const failed = Number(run.failed || 0);
        const skipped = Number(run.skipped || 0);
        return `
          <tr data-run-id="${{escapeHtml(run.run_id)}}">
            <td class="mono">${{escapeHtml(run.run_id)}}</td>
            <td>${{escapeHtml(run.dag_name)}}</td>
            <td>${{statusPill(run.status)}}</td>
            <td>${{escapeHtml(`${{run.node_count}} total / ${{failed}} failed / ${{skipped}} skipped`)}}</td>
            <td>${{formatWhen(run.started_at)}}</td>
            <td>${{formatDuration(run.duration_seconds)}}</td>
          </tr>
        `;
      }}).join("");

      for (const row of body.querySelectorAll("tr[data-run-id]")) {{
        row.addEventListener("click", () => {{
          selectedRunId = row.dataset.runId;
          refreshDetail();
        }});
      }}
    }}

    function renderWorkers(workers) {{
      const target = document.getElementById("workers-body");
      document.getElementById("worker-count").textContent = `${{workers.length}} active`;
      if (!workers.length) {{
        target.innerHTML = `<div class="empty">No active worker heartbeat yet. Start a worker to see live status here.</div>`;
        return;
      }}
      target.innerHTML = workers.map((worker) => `
        <article class="worker">
          <strong>${{escapeHtml(worker.worker_id)}}</strong>
          <div>${{statusPill(worker.status)}}</div>
          <div class="muted">Host: <span class="mono">${{escapeHtml(worker.hostname)}}</span></div>
          <div class="muted">PID: <span class="mono">${{escapeHtml(worker.pid)}}</span></div>
          <div class="muted">Task: <span class="mono">${{escapeHtml(worker.task_name || "idle")}}</span></div>
          <div class="muted">Seen: ${{formatWhen(worker.last_seen_at)}}</div>
        </article>
      `).join("");
    }}

    function renderDlq(entries) {{
      const body = document.getElementById("dlq-body");
      if (!entries.length) {{
        body.innerHTML = `<tr><td colspan="5" class="empty">DLQ is empty. Permanent failures will land here.</td></tr>`;
        return;
      }}
      body.innerHTML = entries.map((entry) => {{
        const item = entry.entry || {{}};
        return `
          <tr>
            <td>${{escapeHtml(item.task_name || "unknown")}}</td>
            <td class="mono">${{escapeHtml(item.node_id || "n/a")}}</td>
            <td>${{escapeHtml(item.attempt_count || 0)}}</td>
            <td>${{escapeHtml(item.exception_type || "n/a")}}</td>
            <td>${{escapeHtml(item.error_message || "")}}</td>
          </tr>
        `;
      }}).join("");
    }}

    async function refreshOverview() {{
      const response = await fetch("/api/overview");
      const payload = await response.json();
      document.getElementById("queue-name").textContent = payload.queue_name;
      document.getElementById("queued-count").textContent = payload.summary.queued;
      document.getElementById("processing-count").textContent = payload.summary.processing;
      document.getElementById("failed-count").textContent = payload.summary.failed;
      document.getElementById("dlq-count").textContent = payload.summary.dlq;
      renderRuns(payload.runs || []);
      renderWorkers((payload.status && payload.status.workers) || []);
      if (!selectedRunId && payload.runs && payload.runs.length) {{
        selectedRunId = payload.runs[0].run_id;
        refreshDetail();
      }}
    }}

    async function refreshDetail() {{
      if (!selectedRunId) {{
        return;
      }}
      const response = await fetch(`/api/runs/${{encodeURIComponent(selectedRunId)}}`);
      if (response.status === 404) {{
        document.getElementById("detail-title").textContent = "Run not found";
        document.getElementById("detail-body").innerHTML = `<div class="empty">This run is no longer in the recent summary buffer.</div>`;
        return;
      }}
      const run = await response.json();
      document.getElementById("detail-title").textContent = `${{run.dag_name}} (${{run.run_id}})`;
      const nodes = Array.isArray(run.nodes) ? run.nodes : [];
      const nodeTable = nodes.length ? `
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Node</th>
              <th>Status</th>
              <th>Attempts</th>
              <th>Duration</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            ${{nodes.map((node) => `
              <tr>
                <td>${{escapeHtml(node.task_name)}}</td>
                <td class="mono">${{escapeHtml(node.node_id)}}</td>
                <td>${{statusPill(node.status)}}</td>
                <td>${{escapeHtml(node.attempt_count)}}</td>
                <td>${{formatDuration(node.duration_seconds)}}</td>
                <td>${{escapeHtml(node.error_message || "")}}</td>
              </tr>
            `).join("")}}
          </tbody>
        </table>
      ` : `<div class="empty">No node details stored for this run.</div>`;

      document.getElementById("detail-body").innerHTML = `
        <div class="detail-grid">
          <div class="detail-card"><label>Status</label>${{statusPill(run.status)}}</div>
          <div class="detail-card"><label>Executor</label><span class="mono">${{escapeHtml(run.executor || "distributed")}}</span></div>
          <div class="detail-card"><label>Started</label>${{formatWhen(run.started_at)}}</div>
          <div class="detail-card"><label>Duration</label>${{formatDuration(run.duration_seconds)}}</div>
        </div>
        <div>${{nodeTable}}</div>
      `;
    }}

    async function refreshDlq() {{
      const response = await fetch("/api/dlq");
      renderDlq(await response.json());
    }}

    async function refreshAll() {{
      try {{
        await Promise.all([refreshOverview(), refreshDlq()]);
      }} catch (error) {{
        document.getElementById("live-label").textContent = "Refresh failed. Retrying...";
        return;
      }}
      document.getElementById("live-label").textContent = "Refreshing every 3s";
    }}

    refreshAll();
    setInterval(refreshAll, REFRESH_MS);
  </script>
</body>
</html>
"""
