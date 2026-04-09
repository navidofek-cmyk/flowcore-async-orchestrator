from __future__ import annotations

import json
import urllib.request

import pytest

from flowcore.brokers.redis import RedisBroker
from flowcore.cli import _seed_demo_data
from flowcore.observability.dashboard import DashboardDataProvider, DashboardHTTPServer


@pytest.mark.asyncio
@pytest.mark.integration
async def test_dashboard_provider_returns_overview(redis_url: str, redis_queue_name: str) -> None:
    await _seed_demo_data(redis_url, redis_queue_name, workers=2, runs=2, dlq_entries=1)

    provider = DashboardDataProvider(redis_url=redis_url, queue_name=redis_queue_name)
    overview = await provider.overview()

    assert overview["queue_name"] == redis_queue_name
    assert overview["summary"]["dlq"] == 1
    assert len(overview["runs"]) == 2
    assert len(overview["status"]["workers"]) == 2


@pytest.mark.integration
def test_dashboard_http_server_serves_overview(redis_url: str, redis_queue_name: str) -> None:
    import asyncio

    asyncio.run(_seed_demo_data(redis_url, redis_queue_name, workers=1, runs=1, dlq_entries=0))
    server = DashboardHTTPServer(
        DashboardDataProvider(redis_url=redis_url, queue_name=redis_queue_name),
        host="127.0.0.1",
        port=0,
    )
    server.start()
    try:
        assert server._server is not None
        port = server._server.server_port
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/overview", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.stop()

    assert payload["queue_name"] == redis_queue_name
    assert len(payload["runs"]) == 1
