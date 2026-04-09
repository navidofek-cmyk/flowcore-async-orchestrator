from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from flowcore.observability.metrics import FlowCoreMetrics, configure_metrics

try:
    from testcontainers.redis import RedisContainer
except ImportError:  # pragma: no cover - exercised only when dependency is missing
    RedisContainer = None


@pytest.fixture(autouse=True)
def isolated_metrics() -> None:
    configure_metrics(FlowCoreMetrics())


@pytest.fixture()
def redis_container() -> AsyncIterator[RedisContainer]:
    if RedisContainer is None:
        pytest.skip("testcontainers is not installed.")
    container = RedisContainer("redis:7.2-alpine")
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"Redis testcontainer could not start: {exc}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture()
async def redis_url(redis_container: RedisContainer) -> AsyncIterator[str]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"
    client = Redis.from_url(url)
    for _ in range(20):
        try:
            if await client.ping():
                break
        except Exception:
            await asyncio.sleep(0.25)
    else:
        await client.aclose()
        pytest.skip("Redis testcontainer did not become ready in time.")

    try:
        yield url
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.fixture()
def redis_queue_name() -> str:
    return f"flowcore-test-{uuid.uuid4().hex}"
