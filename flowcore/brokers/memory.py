"""Deterministic in-memory broker implementation."""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from flowcore.brokers.base import BrokerMessage

MessageT = TypeVar("MessageT")


@dataclass(slots=True)
class InMemoryBroker(Generic[MessageT]):
    """A simple broker backed by ``asyncio.Queue``."""

    _queue: asyncio.Queue[BrokerMessage[MessageT]] = field(default_factory=asyncio.Queue)
    _next_token: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    async def enqueue(self, payload: MessageT) -> BrokerMessage[MessageT]:
        token = str(next(self._next_token))
        message = BrokerMessage(token=token, payload=payload, receipt=token)
        await self._queue.put(message)
        return message

    async def dequeue(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> BrokerMessage[MessageT] | None:
        if timeout_seconds is None:
            return await self._queue.get()
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout_seconds)
        except TimeoutError:
            return None

    async def ack(self, message: BrokerMessage[MessageT]) -> None:
        _ = message

    async def nack(self, message: BrokerMessage[MessageT]) -> None:
        await self._queue.put(message)

    async def depth(self) -> int:
        return self._queue.qsize()
