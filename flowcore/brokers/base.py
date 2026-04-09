"""Typed broker abstraction for future execution backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

MessageT = TypeVar("MessageT")


@dataclass(frozen=True, slots=True)
class BrokerMessage(Generic[MessageT]):
    """A typed broker message wrapper."""

    token: str
    payload: MessageT
    receipt: str


class Broker(Protocol[MessageT]):
    """Minimal async broker contract for queue-backed execution."""

    async def enqueue(self, payload: MessageT) -> BrokerMessage[MessageT]:
        """Push a message into the broker."""

    async def dequeue(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> BrokerMessage[MessageT] | None:
        """Pull the next available message from the broker."""

    async def ack(self, message: BrokerMessage[MessageT]) -> None:
        """Acknowledge successful processing."""

    async def nack(self, message: BrokerMessage[MessageT]) -> None:
        """Reject processing and make the message available again."""

    async def depth(self) -> int:
        """Return the current queue depth."""
