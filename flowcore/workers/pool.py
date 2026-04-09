"""Lightweight worker pool launcher for local distributed demos."""

from __future__ import annotations

import asyncio
import signal
import sys
from dataclasses import dataclass, field


@dataclass(slots=True)
class WorkerPool:
    """Manage local subprocess workers for the distributed demo path."""

    worker_count: int
    redis_url: str
    queue_name: str
    concurrency: int = 1
    _processes: list[asyncio.subprocess.Process] = field(default_factory=list, init=False)

    async def start(self) -> None:
        for _ in range(self.worker_count):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "flowcore",
                "worker",
                "--redis-url",
                self.redis_url,
                "--queue-name",
                self.queue_name,
                "--concurrency",
                str(self.concurrency),
            )
            self._processes.append(process)

    async def stop(self) -> None:
        for process in self._processes:
            if process.returncode is not None:
                continue
            process.send_signal(signal.SIGTERM)
        for process in self._processes:
            if process.returncode is None:
                await process.wait()
