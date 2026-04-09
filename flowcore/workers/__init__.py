"""Distributed worker primitives for FlowCore."""

from flowcore.workers.pool import WorkerPool
from flowcore.workers.worker import DistributedWorker

__all__ = ["DistributedWorker", "WorkerPool"]
