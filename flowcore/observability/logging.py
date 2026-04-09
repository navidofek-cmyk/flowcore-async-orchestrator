"""Centralized structured logging setup for FlowCore."""

from __future__ import annotations

import logging
import sys
from typing import cast

import structlog

from flowcore.observability.tracing import current_context

_CONFIGURED = False


def _add_context(
    _logger: object,
    _method_name: str,
    event_dict: structlog.typing.EventDict,
) -> structlog.typing.EventDict:
    for key, value in current_context().items():
        event_dict.setdefault(key, value)
    return event_dict


def configure_logging(level: int = logging.INFO) -> None:
    """Configure stdlib logging and structlog once per process."""

    global _CONFIGURED
    if _CONFIGURED:
        return

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[structlog.typing.Processor] = [
        structlog.stdlib.add_log_level,
        timestamper,
        _add_context,
    ]

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structured logger instance."""

    configure_logging()
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))
