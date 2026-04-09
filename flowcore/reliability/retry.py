"""Retry policy primitives for local task execution."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from flowcore.exceptions import InvalidRetryConfigurationError

BackoffStrategy = Literal["fixed", "exponential"]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Configuration for retrying a task after failure."""

    max_attempts: int = 1
    backoff: BackoffStrategy = "fixed"
    base_delay_seconds: float = 0.0
    max_delay_seconds: float | None = None
    jitter: bool = False
    random_source: Callable[[], float] = field(default=random.random, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise InvalidRetryConfigurationError("max_attempts must be at least 1.")
        if self.backoff not in {"fixed", "exponential"}:
            raise InvalidRetryConfigurationError("backoff must be either 'fixed' or 'exponential'.")
        if self.base_delay_seconds < 0:
            raise InvalidRetryConfigurationError("base_delay_seconds must be >= 0.")
        if self.max_delay_seconds is not None and self.max_delay_seconds < 0:
            raise InvalidRetryConfigurationError("max_delay_seconds must be >= 0.")
        if self.max_delay_seconds is not None and self.max_delay_seconds < self.base_delay_seconds:
            raise InvalidRetryConfigurationError("max_delay_seconds must be >= base_delay_seconds.")

    def compute_delay(self, attempt_number: int) -> float:
        """Return the backoff delay to use after a failed attempt."""

        if attempt_number < 1:
            raise InvalidRetryConfigurationError("attempt_number must be at least 1.")

        if self.backoff == "fixed":
            delay = self.base_delay_seconds
        else:
            delay = self.base_delay_seconds * (2 ** (attempt_number - 1))

        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)

        if self.jitter and delay > 0:
            delay *= self.random_source()

        return delay
