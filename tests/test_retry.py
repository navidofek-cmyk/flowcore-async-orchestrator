from __future__ import annotations

import pytest

from flowcore.exceptions import InvalidRetryConfigurationError
from flowcore.reliability.retry import RetryPolicy


def test_fixed_backoff_delay() -> None:
    policy = RetryPolicy(max_attempts=3, backoff="fixed", base_delay_seconds=0.5)

    assert policy.compute_delay(1) == 0.5
    assert policy.compute_delay(2) == 0.5


def test_exponential_backoff_delay() -> None:
    policy = RetryPolicy(max_attempts=4, backoff="exponential", base_delay_seconds=0.5)

    assert policy.compute_delay(1) == 0.5
    assert policy.compute_delay(2) == 1.0
    assert policy.compute_delay(3) == 2.0


def test_max_delay_is_clamped() -> None:
    policy = RetryPolicy(
        max_attempts=4,
        backoff="exponential",
        base_delay_seconds=0.5,
        max_delay_seconds=1.25,
    )

    assert policy.compute_delay(3) == 1.25


def test_invalid_retry_configuration_is_rejected() -> None:
    with pytest.raises(InvalidRetryConfigurationError):
        RetryPolicy(max_attempts=0)
