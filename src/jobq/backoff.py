"""Retry policy: exponential backoff with full jitter, and the retry-or-dead decision."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Literal


def backoff_delay(
    attempt: int, base_s: float, max_s: float, rng: random.Random | None = None
) -> float:
    """Delay before retry number ``attempt`` (1-based), using "full jitter".

    ``uniform(0, min(max_s, base_s * 2 ** (attempt - 1)))``. Full jitter spreads retries of
    jobs that failed together (e.g. a downstream outage) so they do not hammer the dependency
    again in lock-step. See the AWS Architecture Blog post "Exponential Backoff And Jitter".
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    ceiling = min(max_s, base_s * 2.0 ** min(attempt - 1, 62))
    return (rng or random).uniform(0.0, ceiling)


@dataclass(frozen=True, slots=True)
class FailureDecision:
    status: Literal["scheduled", "dead"]
    delay_s: float


def decide_failure(
    *,
    attempts: int,
    attempt_offset: int,
    max_attempts: int,
    backoff_base_s: float,
    backoff_max_s: float,
    permanent: bool = False,
    requested_delay_s: float | None = None,
    rng: random.Random | None = None,
) -> FailureDecision:
    """Decide what happens to a job whose latest attempt failed.

    ``attempts`` counts every attempt ever made; ``attempt_offset`` is the value of
    ``attempts`` when the job was last requeued from the dead-letter queue, so a requeued job
    gets a fresh retry budget while execution history keeps unique attempt numbers.
    """
    used = attempts - attempt_offset
    if permanent or used >= max_attempts:
        return FailureDecision("dead", 0.0)
    if requested_delay_s is not None:
        return FailureDecision("scheduled", requested_delay_s)
    return FailureDecision("scheduled", backoff_delay(used, backoff_base_s, backoff_max_s, rng))
