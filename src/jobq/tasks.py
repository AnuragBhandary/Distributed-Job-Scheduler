"""Task registry and the per-execution context handed to ``bind=True`` tasks."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    fn: Callable[..., Any]
    queue: str = "default"
    max_attempts: int = 3
    timeout_s: float = 300.0
    backoff_base_s: float = 1.0
    backoff_max_s: float = 300.0
    bind: bool = False

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)


class Registry:
    """Maps task names to handlers. Workers import task modules, which register here."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskSpec] = {}

    def register(self, spec: TaskSpec) -> TaskSpec:
        existing = self._tasks.get(spec.name)
        if existing is not None and existing.fn is not spec.fn:
            raise ValueError(f"task name {spec.name!r} is already registered")
        self._tasks[spec.name] = spec
        return spec

    def get(self, name: str) -> TaskSpec | None:
        return self._tasks.get(name)

    def names(self) -> list[str]:
        return sorted(self._tasks)

    def __contains__(self, name: object) -> bool:
        return name in self._tasks

    def __len__(self) -> int:
        return len(self._tasks)


default_registry = Registry()


@dataclass(slots=True)
class JobContext:
    """Passed as the first argument to tasks registered with ``bind=True``."""

    job_id: UUID
    task: str
    queue: str
    attempt: int
    lease_token: UUID
    _side_effects: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list, repr=False)

    @property
    def idempotency_key(self) -> str:
        """Stable across retries: pass it to downstream APIs so a retried job cannot repeat an
        external effect (e.g. a payment) that an earlier attempt already made."""
        return str(self.job_id)

    def transactional(self, sql: str, *args: Any) -> None:
        """Queue a SQL statement to run in the *same transaction* that marks the job succeeded,
        and only if this worker still holds the lease. The job's database side effects therefore
        commit exactly once, even if the job itself runs more than once."""
        self._side_effects.append((sql, args))

    @property
    def side_effects(self) -> list[tuple[str, tuple[Any, ...]]]:
        return list(self._side_effects)
