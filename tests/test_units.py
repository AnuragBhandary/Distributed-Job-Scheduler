"""Pure unit tests: no database or Redis needed."""

from __future__ import annotations

import json
import logging
import random
from uuid import uuid4

import pytest

from jobq.auth import generate_key, hash_secret, parse_key
from jobq.backoff import backoff_delay, decide_failure
from jobq.dispatch import StreamPosition, message_lost, stream_id_ms, stream_key
from jobq.errors import JobFailed, RetryLater
from jobq.logs import JsonFormatter, configure_logging
from jobq.tasks import JobContext, Registry, TaskSpec


class TestBackoff:
    def test_delay_is_bounded_by_exponential_ceiling(self) -> None:
        rng = random.Random(7)
        for attempt in range(1, 12):
            ceiling = min(60.0, 0.5 * 2 ** (attempt - 1))
            for _ in range(200):
                assert 0.0 <= backoff_delay(attempt, 0.5, 60.0, rng) <= ceiling

    def test_full_jitter_spreads_retries(self) -> None:
        rng = random.Random(1)
        samples = [backoff_delay(6, 1.0, 300.0, rng) for _ in range(1000)]
        assert min(samples) < 4 and max(samples) > 28  # ~uniform over [0, 32]

    def test_huge_attempt_numbers_do_not_overflow(self) -> None:
        assert backoff_delay(10_000, 1.0, 5.0) <= 5.0

    def test_attempt_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            backoff_delay(0, 1.0, 5.0)

    def test_retry_until_budget_is_spent(self) -> None:
        kw = {"attempt_offset": 0, "max_attempts": 3, "backoff_base_s": 1, "backoff_max_s": 10}
        assert decide_failure(attempts=1, **kw).status == "scheduled"
        assert decide_failure(attempts=2, **kw).status == "scheduled"
        assert decide_failure(attempts=3, **kw).status == "dead"

    def test_requeued_job_gets_fresh_budget(self) -> None:
        d = decide_failure(
            attempts=4, attempt_offset=3, max_attempts=3, backoff_base_s=1, backoff_max_s=10
        )
        assert d.status == "scheduled"

    def test_permanent_error_skips_retries(self) -> None:
        d = decide_failure(
            attempts=1, attempt_offset=0, max_attempts=5, backoff_base_s=1, backoff_max_s=10,
            permanent=True,
        )  # fmt: skip
        assert d.status == "dead"

    def test_requested_delay_overrides_backoff(self) -> None:
        d = decide_failure(
            attempts=1, attempt_offset=0, max_attempts=5, backoff_base_s=1, backoff_max_s=10,
            requested_delay_s=42.0,
        )  # fmt: skip
        assert (d.status, d.delay_s) == ("scheduled", 42.0)


class TestKeys:
    def test_generated_keys_parse(self) -> None:
        raw = generate_key()
        parsed = parse_key(raw)
        assert parsed is not None
        prefix, secret = parsed
        assert raw == f"jq_{prefix}_{secret}"
        assert len(hash_secret(secret)) == 32

    @pytest.mark.parametrize(
        "raw",
        ["", "jq_short_x", "xx_0123456789ab_abcdefghijklmnopqrstuvwxyz", "jq_0123456789ab_!!"],
    )
    def test_malformed_keys_rejected(self, raw: str) -> None:
        assert parse_key(raw) is None

    def test_secret_may_contain_underscores(self) -> None:
        assert parse_key("jq_0123456789ab_abc_def_ghi_jkl_mno_pqr") == (
            "0123456789ab",
            "abc_def_ghi_jkl_mno_pqr",
        )


class TestLostMessageDetection:
    def test_missing_stream_means_lost(self) -> None:
        assert message_lost(1_000, None, 100)

    def test_no_consumer_group_yet_means_waiting(self) -> None:
        assert not message_lost(1_000, StreamPosition("5000-0", None), 100)

    def test_fully_drained_stream_means_lost(self) -> None:
        assert message_lost(1_000, StreamPosition("900-0", "900-0"), 100)

    def test_group_read_past_dispatch_time_means_lost(self) -> None:
        assert message_lost(1_000, StreamPosition("9000-0", "1500-3"), 100)

    def test_backlog_behind_dispatch_time_means_waiting(self) -> None:
        assert not message_lost(1_000, StreamPosition("9000-0", "1050-0"), 100)

    def test_stream_helpers(self) -> None:
        assert stream_id_ms("1712345678901-7") == 1712345678901
        assert stream_key("emails") == "jobq:q:emails"


class TestRegistry:
    def test_register_and_lookup(self) -> None:
        registry = Registry()

        def fn() -> None: ...

        registry.register(TaskSpec("a.b", fn))
        registry.register(TaskSpec("a.b", fn))  # same function again: idempotent
        assert "a.b" in registry and len(registry) == 1 and registry.names() == ["a.b"]
        assert registry.get("missing") is None

    def test_conflicting_names_rejected(self) -> None:
        registry = Registry()
        registry.register(TaskSpec("dup", lambda: 1))
        with pytest.raises(ValueError):
            registry.register(TaskSpec("dup", lambda: 2))

    async def test_is_async(self) -> None:
        async def coro() -> None: ...

        assert TaskSpec("x", coro).is_async and not TaskSpec("y", lambda: 1).is_async

    def test_context(self) -> None:
        job_id = uuid4()
        ctx = JobContext(job_id=job_id, task="t", queue="q", attempt=2, lease_token=uuid4())
        ctx.transactional("SELECT $1", 1)
        assert ctx.idempotency_key == str(job_id)
        assert ctx.side_effects == [("SELECT $1", (1,))]


class TestMisc:
    def test_json_log_formatter(self) -> None:
        record = logging.makeLogRecord({"msg": "hello %s", "args": ("x",), "job_id": "j1"})
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record.exc_info = sys.exc_info()
        payload = json.loads(JsonFormatter().format(record))
        assert (
            payload["msg"] == "hello x" and payload["job_id"] == "j1" and "boom" in payload["exc"]
        )

    def test_configure_logging(self) -> None:
        configure_logging("DEBUG", json_output=False)
        configure_logging("INFO", json_output=True)
        assert isinstance(logging.getLogger().handlers[0].formatter, JsonFormatter)

    def test_errors(self) -> None:
        assert RetryLater(-5).delay_s == 0.0
        err = JobFailed({"id": "1", "status": "dead", "last_error": "x"})
        assert "dead" in str(err)
