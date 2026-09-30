"""Redis Streams as a fast, *lossy* dispatch channel.

One stream per queue (``jobq:q:<queue>``) with one consumer group (``workers``). A message is
only a hint that a job id is ready; the job row in PostgreSQL is the truth and the claim there
is conditional. Therefore:

* Workers read with ``NOACK``: there is no pending-entries list to babysit. A worker that dies
  after reading but before claiming just loses the hint.
* Duplicate messages are harmless: the second claim finds the job no longer 'queued'.
* Lost messages (Redis restart, crash between the PostgreSQL commit and ``XADD``, worker death
  before claim) are found by the scheduler's sweeper, which compares the job's dispatch time
  with how far the consumer group has read (see :func:`message_lost`).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from uuid import UUID

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from jobq.config import Settings

STREAM_PREFIX = "jobq:q:"
GROUP = "workers"


def create_redis(settings: Settings) -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=True)


def stream_key(queue: str) -> str:
    return f"{STREAM_PREFIX}{queue}"


async def dispatch(redis: aioredis.Redis, jobs: Iterable[tuple[UUID, str]], maxlen: int) -> int:
    """XADD one message per (job_id, queue), pipelined into a single round trip."""
    pipe = redis.pipeline(transaction=False)
    count = 0
    for job_id, queue in jobs:
        pipe.xadd(stream_key(queue), {"id": str(job_id)}, maxlen=maxlen, approximate=True)
        count += 1
    if count:
        await pipe.execute()
    return count


async def ensure_group(redis: aioredis.Redis, queue: str) -> None:
    """Create the consumer group (and stream) if missing. Reading starts from id 0 so messages
    added before the first worker ever started are still delivered."""
    try:
        await redis.xgroup_create(stream_key(queue), GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


@dataclass(frozen=True, slots=True)
class StreamPosition:
    last_generated_id: str
    last_delivered_id: str | None  # None: no consumer group yet


async def stream_position(redis: aioredis.Redis, queue: str) -> StreamPosition | None:
    """How far the stream has been written and read, or None if the stream does not exist."""
    key = stream_key(queue)
    try:
        info = await redis.xinfo_stream(key)
        groups = await redis.xinfo_groups(key)
    except ResponseError:
        return None
    delivered = next((g["last-delivered-id"] for g in groups if g["name"] == GROUP), None)
    return StreamPosition(info["last-generated-id"], delivered)


def stream_id_ms(stream_id: str) -> int:
    return int(stream_id.split("-", 1)[0])


def message_lost(dispatched_ms: int, position: StreamPosition | None, margin_ms: int) -> bool:
    """Is a job that has been 'queued' since ``dispatched_ms`` no longer going to be delivered?

    Streams are FIFO and stream ids carry a millisecond timestamp, so once the group has read
    past a message added clearly *after* this job's dispatch, this job's message (if it was ever
    added) was already handed to some worker, which evidently never claimed it.
    """
    if position is None:
        return True  # stream gone: Redis was flushed or restarted without persistence
    if position.last_delivered_id is None:
        return False  # no worker has attached to this queue yet; the message is waiting
    if position.last_delivered_id == position.last_generated_id:
        return True  # the group has read everything; nothing left to deliver this job
    return stream_id_ms(position.last_delivered_id) > dispatched_ms + margin_ms
