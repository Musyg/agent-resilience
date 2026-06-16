"""
DLQ queue - Redis-backed priority queue with scheduled jobs, retry and a
dead-letter queue.

Designed for distributed agents that need durable, prioritised work with
automatic retry and a dead-letter queue for jobs that keep failing.

Backed by:
- a sorted set for the main queue (priority via score),
- a sorted set for scheduled jobs (timestamp via score),
- a hash for in-flight (processing) jobs,
- a list for the DLQ (chronological order).

    from agent_resilience import DLQQueue, Job, Priority

    queue = DLQQueue(redis_url="redis://localhost:6379", key_prefix="myagent:")
    await queue.connect()

    await queue.enqueue(
        Job(id="job-42", payload={"content": "hello"}, metadata={"channel": "x"}),
        priority=Priority.HIGH,
    )

    # Worker loop
    while True:
        job = await queue.dequeue()
        if not job:
            await asyncio.sleep(1)
            continue
        try:
            await process(job)
            await queue.complete(job.id)
        except Exception as e:
            await queue.fail(job, str(e))   # auto retry, then DLQ

The key prefix lets several agents share one Redis without collisions.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = ["Priority", "Job", "DLQQueue"]

try:
    import redis.asyncio as aioredis  # type: ignore

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None  # type: ignore


class Priority(Enum):
    """Job priority. Higher value = higher priority."""

    HIGH = 100
    NORMAL = 50
    LOW = 10


@dataclass
class Job:
    """
    A queued unit of work. Put domain-specific data in `payload`, and routing
    or tagging info in `metadata` (e.g. {"channel": "twitter"}).
    """

    id: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: int = Priority.NORMAL.value
    retry_count: int = 0
    max_retries: int = 3
    scheduled_time: Optional[str] = None
    created_at: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if self.created_at is None:
            self.created_at = datetime.now(UTC).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        return cls(**data)


class DLQQueue:
    """
    Redis-backed priority queue with scheduled jobs, retry and DLQ.

    Parameters
    ----------
    redis_url : str
        Redis URL (default ``redis://localhost:6379``).
    key_prefix : str
        Prefix for all Redis keys (default ``queue:``). Lets multiple agents
        share one Redis without collision.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        key_prefix: str = "queue:",
    ):
        self.redis_url = redis_url
        self.prefix = key_prefix
        self.queue_key = f"{key_prefix}main"
        self.dlq_key = f"{key_prefix}dlq"
        self.scheduled_key = f"{key_prefix}scheduled"
        self.processing_key = f"{key_prefix}processing"
        self.redis: Any = None
        self.connected = False

    async def connect(self) -> bool:
        """Connect to Redis. Returns True on success."""
        if not REDIS_AVAILABLE:
            logger.error("[DLQQueue] redis package not installed")
            return False
        try:
            self.redis = aioredis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
            await self.redis.ping()
            self.connected = True
            logger.info("[DLQQueue] connected (%s, prefix=%s)", self.redis_url, self.prefix)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] connection failed: %s", e)
            self.connected = False
            return False

    async def disconnect(self) -> None:
        """Close the Redis connection cleanly."""
        if self.redis:
            await self.redis.close()
            self.connected = False

    # ------------------------------------------------------------------ #
    # Enqueue / schedule / dequeue
    # ------------------------------------------------------------------ #
    async def enqueue(self, job: Job, priority: Priority = Priority.NORMAL) -> bool:
        """Push a job onto the main queue with the given priority."""
        if not self.connected:
            return False
        try:
            job.priority = priority.value
            # Negative score so ZPOPMIN returns highest priority first.
            await self.redis.zadd(self.queue_key, {json.dumps(job.to_dict()): -priority.value})
            logger.debug("[DLQQueue] enqueue %s priority=%s", job.id, priority.name)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] enqueue error: %s", e)
            return False

    async def schedule(self, job: Job, run_at: datetime) -> bool:
        """Schedule a job for the future. ``process_scheduled()`` promotes due jobs."""
        if not self.connected:
            return False
        try:
            job.scheduled_time = run_at.isoformat()
            await self.redis.zadd(self.scheduled_key, {json.dumps(job.to_dict()): run_at.timestamp()})
            logger.info("[DLQQueue] scheduled %s for %s", job.id, run_at)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] schedule error: %s", e)
            return False

    async def dequeue(self) -> Optional[Job]:
        """Pop the highest-priority job (atomic ZPOPMIN). Marks it in-flight."""
        if not self.connected:
            return None
        try:
            result = await self.redis.zpopmin(self.queue_key, count=1)
            if not result:
                return None
            job_json, _score = result[0]
            job = Job.from_dict(json.loads(job_json))
            await self.redis.hset(
                self.processing_key,
                job.id,
                json.dumps({"started_at": datetime.now(UTC).isoformat(), "job": job.to_dict()}),
            )
            return job
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] dequeue error: %s", e)
            return None

    # ------------------------------------------------------------------ #
    # Complete / fail
    # ------------------------------------------------------------------ #
    async def complete(self, job_id: str) -> bool:
        """Mark a job completed (removes it from in-flight)."""
        if not self.connected:
            return False
        try:
            await self.redis.hdel(self.processing_key, job_id)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] complete error: %s", e)
            return False

    async def fail(self, job: Job, error: str) -> bool:
        """
        Handle a job failure. Retries up to ``max_retries`` times, then moves the
        job to the DLQ. Retries get LOW priority so fresh jobs keep precedence.
        """
        if not self.connected:
            return False
        try:
            await self.redis.hdel(self.processing_key, job.id)
            job.retry_count += 1
            job.error = error
            if job.retry_count < job.max_retries:
                job.priority = Priority.LOW.value
                await self.redis.zadd(self.queue_key, {json.dumps(job.to_dict()): -Priority.LOW.value})
                logger.warning("[DLQQueue] retry %s %d/%d", job.id, job.retry_count, job.max_retries)
            else:
                await self.redis.lpush(self.dlq_key, json.dumps(job.to_dict()))
                logger.error("[DLQQueue] %s -> DLQ after %d retries", job.id, job.max_retries)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] fail error: %s", e)
            return False

    # ------------------------------------------------------------------ #
    # Scheduled promotion
    # ------------------------------------------------------------------ #
    async def process_scheduled(self) -> int:
        """Promote scheduled jobs whose time has come to the main queue."""
        if not self.connected:
            return 0
        try:
            now = datetime.now(UTC).timestamp()
            ready = await self.redis.zrangebyscore(self.scheduled_key, min=0, max=now)
            count = 0
            for job_json in ready:
                job = Job.from_dict(json.loads(job_json))
                await self.redis.zrem(self.scheduled_key, job_json)
                await self.enqueue(job, Priority.HIGH)
                count += 1
            if count:
                logger.info("[DLQQueue] promoted %d scheduled job(s)", count)
            return count
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] process_scheduled error: %s", e)
            return 0

    # ------------------------------------------------------------------ #
    # DLQ inspection / replay
    # ------------------------------------------------------------------ #
    async def list_dlq(self, limit: int = 20) -> List[Job]:
        """List the most recent jobs in the DLQ."""
        if not self.connected:
            return []
        try:
            raw = await self.redis.lrange(self.dlq_key, 0, limit - 1)
            return [Job.from_dict(json.loads(j)) for j in raw]
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] list_dlq error: %s", e)
            return []

    async def retry_dlq_job(self, job_id: str, priority: Priority = Priority.NORMAL) -> bool:
        """Find a job by id in the DLQ, reset its retry counter and re-enqueue it."""
        if not self.connected:
            return False
        try:
            for raw in await self.redis.lrange(self.dlq_key, 0, -1):
                job = Job.from_dict(json.loads(raw))
                if job.id == job_id:
                    job.retry_count = 0
                    job.error = None
                    await self.redis.lrem(self.dlq_key, 1, raw)
                    await self.enqueue(job, priority)
                    logger.info("[DLQQueue] retried %s from DLQ", job_id)
                    return True
            return False
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] retry_dlq error: %s", e)
            return False

    async def purge_dlq(self) -> int:
        """Empty the DLQ (admin action). Returns count purged."""
        if not self.connected:
            return 0
        try:
            n = await self.redis.llen(self.dlq_key)
            await self.redis.delete(self.dlq_key)
            return n or 0
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] purge_dlq error: %s", e)
            return 0

    # ------------------------------------------------------------------ #
    # Stats
    # ------------------------------------------------------------------ #
    async def get_stats(self) -> Dict[str, Any]:
        """For a /health or /queue/status endpoint."""
        if not self.connected:
            return {"connected": False}
        try:
            return {
                "connected": True,
                "queue_size": await self.redis.zcard(self.queue_key),
                "scheduled_size": await self.redis.zcard(self.scheduled_key),
                "dlq_size": await self.redis.llen(self.dlq_key),
                "processing": await self.redis.hlen(self.processing_key),
                "prefix": self.prefix,
            }
        except Exception as e:  # noqa: BLE001
            logger.error("[DLQQueue] stats error: %s", e)
            return {"connected": True, "error": str(e)}
