"""Unit tests for DLQ behavior using a mocked asynchronous Redis client."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

from agent_resilience import DLQQueue, Job, Priority


def make_queue() -> tuple[DLQQueue, AsyncMock]:
    queue = DLQQueue(key_prefix="test:")
    redis = AsyncMock()
    queue.redis = redis
    queue.connected = True
    return queue, redis


def test_job_round_trip_preserves_fields():
    job = Job(id="job-1", payload={"task": "render"}, metadata={"agent": "worker-1"})

    restored = Job.from_dict(job.to_dict())

    assert restored == job
    assert restored.created_at is not None


def test_enqueue_serializes_priority():
    queue, redis = make_queue()
    job = Job(id="job-2", payload={"task": "index"})

    assert asyncio.run(queue.enqueue(job, Priority.HIGH))

    mapping = redis.zadd.await_args.args[1]
    serialized, score = next(iter(mapping.items()))
    assert json.loads(serialized)["id"] == "job-2"
    assert score == -Priority.HIGH.value
    assert job.priority == Priority.HIGH.value


def test_dequeue_marks_job_in_flight():
    queue, redis = make_queue()
    job = Job(id="job-3", payload={"task": "deliver"})
    redis.zpopmin.return_value = [(json.dumps(job.to_dict()), -Priority.NORMAL.value)]

    dequeued = asyncio.run(queue.dequeue())

    assert dequeued is not None
    assert dequeued.id == "job-3"
    redis.hset.assert_awaited_once()
    assert redis.hset.await_args.args[1] == "job-3"


def test_failure_retries_then_moves_job_to_dlq():
    queue, redis = make_queue()
    job = Job(id="job-4", payload={"task": "notify"}, max_retries=2)

    assert asyncio.run(queue.fail(job, "first failure"))
    assert job.retry_count == 1
    redis.zadd.assert_awaited_once()
    redis.lpush.assert_not_awaited()

    assert asyncio.run(queue.fail(job, "second failure"))
    assert job.retry_count == 2
    redis.lpush.assert_awaited_once()
    stored = json.loads(redis.lpush.await_args.args[1])
    assert stored["id"] == "job-4"
    assert stored["error"] == "second failure"


def test_due_scheduled_job_is_promoted_at_high_priority():
    queue, redis = make_queue()
    job = Job(id="job-5", payload={"task": "scheduled"})
    job.scheduled_time = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    serialized = json.dumps(job.to_dict())
    redis.zrangebyscore.return_value = [serialized]
    queue.enqueue = AsyncMock(return_value=True)

    promoted = asyncio.run(queue.process_scheduled())

    assert promoted == 1
    redis.zrem.assert_awaited_once_with(queue.scheduled_key, serialized)
    queue.enqueue.assert_awaited_once()
    assert queue.enqueue.await_args.args[1] == Priority.HIGH


def test_retry_dlq_job_resets_failure_state():
    queue, redis = make_queue()
    job = Job(id="job-6", payload={"task": "retry"}, retry_count=3, error="failed")
    redis.lrange.return_value = [json.dumps(job.to_dict())]
    queue.enqueue = AsyncMock(return_value=True)

    assert asyncio.run(queue.retry_dlq_job("job-6", Priority.NORMAL))

    redis.lrem.assert_awaited_once()
    retried = queue.enqueue.await_args.args[0]
    assert retried.retry_count == 0
    assert retried.error is None
    assert queue.enqueue.await_args.args[1] == Priority.NORMAL
