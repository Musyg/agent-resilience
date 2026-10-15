"""Observed DLQ and MQTT scenarios using local, in-memory service adapters.

Only the service boundaries are simulated. All queue, retry and buffering
decisions are made by the unmodified agent_resilience modules.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from agent_resilience import DLQQueue, Job, ResilientMQTTPublisher, __version__, dlq, mqtt_buffer


class MemoryRedis:
    """The Redis commands used by this serial demo, backed by local data.

    This adapter does not model server durability, concurrent clients or a
    network. It keeps real serialized jobs rather than canned mock responses.
    """

    def __init__(self):
        self.sorted_sets = defaultdict(dict)
        self.hashes = defaultdict(dict)
        self.lists = defaultdict(list)

    async def zadd(self, key, mapping):
        self.sorted_sets[key].update(mapping)

    async def zpopmin(self, key, count=1):
        values = self.sorted_sets[key]
        result = sorted(values.items(), key=lambda item: (item[1], item[0]))[:count]
        for member, _ in result:
            del values[member]
        return result

    async def zcard(self, key):
        return len(self.sorted_sets[key])

    async def zrange(self, key, start, end):
        ordered = sorted(self.sorted_sets[key], key=lambda member: (self.sorted_sets[key][member], member))
        return ordered[start : None if end == -1 else end + 1]

    async def hset(self, key, field, value):
        self.hashes[key][field] = value

    async def hdel(self, key, field):
        return int(self.hashes[key].pop(field, None) is not None)

    async def hlen(self, key):
        return len(self.hashes[key])

    async def lpush(self, key, value):
        self.lists[key].insert(0, value)

    async def lrange(self, key, start, end):
        return self.lists[key][start : None if end == -1 else end + 1]

    async def llen(self, key):
        return len(self.lists[key])

    async def lrem(self, key, count, value):
        if count != 1:
            raise ValueError("This demo only uses LREM count=1")
        try:
            self.lists[key].remove(value)
            return 1
        except ValueError:
            return 0


def source_hash(module) -> str:
    return hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()


def make_trace(scenario, environment, module, events, config):
    return {
        "schema_version": 1,
        "library_version": __version__,
        "source_sha256": source_hash(module),
        "recorded_at": datetime.now(UTC).isoformat(),
        "scenario": scenario,
        "environment": environment,
        "config": config,
        "events": events,
    }


async def run_dlq_scenario() -> dict:
    """Enqueue, fail twice, inspect, replay and complete the same task."""
    queue = DLQQueue(key_prefix="demo:dlq:")
    queue.redis = MemoryRedis()
    queue.connected = True
    original = Job(id="task-42", payload={"task": "summarize", "document": "demo-note"}, max_retries=2)
    job = original
    events = []
    started = time.perf_counter()
    worker_calls = 0

    async def record(phase, outcome):
        stats = await queue.get_stats()
        if stats["processing"]:
            state = "processing"
        elif stats["dlq_size"]:
            state = "dlq"
        elif stats["queue_size"]:
            state = "queued"
        else:
            state = "completed"
        events.append(
            {
                "index": len(events),
                "elapsed_s": round(time.perf_counter() - started, 6),
                "phase": phase,
                "outcome": outcome,
                "state": state,
                "queue_size": stats["queue_size"],
                "processing": stats["processing"],
                "dlq_size": stats["dlq_size"],
                "worker_calls": worker_calls,
                "retry_count": job.retry_count,
                "job": job.to_dict(),
            }
        )

    if not await queue.enqueue(original):
        raise RuntimeError("Demo enqueue failed")
    await record("queued", "enqueued")
    for attempt in range(2):
        job = await queue.dequeue()
        if job is None:
            raise RuntimeError("Demo task disappeared")
        await record("processing", "dequeued")
        try:
            worker_calls += 1
            raise RuntimeError("local worker unavailable")
        except RuntimeError as error:
            if not await queue.fail(job, str(error)):
                raise RuntimeError("Demo fail() did not retain the task") from error
        await record("retry" if attempt == 0 else "quarantined", "failure")
    retained = await queue.list_dlq()
    if len(retained) != 1 or retained[0].to_dict() != job.to_dict():
        raise RuntimeError("DLQ inspection lost task data")
    await record("inspected", "inspected")
    if not await queue.retry_dlq_job(original.id):
        raise RuntimeError("Demo replay failed")
    queued = await queue.redis.zrange(queue.queue_key, 0, -1)
    if len(queued) != 1:
        raise RuntimeError("Replayed task was not re-enqueued")
    job = Job.from_dict(json.loads(queued[0]))
    await record("replayed", "reenqueued")
    job = await queue.dequeue()
    if job is None:
        raise RuntimeError("Replayed task disappeared")
    await record("final_attempt", "dequeued")
    worker_calls += 1
    if not await queue.complete(job.id):
        raise RuntimeError("Demo completion failed")
    await record("completed", "success")
    trace = make_trace(
        "DLQ task failure and replay",
        "Python DLQQueue with an in-memory Redis service adapter",
        dlq,
        events,
        {"max_retries": 2},
    )
    validate_dlq_trace(trace)
    return trace


def validate_dlq_trace(trace):
    events = trace["events"]
    expected = [
        "queued",
        "processing",
        "queued",
        "processing",
        "dlq",
        "dlq",
        "queued",
        "processing",
        "completed",
    ]
    if [event["state"] for event in events] != expected:
        raise ValueError("Unexpected DLQ state sequence")
    if [event["phase"] for event in events] != [
        "queued",
        "processing",
        "retry",
        "processing",
        "quarantined",
        "inspected",
        "replayed",
        "final_attempt",
        "completed",
    ]:
        raise ValueError("Unexpected DLQ action sequence")
    for event in events:
        sizes = (event["queue_size"], event["processing"], event["dlq_size"])
        wanted = {"queued": (1, 0, 0), "processing": (0, 1, 0), "dlq": (0, 0, 1), "completed": (0, 0, 0)}[
            event["state"]
        ]
        if sizes != wanted:
            raise ValueError("Task counters do not match DLQ state")
        if event["job"]["id"] != "task-42" or event["job"]["payload"] != {
            "task": "summarize",
            "document": "demo-note",
        }:
            raise ValueError("Task identity or payload changed")
        if event["job"]["retry_count"] != event["retry_count"]:
            raise ValueError("Task failure count disagrees with the recorded job")
    if [event["retry_count"] for event in events] != [0, 0, 1, 1, 2, 2, 0, 0, 0]:
        raise ValueError("Incorrect failure counter or replay reset")
    if events[-1]["worker_calls"] != 3 or events[-1]["job"]["error"] is not None:
        raise ValueError("Replay did not complete cleanly")
    if [event["worker_calls"] for event in events] != [0, 0, 1, 1, 2, 2, 2, 2, 3]:
        raise ValueError("Unexpected worker attempt count")


class LocalMQTTClient:
    """Records actual publisher calls at a simulated local MQTT boundary."""

    def __init__(self, lock):
        self._connected = False
        self.offline_observed = threading.Event()
        self.lock = lock
        self._client = self
        self.received = []
        self.on_publish = None

    @property
    def is_connected(self):
        if not self._connected and threading.current_thread().name == "mqtt_buffer_flush":
            self.offline_observed.set()
        return self._connected

    @is_connected.setter
    def is_connected(self, value):
        self._connected = value

    def publish(self, topic, payload, qos, retain):
        if not self.is_connected:
            raise ValueError("local MQTT client disconnected")
        with self.lock:
            self.received.append(
                {"topic": topic, "payload": json.loads(payload), "qos": qos, "retain": retain}
            )
            if self.on_publish:
                self.on_publish()


def run_mqtt_scenario() -> dict:
    """Run the real background flusher across a local client reconnection."""
    lock = threading.RLock()
    client = LocalMQTTClient(lock)
    publisher = ResilientMQTTPublisher(client)
    completed = threading.Event()
    started = time.perf_counter()
    events = []
    produced = 0

    def record(phase, outcome):
        with lock:
            buffered = publisher.buffer.size()
            state = "connected" if client.is_connected else "buffered" if buffered else "offline"
            events.append(
                {
                    "index": len(events),
                    "elapsed_s": round(time.perf_counter() - started, 6),
                    "phase": phase,
                    "outcome": outcome,
                    "state": state,
                    "connected": client.is_connected,
                    "buffered": buffered,
                    "published_count": len(client.received),
                    "produced": produced,
                    "messages": [dict(message) for message in client.received],
                }
            )

    def on_publish():
        record("flushing", "published")
        if len(client.received) == 3:
            completed.set()

    client.on_publish = on_publish
    record("offline", "disconnected")
    publisher.start()
    try:
        if not client.offline_observed.wait(timeout=2):
            raise RuntimeError("Background flusher did not observe the offline client")
        for sequence in range(1, 4):
            produced += 1
            if publisher.publish_critical("demo/events", {"sequence": sequence}):
                raise RuntimeError("An offline message bypassed the buffer")
            record("buffered", "retained")
        with lock:
            client.is_connected = True
            record("reconnected", "connected")
        if not completed.wait(timeout=8):
            raise RuntimeError("Background flusher did not transmit the buffered messages")
        record("drained", "success")
    finally:
        publisher.stop()
        if publisher._flush_thread is not None:
            publisher._flush_thread.join(timeout=6)
            if publisher._flush_thread.is_alive():
                raise RuntimeError("Demo background flusher did not stop")
    trace = make_trace(
        "MQTT offline buffering and automatic flush",
        "Python ResilientMQTTPublisher with a simulated local MQTT client",
        mqtt_buffer,
        events,
        {"messages": 3, "flush_loop": "library background thread"},
    )
    validate_mqtt_trace(trace)
    return trace


def validate_mqtt_trace(trace):
    events = trace["events"]
    if [event["phase"] for event in events] != [
        "offline",
        "buffered",
        "buffered",
        "buffered",
        "reconnected",
        "flushing",
        "flushing",
        "flushing",
        "drained",
    ]:
        raise ValueError("Unexpected MQTT event sequence")
    if [event["buffered"] for event in events[:5]] != [0, 1, 2, 3, 3]:
        raise ValueError("Offline messages were not retained")
    if [event["connected"] for event in events] != [False] * 4 + [True] * 5:
        raise ValueError("Unexpected connection observations")
    if [event["produced"] for event in events] != [0, 1, 2, 3, 3, 3, 3, 3, 3]:
        raise ValueError("Unexpected produced message count")
    if [event["published_count"] for event in events] != [0, 0, 0, 0, 0, 1, 2, 3, 3]:
        raise ValueError("Unexpected transmission progress")
    for event in events:
        if event["published_count"] != len(event["messages"]):
            raise ValueError("Published count does not match observed messages")
        sequences = [message["payload"]["sequence"] for message in event["messages"]]
        if sequences != list(range(1, event["published_count"] + 1)):
            raise ValueError("Message order or identity changed")
        if not event["connected"] and event["published_count"]:
            raise ValueError("A message was transmitted while offline")
        if any(message["topic"] != "demo/events" or message["qos"] != 1 for message in event["messages"]):
            raise ValueError("MQTT message parameters changed")
    if (events[-1]["buffered"], events[-1]["published_count"], events[-1]["produced"]) != (0, 3, 3):
        raise ValueError("Buffer did not drain completely")


def run_additional_scenarios():
    return {"dlq": asyncio.run(run_dlq_scenario()), "mqtt": run_mqtt_scenario()}
