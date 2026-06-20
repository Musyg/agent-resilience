# agent-resilience

[![CI](https://github.com/Musyg/agent-resilience/actions/workflows/ci.yml/badge.svg)](https://github.com/Musyg/agent-resilience/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)

Small, dependency-light resilience primitives for distributed agent systems.
Three building blocks I rely on to keep long-running, multi-machine agents
stable when dependencies misbehave:

- **Circuit breaker** - per-dependency failure isolation (`CLOSED → OPEN → HALF_OPEN → CLOSED`),
  with a registry, a context manager, a decorator, and optional Redis state persistence.
- **DLQ queue** - a Redis-backed priority queue with scheduled jobs, automatic retry,
  and a dead-letter queue for jobs that keep failing.
- **MQTT buffer** - offline message buffering so critical events survive a broker outage
  and flush automatically on reconnect.

The core (circuit breaker, MQTT buffer) has **zero required dependencies** -
just the standard library. Redis and paho-mqtt are optional extras.

> These primitives run in production in [Talos](https://github.com/Musyg/talos), my distributed agentic
> platform, across a four-node fleet. This repo is the generalized, standalone
> extraction.

## Install

```bash
pip install agent-resilience          # core only
pip install "agent-resilience[redis]" # + Redis DLQ / breaker persistence
pip install "agent-resilience[mqtt]"  # + MQTT buffer client
```

## Circuit breaker

Stop hammering a failing dependency and fail fast instead.

```python
from agent_resilience import CircuitBreakerRegistry, CircuitBreakerOpen

registry = CircuitBreakerRegistry(config={
    "default": {"failure_threshold": 5, "recovery_timeout_s": 30},
    "llm":     {"failure_threshold": 3, "recovery_timeout_s": 60},
})

# Context manager - auto records success/failure
with registry.guarded("llm"):
    answer = call_llm(prompt)

# Decorator - works on sync and async functions
@registry.protect("llm")
async def call_llm(prompt): ...
```

When `"llm"` fails 3 times it opens; further calls raise `CircuitBreakerOpen`
immediately for 60s, then one probe is allowed (`HALF_OPEN`). A success closes
it; a failure reopens it.

```
CLOSED ──fails ≥ threshold──▶ OPEN ──after recovery_timeout──▶ HALF_OPEN
   ▲                                                              │
   └────────────────── probe succeeds ◀───────────────────────── │
                       probe fails ─────────────────────────────▶ OPEN
```

`registry.summary()` returns every breaker's state for a `/health` endpoint.
`RedisCircuitBreakerSync` checkpoints state so breakers survive a restart.

## DLQ queue

Durable, prioritized work with retry and a dead-letter queue.

```python
from agent_resilience import DLQQueue, Job, Priority

queue = DLQQueue(redis_url="redis://localhost:6379", key_prefix="myagent:")
await queue.connect()

await queue.enqueue(Job(id="job-42", payload={"task": "render"}), priority=Priority.HIGH)

# Worker
job = await queue.dequeue()
try:
    await process(job)
    await queue.complete(job.id)
except Exception as e:
    await queue.fail(job, str(e))   # retries up to max_retries, then -> DLQ
```

Failed jobs retry at LOW priority so fresh work keeps precedence; after
`max_retries` they land in the DLQ, inspectable via `list_dlq()` and replayable
via `retry_dlq_job()`. Jobs can also be `schedule()`d for the future and
promoted by `process_scheduled()`.

## MQTT buffer

Buffer critical messages while the broker is down; flush on reconnect.

```python
from agent_resilience import ResilientMQTTPublisher

publisher = ResilientMQTTPublisher(mqtt_client)
publisher.start()
publisher.publish_critical("agents/heartbeat", {"id": "worker-1", "ok": True})
```

## Demo & tests

```bash
python -m examples.demo   # watch a breaker trip and recover, no services needed
pytest                    # state-machine tests
```

## License

MIT
