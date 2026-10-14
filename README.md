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

## Install

The project is not currently distributed through PyPI. Install the tagged
release directly from GitHub:

```bash
python -m pip install "agent-resilience @ https://github.com/Musyg/agent-resilience/archive/refs/tags/v0.1.0.zip"
python -m pip install "agent-resilience[redis] @ https://github.com/Musyg/agent-resilience/archive/refs/tags/v0.1.0.zip"
python -m pip install "agent-resilience[mqtt] @ https://github.com/Musyg/agent-resilience/archive/refs/tags/v0.1.0.zip"
```

For development, clone the repository and run
`python -m pip install -e ".[dev]"`.

## Circuit breaker

Stop hammering a failing dependency and fail fast instead.

```python
from agent_resilience import CircuitBreakerRegistry, CircuitBreakerOpen

registry = CircuitBreakerRegistry(
    config={
        "default": {"failure_threshold": 5, "recovery_timeout_s": 30},
        "llm": {"failure_threshold": 3, "recovery_timeout_s": 60},
    }
)

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
    await queue.fail(job, str(e))  # retries up to max_retries, then -> DLQ
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

### Failure and recovery, in the browser

[Open the interactive demo](https://musyg.com/demos/agent-resilience/?lang=en)
([French](https://musyg.com/demos/agent-resilience/?lang=fr)).

From a clone of this repository, run:

```bash
python -m examples.visual_demo --output demo-output
```

Open `demo-output/index.html`. The self-contained replay has French and English
captions, pause, step and restart controls, and the full recorded trace.

The scenario executes the library's circuit breaker against a local simulated
dependency. Three failures open the circuit; two further requests are rejected
without calling upstream. After the two-second recovery timeout, a successful
`HALF_OPEN` probe closes the circuit. The final request passes normally:
**8 attempts, 6 upstream calls, 2 blocked calls**.

Two more scenarios cover the other components:

- **DLQ**: one task fails twice, moves to the dead-letter queue, is inspected,
  re-enqueued with its error counter reset, and completes on its third attempt.
- **MQTT buffer**: three messages are retained while the client is offline. The
  library's background flusher transmits them in order after reconnection.

The queue uses an in-memory Redis adapter and MQTT uses a local client adapter.
The library components execute unchanged; no external service is needed. The
JSON trace preserves the task payload, queue sizes and observed message sequence.
Use `--scenario circuit` to export only the original circuit breaker scenario.

`demo-output/trace.json` contains observations from the three Python scenarios. The
browser replays them at a readable pace; it does not run a backend or contact an
external service. No Redis, MQTT broker, API key or third-party script is needed.
See [the scenario](examples/visual_demo.py) and [the replay template](examples/visual_demo.html).

### Console example and checks

```bash
python -m examples.demo   # watch a breaker trip and recover, no services needed
pytest -q                 # circuit breaker, DLQ, and MQTT buffer unit tests
python -m build           # build source and wheel distributions
```

## License

MIT
