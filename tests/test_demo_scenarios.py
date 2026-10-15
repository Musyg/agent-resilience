"""Contracts for recorded DLQ/MQTT behavior, including corrupt-trace controls."""

import asyncio
import copy
from itertools import count
from types import SimpleNamespace

import pytest

from examples import demo_scenarios
from examples.demo_scenarios import (
    MemoryRedis,
    run_dlq_scenario,
    run_mqtt_scenario,
    validate_dlq_trace,
    validate_mqtt_trace,
)


@pytest.fixture(scope="module")
def dlq_trace():
    return asyncio.run(run_dlq_scenario())


@pytest.fixture(scope="module")
def mqtt_trace():
    return run_mqtt_scenario()


@pytest.mark.parametrize("scenario", ["dlq", "mqtt"])
def test_trace_preserves_submillisecond_elapsed_time(monkeypatch, scenario):
    ticks = count()
    monkeypatch.setattr(demo_scenarios, "time", SimpleNamespace(perf_counter=lambda: next(ticks) * 0.000123))
    trace = asyncio.run(run_dlq_scenario()) if scenario == "dlq" else run_mqtt_scenario()
    assert [event["elapsed_s"] for event in trace["events"]] == [
        round(index * 0.000123, 6) for index in range(1, 10)
    ]


def test_serial_adapter_orders_scores_and_does_not_invent_jobs():
    async def exercise():
        store = MemoryRedis()
        await store.zadd("queue", {"low": -10, "high": -100, "normal": -50})
        assert await store.zrange("queue", 0, -1) == ["high", "normal", "low"]
        assert await store.zpopmin("queue") == [("high", -100)]
        assert await store.zcard("queue") == 2
        await store.lpush("dlq", "one")
        await store.lpush("dlq", "two")
        assert await store.lrange("dlq", 0, -1) == ["two", "one"]
        assert await store.lrem("dlq", 1, "one") == 1
        assert await store.lrange("dlq", 0, 0) == ["two"]

    asyncio.run(exercise())


def test_dlq_retains_same_task_then_resets_error_and_completes(dlq_trace):
    validate_dlq_trace(dlq_trace)
    events = dlq_trace["events"]
    assert events[4]["job"]["error"] == "local worker unavailable"
    assert events[6]["job"]["error"] is None
    assert events[6]["retry_count"] == 0
    assert events[0]["job"]["created_at"] == events[-1]["job"]["created_at"]
    assert events[-1]["worker_calls"] == 3


@pytest.mark.parametrize("defect", ["lost_payload", "wrong_sizes", "no_reset", "wrong_state"])
def test_dlq_rejects_incorrect_observations(dlq_trace, defect):
    broken = copy.deepcopy(dlq_trace)
    if defect == "lost_payload":
        broken["events"][-1]["job"]["payload"] = {}
    elif defect == "wrong_sizes":
        broken["events"][4]["dlq_size"] = 0
    elif defect == "no_reset":
        broken["events"][6]["retry_count"] = 2
    else:
        broken["events"][-1]["state"] = "processing"
    with pytest.raises(ValueError):
        validate_dlq_trace(broken)


def test_mqtt_real_background_flusher_preserves_order_and_drains(mqtt_trace):
    validate_mqtt_trace(mqtt_trace)
    events = mqtt_trace["events"]
    assert [event["published_count"] for event in events[:5]] == [0] * 5
    assert [event["published_count"] for event in events[5:8]] == [1, 2, 3]
    assert events[-1]["messages"] == [
        {"topic": "demo/events", "payload": {"sequence": n}, "qos": 1, "retain": False} for n in [1, 2, 3]
    ]


@pytest.mark.parametrize("defect", ["lost_message", "wrong_order", "wrong_count", "not_drained"])
def test_mqtt_rejects_incorrect_observations(mqtt_trace, defect):
    broken = copy.deepcopy(mqtt_trace)
    if defect == "lost_message":
        broken["events"][-1]["messages"].pop()
    elif defect == "wrong_order":
        broken["events"][-1]["messages"].reverse()
    elif defect == "wrong_count":
        broken["events"][-1]["published_count"] = 4
    else:
        broken["events"][-1]["buffered"] = 1
    with pytest.raises(ValueError):
        validate_mqtt_trace(broken)
