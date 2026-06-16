"""Tests for the circuit breaker state machine."""

import time

from agent_resilience import CircuitBreakerOpen, CircuitBreakerRegistry, CircuitState


def test_closed_to_open_after_threshold():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 3, "recovery_timeout_s": 60}})
    cb = reg.get("dep")
    for _ in range(3):
        assert cb.allow_request()
        cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert not cb.allow_request()


def test_open_to_half_open_after_timeout():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 1, "recovery_timeout_s": 0.2}})
    cb = reg.get("dep")
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert not cb.allow_request()
    time.sleep(0.25)
    assert cb.allow_request()  # probe allowed
    assert cb.state == CircuitState.HALF_OPEN


def test_half_open_success_closes():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 1, "recovery_timeout_s": 0.1}})
    cb = reg.get("dep")
    cb.record_failure()
    time.sleep(0.15)
    cb.allow_request()  # -> HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitState.CLOSED


def test_half_open_failure_reopens():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 1, "recovery_timeout_s": 0.1}})
    cb = reg.get("dep")
    cb.record_failure()
    time.sleep(0.15)
    cb.allow_request()  # -> HALF_OPEN
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.total_trips == 2


def test_guarded_context_manager_records_failure():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 1, "recovery_timeout_s": 60}})
    try:
        with reg.guarded("dep"):
            raise ValueError("boom")
    except ValueError:
        pass
    assert reg.get("dep").state == CircuitState.OPEN
    try:
        with reg.guarded("dep"):
            pass
    except CircuitBreakerOpen as e:
        assert e.name == "dep"
    else:
        raise AssertionError("expected CircuitBreakerOpen")


def test_protect_decorator_sync():
    reg = CircuitBreakerRegistry(config={"dep": {"failure_threshold": 5, "recovery_timeout_s": 60}})

    @reg.protect("dep")
    def add(a, b):
        return a + b

    assert add(2, 3) == 5
    assert reg.get("dep").success_count == 1
