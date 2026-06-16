"""
Circuit breaker - per-dependency failure isolation for distributed agents.

A circuit breaker isolates a flaky dependency (an LLM endpoint, an HTTP API, a
database) so that repeated failures stop hammering it and fail fast instead.

States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED

    from agent_resilience import CircuitBreakerRegistry, CircuitBreakerOpen

    registry = CircuitBreakerRegistry(config={
        "default": {"failure_threshold": 5, "recovery_timeout_s": 30},
        "llm": {"failure_threshold": 3, "recovery_timeout_s": 60},
    })

    # Direct API
    cb = registry.get("llm")
    if cb.allow_request():
        try:
            result = call_llm(...)
            cb.record_success()
        except Exception:
            cb.record_failure()
            raise

    # Context manager (auto record_success / record_failure)
    with registry.guarded("llm"):
        result = call_llm(...)

    # Decorator (sync or async)
    @registry.protect("llm")
    async def call_llm(prompt): ...

Optional Redis persistence checkpoints state across restarts:
    RedisCircuitBreakerSync(registry, redis_url, prefix).
"""

from __future__ import annotations

import functools
import logging
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable, Dict, Iterator, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "CircuitState",
    "CircuitBreakerOpen",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "RedisCircuitBreakerSync",
]


class CircuitState(Enum):
    """States of a circuit breaker."""

    CLOSED = "closed"  # Requests pass through normally
    OPEN = "open"  # Requests are blocked, dependency considered down
    HALF_OPEN = "half_open"  # Probing: limited requests allowed to test recovery


class CircuitBreakerOpen(Exception):
    """Raised when a request is rejected because the circuit is OPEN."""

    def __init__(self, name: str, last_failure_time: float):
        self.name = name
        self.last_failure_time = last_failure_time
        super().__init__(f"Circuit breaker '{name}' is OPEN")


class CircuitBreaker:
    """Single circuit breaker tracking one dependency."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 30.0,
        half_open_max_calls: int = 1,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout_s
        self.half_open_max = half_open_max_calls
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: float = 0
        self.half_open_calls = 0
        self.total_trips = 0

    def allow_request(self) -> bool:
        """Return True if a request is allowed under the current state."""
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                logger.info("[CB:%s] OPEN -> HALF_OPEN", self.name)
                return True
            return False
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls < self.half_open_max:
                self.half_open_calls += 1
                return True
            return False
        return False

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            logger.info("[CB:%s] HALF_OPEN -> CLOSED", self.name)
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0
        self.success_count += 1

    def record_failure(self) -> None:
        """Record a failed call. May transition CLOSED -> OPEN or HALF_OPEN -> OPEN."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self.total_trips += 1
            logger.warning("[CB:%s] HALF_OPEN -> OPEN", self.name)
        elif self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.total_trips += 1
            logger.warning(
                "[CB:%s] CLOSED -> OPEN (%d/%d)", self.name, self.failure_count, self.failure_threshold
            )

    def reset(self) -> None:
        """Manual reset (admin action). Forces CLOSED, clears counters."""
        prev = self.state.value
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
        logger.info("[CB:%s] %s -> CLOSED (manual reset)", self.name, prev)

    def status(self) -> Dict[str, Any]:
        """Return current state for inspection / dashboard."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_trips": self.total_trips,
            "last_failure_time": self.last_failure_time,
        }


class CircuitBreakerRegistry:
    """
    Registry of circuit breakers by name. Lazily creates breakers using the
    `default` config when an unknown name is requested.

        {
            "default": {"failure_threshold": 5, "recovery_timeout_s": 30, "half_open_max_calls": 1},
            "specific_dep": {"failure_threshold": 3, "recovery_timeout_s": 60},
        }
    """

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self._breakers: Dict[str, CircuitBreaker] = {}
        default_cfg = config.get("default", {})
        self._default_threshold = default_cfg.get("failure_threshold", 5)
        self._default_timeout = default_cfg.get("recovery_timeout_s", 30)
        self._default_half_open = default_cfg.get("half_open_max_calls", 1)
        for name, cfg in config.items():
            if name == "default":
                continue
            self._breakers[name] = CircuitBreaker(
                name=name,
                failure_threshold=cfg.get("failure_threshold", self._default_threshold),
                recovery_timeout_s=cfg.get("recovery_timeout_s", self._default_timeout),
                half_open_max_calls=cfg.get("half_open_max_calls", self._default_half_open),
            )

    def get(self, name: str) -> CircuitBreaker:
        """Get or lazily create a breaker by name."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                failure_threshold=self._default_threshold,
                recovery_timeout_s=self._default_timeout,
                half_open_max_calls=self._default_half_open,
            )
        return self._breakers[name]

    def summary(self) -> Dict[str, Dict[str, Any]]:
        """Return status of all breakers (for a /health or /circuits endpoint)."""
        return {name: cb.status() for name, cb in self._breakers.items()}

    def any_open(self) -> bool:
        """True if any breaker is currently OPEN."""
        return any(cb.state == CircuitState.OPEN for cb in self._breakers.values())

    @contextmanager
    def guarded(self, name: str) -> Iterator[CircuitBreaker]:
        """
        Context manager: record_success on clean exit, record_failure on exception.
        Raises CircuitBreakerOpen if the breaker disallows the request.
        """
        cb = self.get(name)
        if not cb.allow_request():
            raise CircuitBreakerOpen(name, cb.last_failure_time)
        try:
            yield cb
            cb.record_success()
        except Exception:
            cb.record_failure()
            raise

    def protect(self, name: str) -> Callable:
        """Decorator wrapping a function (sync or async) with breaker protection."""

        def decorator(fn):
            import asyncio

            if asyncio.iscoroutinefunction(fn):

                @functools.wraps(fn)
                async def async_wrapper(*args, **kwargs):
                    cb = self.get(name)
                    if not cb.allow_request():
                        raise CircuitBreakerOpen(name, cb.last_failure_time)
                    try:
                        result = await fn(*args, **kwargs)
                        cb.record_success()
                        return result
                    except Exception:
                        cb.record_failure()
                        raise

                return async_wrapper

            @functools.wraps(fn)
            def sync_wrapper(*args, **kwargs):
                cb = self.get(name)
                if not cb.allow_request():
                    raise CircuitBreakerOpen(name, cb.last_failure_time)
                try:
                    result = fn(*args, **kwargs)
                    cb.record_success()
                    return result
                except Exception:
                    cb.record_failure()
                    raise

            return sync_wrapper

        return decorator

    def reset(self, name: str) -> bool:
        """Manual reset of one breaker. Returns False if unknown."""
        if name not in self._breakers:
            return False
        self._breakers[name].reset()
        return True

    def reset_all(self) -> int:
        """Manual reset of all breakers. Returns count reset."""
        for cb in self._breakers.values():
            cb.reset()
        return len(self._breakers)


# --------------------------------------------------------------------------- #
# Optional Redis state persistence
# --------------------------------------------------------------------------- #
try:
    import json as _json

    import redis as _redis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


class RedisCircuitBreakerSync:
    """
    Checkpoint breaker states to/from Redis for crash recovery and
    cross-process coherence.

        sync = RedisCircuitBreakerSync(registry, redis_url, prefix="cb:")
        sync.restore_all()   # at startup
        # ... agent runs ...
        sync.save_all()      # periodically or on shutdown
    """

    def __init__(
        self,
        registry: CircuitBreakerRegistry,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "cb:",
        ttl_seconds: int = 3600,
    ):
        self.registry = registry
        self.prefix = prefix
        self.ttl_seconds = ttl_seconds
        self._redis = None
        if HAS_REDIS:
            try:
                self._redis = _redis.from_url(redis_url, decode_responses=True)
                self._redis.ping()
                logger.info("[CB-Redis] connected (prefix=%s) - persistence enabled", prefix)
            except Exception as e:  # noqa: BLE001 - degrade to in-memory
                logger.warning("[CB-Redis] connection failed: %s - in-memory only", e)
                self._redis = None

    def save_all(self) -> int:
        """Persist all breaker states to Redis. Returns count saved."""
        if not self._redis:
            return 0
        saved = 0
        try:
            pipe = self._redis.pipeline()
            for name, cb in self.registry._breakers.items():
                data = _json.dumps(
                    {
                        "state": cb.state.value,
                        "failure_count": cb.failure_count,
                        "success_count": cb.success_count,
                        "last_failure_time": cb.last_failure_time,
                        "total_trips": cb.total_trips,
                    }
                )
                pipe.set(f"{self.prefix}{name}", data, ex=self.ttl_seconds)
                saved += 1
            pipe.execute()
        except Exception as e:  # noqa: BLE001
            logger.error("[CB-Redis] save error: %s", e)
        return saved

    def restore_all(self) -> int:
        """Restore breaker states from Redis after restart. Returns count restored."""
        if not self._redis:
            return 0
        restored = 0
        try:
            for key in self._redis.keys(f"{self.prefix}*"):
                name = key.replace(self.prefix, "")
                raw = self._redis.get(key)
                if not raw:
                    continue
                data = _json.loads(raw)
                cb = self.registry.get(name)
                cb.state = CircuitState(data.get("state", "closed"))
                cb.failure_count = data.get("failure_count", 0)
                cb.success_count = data.get("success_count", 0)
                cb.last_failure_time = data.get("last_failure_time", 0)
                cb.total_trips = data.get("total_trips", 0)
                restored += 1
            if restored:
                logger.info("[CB-Redis] restored %d breaker states", restored)
        except Exception as e:  # noqa: BLE001
            logger.error("[CB-Redis] restore error: %s", e)
        return restored

    @property
    def connected(self) -> bool:
        """True if the Redis connection is alive."""
        return self._redis is not None
