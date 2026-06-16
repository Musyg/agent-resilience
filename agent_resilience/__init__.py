"""
agent-resilience - small, dependency-light resilience primitives for
distributed agent systems: a circuit breaker, a Redis-backed DLQ, and an
offline MQTT buffer.
"""

from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpen,
    CircuitBreakerRegistry,
    CircuitState,
    RedisCircuitBreakerSync,
)
from .dlq import DLQQueue, Job, Priority
from .mqtt_buffer import MQTTMessageBuffer, ResilientMQTTPublisher, create_resilient_publisher

__version__ = "0.1.0"

__all__ = [
    "CircuitState",
    "CircuitBreakerOpen",
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    "RedisCircuitBreakerSync",
    "Priority",
    "Job",
    "DLQQueue",
    "MQTTMessageBuffer",
    "ResilientMQTTPublisher",
    "create_resilient_publisher",
]
