"""
Runnable demo: a circuit breaker protecting a flaky dependency.

    python -m examples.demo

No external services required - the "dependency" is a fake that fails for a
while, then recovers, so you can watch the breaker trip and reset.
"""

import logging
import time

from agent_resilience import CircuitBreakerOpen, CircuitBreakerRegistry

logging.basicConfig(level=logging.INFO, format="%(message)s")


class FlakyService:
    """Fails its first `fail_until` calls, then succeeds."""

    def __init__(self, fail_until: int):
        self.calls = 0
        self.fail_until = fail_until

    def call(self) -> str:
        self.calls += 1
        if self.calls <= self.fail_until:
            raise RuntimeError(f"upstream error (call {self.calls})")
        return f"ok (call {self.calls})"


def main() -> None:
    registry = CircuitBreakerRegistry(config={"upstream": {"failure_threshold": 3, "recovery_timeout_s": 2}})
    service = FlakyService(fail_until=3)

    for i in range(12):
        try:
            with registry.guarded("upstream"):
                result = service.call()
            print(f"[{i:02d}] success: {result}")
        except CircuitBreakerOpen:
            print(f"[{i:02d}] short-circuited (breaker OPEN, not calling upstream)")
        except RuntimeError as e:
            print(f"[{i:02d}] failed: {e}")
        time.sleep(0.6)

    print("\nfinal breaker state:", registry.summary()["upstream"])


if __name__ == "__main__":
    main()
