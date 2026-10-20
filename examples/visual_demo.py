"""Execute three resilience scenarios and export their browser replay.

    python -m examples.visual_demo --output demo-output

Service boundaries are simulated locally; resilience logic is the library code.
The HTML replays recorded observations, without running Python in the browser.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

from agent_resilience import CircuitBreakerOpen, CircuitBreakerRegistry, __version__
from agent_resilience import circuit_breaker as source_module


class LocalDependency:
    """A local dependency whose availability we control for the scenario."""

    def __init__(self):
        self.available = True
        self.calls = 0

    def call(self) -> str:
        self.calls += 1
        if not self.available:
            raise RuntimeError("local dependency unavailable")
        return "ok"


def run_scenario(input_schedule=None, experiment_id=None) -> dict:
    """Observe guarded() before, during and after each sequential attempt."""
    from examples.demo_comparison import circuit_input_schedule

    input_schedule = circuit_input_schedule() if input_schedule is None else input_schedule
    registry = CircuitBreakerRegistry(config={"upstream": {"failure_threshold": 3, "recovery_timeout_s": 2}})
    dependency = LocalDependency()
    breaker = registry.get("upstream")
    # High-resolution on Windows too, unlike monotonic() on Python <= 3.12.
    started = time.perf_counter()
    events = []
    attempts = 0
    blocked = 0

    def record(phase: str, outcome: str, called: bool = False, input_id=None) -> None:
        events.append(
            {
                "index": len(events),
                "elapsed_s": round(time.perf_counter() - started, 6),
                "phase": phase,
                "outcome": outcome,
                "state": breaker.state.value,
                "attempts": attempts,
                "upstream_calls": dependency.calls,
                "blocked": blocked,
                "called": called,
                "available": dependency.available,
                "input_id": input_id,
                "breaker": breaker.status(),
            }
        )

    def attempt(phase: str, input_id: int) -> None:
        nonlocal attempts, blocked
        attempts += 1
        before = dependency.calls
        try:
            with registry.guarded("upstream"):
                # Capture HALF_OPEN before the probe result closes the breaker.
                if breaker.state.value == "half_open":
                    record("probe", "allowed", input_id=input_id)
                dependency.call()
            outcome = "success"
        except CircuitBreakerOpen:
            blocked += 1
            outcome = "blocked"
        except RuntimeError:
            outcome = "failure"
        record(phase, outcome, dependency.calls > before, input_id=input_id)

    record("initial", "ready")
    for item in input_schedule:
        if item["wait_before_s"]:
            record("cooldown", "waiting")
            # No manual reset, state assignment or patched clock in the real run.
            time.sleep(item["wait_before_s"])
        dependency.available = item["available"]
        attempt(item["phase"], item["input_id"])
    trace = {
        "schema_version": 1,
        "library_version": __version__,
        "source_sha256": hashlib.sha256(Path(source_module.__file__).read_bytes()).hexdigest(),
        "recorded_at": datetime.now(UTC).isoformat(),
        "scenario": "serial local dependency failure and recovery",
        "config": {"failure_threshold": 3, "recovery_timeout_s": 2},
        "events": events,
        "input_schedule": deepcopy(input_schedule),
        "experiment_id": experiment_id,
    }
    validate_trace(trace)
    return trace


def validate_trace(trace: dict) -> None:
    """Fail the export if the intended behavior was not actually observed."""
    events = trace["events"]
    attempts = [event for event in events if event["outcome"] in {"success", "failure", "blocked"}]
    if [event["outcome"] for event in attempts] != [
        "success",
        "failure",
        "failure",
        "failure",
        "blocked",
        "blocked",
        "success",
        "success",
    ]:
        raise ValueError("Unexpected attempt outcomes")
    if [event["state"] for event in attempts] != [
        "closed",
        "closed",
        "closed",
        "open",
        "open",
        "open",
        "closed",
        "closed",
    ]:
        raise ValueError("Unexpected breaker states")
    previous_calls = 0
    for event in attempts:
        expected_delta = 0 if event["outcome"] == "blocked" else 1
        if event["upstream_calls"] - previous_calls != expected_delta:
            raise ValueError("Upstream call count does not match the outcome")
        if event["called"] != (expected_delta == 1):
            raise ValueError("Upstream call observation does not match the outcome")
        previous_calls = event["upstream_calls"]
    probes = [event for event in events if event["phase"] == "probe"]
    if len(probes) != 1 or probes[0]["state"] != "half_open":
        raise ValueError("Expected one observed HALF_OPEN probe")
    trip = attempts[3]
    if probes[0]["elapsed_s"] - trip["elapsed_s"] < trace["config"]["recovery_timeout_s"]:
        raise ValueError("Probe happened before the recovery timeout")
    last = events[-1]
    if (last["attempts"], last["upstream_calls"], last["blocked"], last["breaker"]["total_trips"]) != (
        8,
        6,
        2,
        1,
    ):
        raise ValueError("Unexpected final counters")


def render_html(trace: dict) -> str:
    template = Path(__file__).with_name("visual_demo.html").read_text(encoding="utf-8")
    if template.count("__RECORDED_TRACE__") != 1:
        raise ValueError("Expected exactly one trace placeholder")
    # Prevent an embedded JSON string from terminating the script element.
    payload = json.dumps(trace, ensure_ascii=True).replace("<", "\\u003c")
    return template.replace("__RECORDED_TRACE__", payload)


def run_all_scenarios() -> dict:
    from uuid import uuid4

    from examples.demo_comparison import (
        build_comparison,
        capture_provenance,
        circuit_input_schedule,
        run_baseline,
    )
    from examples.demo_scenarios import run_additional_scenarios

    schedule = circuit_input_schedule()
    experiment_id = f"circuit-outage-{uuid4().hex}"
    circuit = run_scenario(schedule, experiment_id)
    baseline = run_baseline(schedule, experiment_id)
    comparison = build_comparison(circuit, baseline)
    additional = run_additional_scenarios()
    return {
        "schema_version": 2,
        "library_version": __version__,
        "recorded_at": datetime.now(UTC).isoformat(),
        "scenarios": {"circuit": circuit, **additional},
        "comparisons": {"circuit": comparison},
        "provenance": capture_provenance(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("demo-output"))
    parser.add_argument("--scenario", choices=["all", "circuit"], default="all")
    args = parser.parse_args()
    trace = run_scenario() if args.scenario == "circuit" else run_all_scenarios()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "trace.json").write_text(
        json.dumps(trace, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    (args.output / "index.html").write_text(render_html(trace), encoding="utf-8", newline="\n")
    scenarios = trace.get("scenarios", {"circuit": trace})
    for name, scenario in scenarios.items():
        print(f"\n{name}: {len(scenario['events'])} observed events")
        for event in scenario["events"]:
            print(f"{event['elapsed_s'] * 1000:10.3f} ms {event['state']:10} {event['outcome']}")
    print(f"Open {args.output / 'index.html'} in a browser.")


if __name__ == "__main__":
    main()
