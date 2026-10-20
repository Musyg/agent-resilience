"""Paired local executions driven by the same explicit outage schedule.

The comparison observes upstream calls and outcomes, not latency savings. Both
runs use local simulated dependencies and execute their own real recovery wait.
"""

from __future__ import annotations

import hashlib
import math
import platform
import subprocess
import time
from copy import deepcopy
from pathlib import Path


def circuit_input_schedule() -> list[dict]:
    phases = ["healthy", "failure", "failure", "failure", "isolation", "isolation", "recovery", "resumed"]
    return [
        {
            "input_id": index + 1,
            "phase": phase,
            "available": phase in {"healthy", "recovery", "resumed"},
            "wait_before_s": 2.05 if phase == "recovery" else 0,
        }
        for index, phase in enumerate(phases)
    ]


def run_baseline(input_schedule: list[dict], experiment_id: str) -> dict:
    from examples.visual_demo import LocalDependency

    dependency = LocalDependency()
    started = time.perf_counter()
    events = []
    for item in input_schedule:
        if item["wait_before_s"]:
            time.sleep(item["wait_before_s"])
        dependency.available = item["available"]
        try:
            dependency.call()
            outcome = "success"
        except RuntimeError:
            outcome = "failure"
        events.append(
            {
                "index": len(events),
                "input_id": item["input_id"],
                "elapsed_s": round(time.perf_counter() - started, 6),
                "phase": item["phase"],
                "state": "unprotected",
                "outcome": outcome,
                "attempts": len(events) + 1,
                "upstream_calls": dependency.calls,
                "blocked": 0,
                "called": True,
                "available": dependency.available,
            }
        )
    return {"experiment_id": experiment_id, "input_schedule": deepcopy(input_schedule), "events": events}


def summarize(events: list[dict]) -> dict:
    attempts = [event for event in events if event["outcome"] in {"success", "failure", "blocked"}]
    return {
        "attempts": len(attempts),
        "upstream_calls": sum(event["called"] for event in attempts),
        "failures": sum(event["outcome"] == "failure" for event in attempts),
        "successes": sum(event["outcome"] == "success" for event in attempts),
        "blocked": sum(event["outcome"] == "blocked" for event in attempts),
    }


def build_comparison(protected: dict, baseline: dict) -> dict:
    if not protected.get("experiment_id") or protected["experiment_id"] != baseline["experiment_id"]:
        raise ValueError("Comparison runs have different experiment identities")
    if protected["input_schedule"] != baseline["input_schedule"]:
        raise ValueError("Comparison runs used different inputs")
    comparison = {
        "experiment_id": protected["experiment_id"],
        "input_schedule": deepcopy(protected["input_schedule"]),
        "baseline": {"events": baseline["events"], "summary": summarize(baseline["events"])},
        "protected": {"events": protected["events"], "summary": summarize(protected["events"])},
    }
    validate_comparison(comparison, protected)
    return comparison


def validate_comparison(comparison: dict, protected_trace: dict) -> None:
    from examples.visual_demo import validate_trace

    validate_trace(protected_trace)
    schedule = comparison["input_schedule"]
    if schedule != circuit_input_schedule():
        raise ValueError("Unexpected comparison input schedule")
    if not comparison["experiment_id"] or comparison["experiment_id"] != protected_trace["experiment_id"]:
        raise ValueError("Comparison experiment differs from protected trace")
    if schedule != protected_trace["input_schedule"]:
        raise ValueError("Comparison schedule differs from protected trace")
    if comparison["protected"]["events"] != protected_trace["events"]:
        raise ValueError("Comparison must preserve the actual protected trace")
    for name in ["baseline", "protected"]:
        run = comparison[name]
        events = run["events"]
        attempts = [event for event in events if event["outcome"] in {"success", "failure", "blocked"}]
        if len(attempts) != len(schedule) or run["summary"] != summarize(events):
            raise ValueError("Comparison summary does not match observed attempts")
        previous = 0.0
        for index, event in enumerate(events):
            elapsed = event["elapsed_s"]
            if not math.isfinite(elapsed) or elapsed < previous or event["index"] != index:
                raise ValueError("Comparison event order or timestamps are invalid")
            previous = elapsed
        upstream = blocked = 0
        for number, (item, event) in enumerate(zip(schedule, attempts, strict=True), 1):
            if any(event[key] != item[key] for key in ["input_id", "phase", "available"]):
                raise ValueError("Comparison attempt did not receive the scheduled input")
            if not isinstance(event["called"], bool):
                raise ValueError("Upstream observation must be boolean")
            if name == "baseline" and not event["called"]:
                raise ValueError("Unprotected attempt did not call upstream")
            expected = ("success" if item["available"] else "failure") if event["called"] else "blocked"
            if event["outcome"] != expected:
                raise ValueError("Comparison outcome disagrees with the upstream observation")
            upstream += event["called"]
            blocked += event["outcome"] == "blocked"
            if (event["attempts"], event["upstream_calls"], event["blocked"]) != (number, upstream, blocked):
                raise ValueError("Comparison cumulative counters disagree with observations")
            if number > 1:
                actual_wait = event["elapsed_s"] - attempts[number - 2]["elapsed_s"]
                if actual_wait + 0.000002 < item["wait_before_s"]:
                    raise ValueError("Comparison recovery wait was not observed")


def capture_provenance() -> dict:
    root = Path(__file__).resolve().parents[1]

    def git(*arguments):
        return subprocess.run(
            ["git", "-C", str(root), *arguments], capture_output=True, text=True, check=True, timeout=5
        ).stdout.strip()

    try:
        revision = git("rev-parse", "HEAD")
        dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    except (OSError, subprocess.SubprocessError):
        revision = dirty = None
    clock = time.get_clock_info("perf_counter")
    return {
        "source_revision": revision,
        "source_dirty": dirty,
        "capture_sources_sha256": {
            f"examples/{name}": hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in ["visual_demo.py", "demo_scenarios.py", "demo_comparison.py", "visual_demo.html"]
        },
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        # Only OS and architecture: no hostname, user, path or network identity.
        "platform": " ".join([platform.system(), platform.release(), platform.machine()]),
        "clock": {
            "name": "perf_counter",
            "implementation": clock.implementation,
            "resolution_s": clock.resolution,
            "monotonic": clock.monotonic,
            "adjustable": clock.adjustable,
        },
    }
