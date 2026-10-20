"""Paired experiment contracts; patched clocks below are unit-test only."""

import copy
from types import SimpleNamespace

import pytest

import agent_resilience.circuit_breaker as cb_module
from examples import demo_comparison, visual_demo


@pytest.fixture
def paired(monkeypatch):
    clock = [100.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds

    fake_time = SimpleNamespace(time=lambda: clock[0], perf_counter=lambda: clock[0], sleep=sleep)
    for module in [cb_module, visual_demo, demo_comparison]:
        monkeypatch.setattr(module, "time", fake_time)
    schedule = demo_comparison.circuit_input_schedule()
    protected = visual_demo.run_scenario(schedule, "test-outage")
    baseline = demo_comparison.run_baseline(schedule, "test-outage")
    comparison = demo_comparison.build_comparison(protected, baseline)
    return comparison, protected, baseline, waits


def test_comparison_records_both_runs_with_the_same_schedule(paired):
    comparison, protected, baseline, waits = paired
    assert waits == [2.05, 2.05]
    assert comparison["protected"]["events"] is protected["events"]
    assert protected["input_schedule"] == baseline["input_schedule"] == comparison["input_schedule"]
    assert comparison["baseline"]["summary"] == {
        "attempts": 8,
        "upstream_calls": 8,
        "failures": 5,
        "successes": 3,
        "blocked": 0,
    }
    assert comparison["protected"]["summary"] == {
        "attempts": 8,
        "upstream_calls": 6,
        "failures": 3,
        "successes": 3,
        "blocked": 2,
    }


@pytest.mark.parametrize("defect", ["experiment", "schedule"])
def test_build_rejects_incomparable_runs(paired, defect):
    _, protected, baseline, _ = copy.deepcopy(paired)
    # Break shared object references so corruption affects only one run.
    baseline = copy.deepcopy(baseline)
    if defect == "experiment":
        baseline["experiment_id"] = "another-experiment"
    else:
        baseline["input_schedule"][0]["available"] = False
    with pytest.raises(ValueError):
        demo_comparison.build_comparison(protected, baseline)


@pytest.mark.parametrize(
    "defect",
    ["summary", "input", "counter", "wait", "outcome", "called", "time", "protected_copy", "missing"],
)
def test_validation_rejects_fabricated_or_corrupt_observations(paired, defect):
    comparison = copy.deepcopy(paired[0])
    protected = paired[1]
    events = comparison["baseline"]["events"]
    if defect == "summary":
        comparison["baseline"]["summary"]["failures"] = 0
    elif defect == "input":
        events[1]["available"] = True
    elif defect == "counter":
        events[1]["upstream_calls"] = 0
    elif defect == "wait":
        for event in events:
            event["elapsed_s"] = 0
    elif defect == "outcome":
        events[1]["outcome"] = "success"
        comparison["baseline"]["summary"] = demo_comparison.summarize(events)
    elif defect == "called":
        events[1]["called"] = 1
    elif defect == "time":
        events[0]["elapsed_s"] = float("nan")
    elif defect == "protected_copy":
        comparison["protected"]["events"][0]["available"] = False
    else:
        events.pop()
    with pytest.raises(ValueError):
        demo_comparison.validate_comparison(comparison, protected)


def test_provenance_reports_clock_environment_and_actual_file_hashes():
    provenance = demo_comparison.capture_provenance()
    assert provenance["clock"]["name"] == "perf_counter"
    assert provenance["clock"]["monotonic"] is True
    assert provenance["clock"]["resolution_s"] > 0
    assert provenance["python_version"]
    assert provenance["platform"]
    assert len(provenance["capture_sources_sha256"]) == 4
    assert all(len(digest) == 64 for digest in provenance["capture_sources_sha256"].values())
    assert len(provenance["source_revision"]) == 40
    assert isinstance(provenance["source_dirty"], bool)


def test_missing_git_does_not_invent_clean_revision(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("git not available")

    monkeypatch.setattr(demo_comparison.subprocess, "run", missing)
    provenance = demo_comparison.capture_provenance()
    assert provenance["source_revision"] is None
    assert provenance["source_dirty"] is None


@pytest.mark.parametrize("status,dirty", [("", False), (" M examples/visual_demo.py\n", True)])
def test_provenance_preserves_actual_clean_or_dirty_status(monkeypatch, status, dirty):
    def git_result(arguments, **kwargs):
        return SimpleNamespace(stdout=("a" * 40 if "rev-parse" in arguments else status))

    monkeypatch.setattr(demo_comparison.subprocess, "run", git_result)
    provenance = demo_comparison.capture_provenance()
    assert provenance["source_revision"] == "a" * 40
    assert provenance["source_dirty"] is dirty


def test_provenance_uses_only_public_environment_metadata(monkeypatch):
    def private_metadata():
        raise AssertionError("Do not collect hostname or full machine metadata")

    monkeypatch.setattr(demo_comparison.platform, "node", private_metadata)
    monkeypatch.setattr(demo_comparison.platform, "platform", private_metadata)
    monkeypatch.setattr(demo_comparison.platform, "system", lambda: "Windows")
    monkeypatch.setattr(demo_comparison.platform, "release", lambda: "11")
    monkeypatch.setattr(demo_comparison.platform, "machine", lambda: "AMD64")
    provenance = demo_comparison.capture_provenance()
    assert provenance["platform"] == "Windows 11 AMD64"
    assert set(provenance) == {
        "source_revision",
        "source_dirty",
        "capture_sources_sha256",
        "python_version",
        "python_implementation",
        "platform",
        "clock",
    }
    assert all(
        name.startswith("examples/") and ":" not in name for name in provenance["capture_sources_sha256"]
    )


def test_all_scenario_export_adds_comparison_without_replacing_scenarios(paired, monkeypatch):
    from examples import demo_scenarios

    _, protected, _, _ = paired
    monkeypatch.setattr(visual_demo, "run_scenario", lambda *args: protected)
    monkeypatch.setattr(demo_scenarios, "run_additional_scenarios", lambda: {"dlq": {}, "mqtt": {}})
    monkeypatch.setattr(demo_comparison, "capture_provenance", lambda: {"test": True})
    # Keep the generated experiment identity aligned with the supplied fixture.
    monkeypatch.setattr("uuid.uuid4", lambda: SimpleNamespace(hex="fixture"))
    protected["experiment_id"] = "circuit-outage-fixture"
    trace = visual_demo.run_all_scenarios()
    assert set(trace["scenarios"]) == {"circuit", "dlq", "mqtt"}
    assert trace["comparisons"]["circuit"]["protected"]["events"] == protected["events"]
    assert trace["provenance"] == {"test": True}
