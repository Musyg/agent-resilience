"""Demo trace contracts; the real export uses the wall clock, not this fake."""

import copy
import json
from itertools import count
from types import SimpleNamespace

import pytest

import agent_resilience.circuit_breaker as cb_module
from examples import visual_demo


@pytest.fixture
def trace(monkeypatch):
    clock = [100.0]

    def sleep(seconds):
        clock[0] += seconds

    fake_time = SimpleNamespace(time=lambda: clock[0], perf_counter=lambda: clock[0], sleep=sleep)
    monkeypatch.setattr(cb_module, "time", fake_time)
    monkeypatch.setattr(visual_demo, "time", fake_time)
    return visual_demo.run_scenario()


def test_observed_probe_and_final_counters(trace):
    visual_demo.validate_trace(trace)
    probe = next(event for event in trace["events"] if event["phase"] == "probe")
    assert probe["state"] == "half_open"
    assert probe["upstream_calls"] == 4  # probe has not called upstream yet
    assert trace["events"][-1]["breaker"]["failure_count"] == 0
    assert trace["events"][-1]["breaker"]["success_count"] == 3


def test_circuit_trace_preserves_submillisecond_elapsed_time(monkeypatch):
    clock = [100.0]
    ticks = count()

    def sleep(seconds):
        clock[0] += seconds

    fake_time = SimpleNamespace(
        time=lambda: clock[0],
        perf_counter=lambda: clock[0] + next(ticks) * 0.000123,
        sleep=sleep,
    )
    monkeypatch.setattr(cb_module, "time", fake_time)
    monkeypatch.setattr(visual_demo, "time", fake_time)
    events = visual_demo.run_scenario()["events"]
    assert events[0]["elapsed_s"] == 0.000123
    assert events[1]["elapsed_s"] == 0.000246


@pytest.mark.parametrize("defect", ["blocked_call", "missing_probe", "early_probe", "wrong_state"])
def test_rejects_incorrect_trace(trace, defect):
    broken = copy.deepcopy(trace)
    if defect == "blocked_call":
        next(event for event in broken["events"] if event["outcome"] == "blocked")["upstream_calls"] += 1
    elif defect == "missing_probe":
        broken["events"] = [event for event in broken["events"] if event["phase"] != "probe"]
    elif defect == "early_probe":
        next(event for event in broken["events"] if event["phase"] == "probe")["elapsed_s"] = 1
    else:
        broken["events"][-1]["state"] = "open"
    with pytest.raises(ValueError):
        visual_demo.validate_trace(broken)


def test_html_embeds_trace_without_script_escape(trace):
    trace["scenario"] = "</script><script>alert(1)</script>"
    html = visual_demo.render_html(trace)
    payload = html.split('<script id="recorded-trace" type="application/json">')[1].split("</script>")[0]
    assert json.loads(payload) == trace
    assert "<" not in payload
    assert "__RECORDED_TRACE__" not in html
    assert "#005EFF" in html


def test_cli_exports_reproducible_files(trace, tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["visual_demo", "--scenario", "circuit", "--output", str(tmp_path)])
    monkeypatch.setattr(visual_demo, "run_scenario", lambda: trace)
    visual_demo.main()
    assert json.loads((tmp_path / "trace.json").read_text(encoding="utf-8")) == trace
    assert (tmp_path / "index.html").read_text(encoding="utf-8") == visual_demo.render_html(trace)
    for name in ["index.html", "trace.json"]:
        assert b"\r\n" not in (tmp_path / name).read_bytes()
