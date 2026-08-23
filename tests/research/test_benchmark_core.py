from pathlib import Path
import json

import httpx
from jsonschema import Draft202012Validator
import pytest

from scripts.research.benchmark_core import (
    BenchmarkRun,
    TrialResult,
    assert_stack_ready,
    register_envelope,
    render_markdown_summary,
    summarize,
    write_run,
)


def test_summarize_latency_percentiles():
    trials = [
        TrialResult(trial=i, task_id=f"task-{i}", task_latency_ms=float(i))
        for i in range(1, 6)
    ]
    summary = summarize(trials)
    assert summary["trial_count"] == 5
    assert summary["task_latency_ms"]["p50"] == 3.0
    assert summary["task_latency_ms"]["p95"] == 5.0
    assert summary["duplicates_seen"] == 0


def test_write_run_creates_json(tmp_path: Path):
    run = BenchmarkRun(
        run_id="unit",
        workload="E1",
        mode="A",
        started_at="2026-07-12T00:00:00.000Z",
        ended_at="2026-07-12T00:00:01.000Z",
        environment={"api_base": "http://localhost/api"},
        trials=[TrialResult(trial=1, task_id="task-1", task_latency_ms=10.0)],
    )
    out = write_run(run, tmp_path)
    data = json.loads(out.read_text())
    assert data["suite_version"] == 1
    assert data["workload"] == "E1"
    assert data["summary"]["task_latency_ms"]["p50"] == 10.0


def test_render_markdown_summary_outputs_compact_table():
    run = BenchmarkRun(
        run_id="unit",
        workload="E1",
        mode="A",
        started_at="2026-07-12T00:00:00.000Z",
        ended_at="2026-07-12T00:00:01.000Z",
        environment={},
        trials=[TrialResult(trial=1, task_id="task-1", task_latency_ms=10.0)],
    )

    md = render_markdown_summary(run)

    assert "| Workload | Trials | p50 task ms |" in md
    assert "| E1 | 1 | 10.0 | 10.0 | 10.0 | 0 | 0 |" in md


def test_register_envelope_defaults_to_schema_valid_l1_conformance():
    envelope = register_envelope(
        "benchmark-agent",
        metadata={
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.heartbeat_interval_sec": 30,
        },
    )
    schema_path = Path(__file__).parents[2] / "schemas" / "agent-card.v1.json"
    validator = Draft202012Validator(json.loads(schema_path.read_text()))

    assert envelope["payload"]["metadata"]["runtime.conformance"] == "L1"
    validator.validate(envelope["payload"])


def test_register_envelope_preserves_explicit_conformance():
    envelope = register_envelope(
        "benchmark-agent",
        metadata={
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L2",
            "runtime.heartbeat_interval_sec": 30,
        },
    )

    assert envelope["payload"]["metadata"]["runtime.conformance"] == "L2"


def test_register_envelope_does_not_mutate_metadata():
    metadata = {
        "runtime.kind": "native",
        "runtime.roles": ["worker"],
        "runtime.heartbeat_interval_sec": 30,
    }

    register_envelope("benchmark-agent", metadata=metadata)

    assert metadata == {
        "runtime.kind": "native",
        "runtime.roles": ["worker"],
        "runtime.heartbeat_interval_sec": 30,
    }


@pytest.mark.asyncio
async def test_assert_stack_ready_wraps_connection_errors(monkeypatch):
    class BrokenClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url: str):
            request = httpx.Request("GET", url)
            raise httpx.ConnectError("boom", request=request)

    monkeypatch.setattr("scripts.research.benchmark_core.httpx.AsyncClient", BrokenClient)

    with pytest.raises(RuntimeError, match="stack preflight failed"):
        await assert_stack_ready("http://localhost/api")
