"""Real Leaf exporter SIGKILL boundaries against the production Core payload layout."""

import asyncio
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_plugin_runtime.telemetry_stream import (
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)


@pytest.mark.parametrize(
    "point",
    [
        "before_publish",
        "broker_ack_before_checkpoint",
        "after_checkpoint",
        "core_commit_before_ack",
        "settlement_before_retirement",
    ],
)
async def test_leaf_sigkill_recovers_exact_evidence(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    point,
    record_property,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.tests.test_trace_export_crash import (
        kill_child,
        prepare_core,
        recover_and_assert,
    )

    core_js, source_js = topology["core"].jetstream(), topology["a"].jetstream()
    await ensure_telemetry_stream(core_js)
    await ensure_telemetry_consumer(core_js)
    source_path, core_path = tmp_path / "source.db", tmp_path / "core.db"
    core_url, core_token = topology.endpoints["core"]
    source_url, source_token = topology.endpoints["a"]
    event_id = str(uuid4())
    db = sqlite3.connect(core_path)
    try:
        prepare_core(db)
        await asyncio.to_thread(
            kill_child,
            {
                "source": str(source_path),
                "core": str(core_path),
                "source_url": source_url,
                "source_token": source_token,
                "core_url": core_url,
                "core_token": core_token,
                "point": point,
                "event_id": event_id,
            },
            tmp_path / "child.json",
        )
        await recover_and_assert(source_path, db, core_js, source_js, point, event_id)
        assert db.execute("SELECT event_json FROM trace_raw_events").fetchall() == [
            ("",)
        ]
        assert db.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 1
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert all(process.poll() is None for process in topology.processes)
        evidence = {
            "point": point,
            "signal": "SIGKILL",
            "core_journal_mode": db.execute("PRAGMA journal_mode").fetchone()[0],
            "exact_events": 1,
            "exact_payloads": 1,
            "source_settled_after_reopen": True,
            "broker_unacked_after_recovery": 0,
            "created_tasks": 0,
            "scope": "Real Leaf publication and Core durable; production persistence/export helpers, not full daemon process or remote settlement responder",
        }
        record_property("crash_evidence", json.dumps(evidence))
        print(json.dumps(evidence), flush=True)
    finally:
        db.close()
