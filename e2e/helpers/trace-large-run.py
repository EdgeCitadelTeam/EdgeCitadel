"""Synthetic large-run fixture on jim-eq; this does not prove tool execution."""

import json
import platform
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for

assert platform.node().lower() == "jim-eq", "Run real E2E on jim-eq only"
OUT = Path(sys.argv[1])
assert OUT.is_absolute() and OUT.is_dir()
ROOT = Path("/root/.edgecitadel/agentd")
CORE = Path("/root/.edgecitadel/core/data/openclaw.db")


def read(path, sql, args):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return db.execute(sql, args).fetchall()
    finally:
        db.close()


def rows(trace_id):
    return read(
        ROOT / "agentd.sqlite3",
        "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE trace_id=? ORDER BY source_seq",
        (trace_id,),
    )


admin = AgentdClient(
    socket_path_for(ROOT), admin_token=(ROOT / "admin.token").read_text().strip()
)
name = "trace-large-fixture-" + uuid4().hex[:10]
registered = admin.call(
    "connector.register",
    connector_id=name,
    host_type="codex",
    agent_id=name,
    capabilities=["edgecitadel_trace"],
)
client = AgentdClient(
    socket_path_for(ROOT), connector_id=name, token=registered["token"]
)
session = None
try:
    session = client.call("session.open", lease_seconds=300)["session_id"]
    bound = client.call(
        "trace.bind",
        schema_version=1,
        request_id=str(uuid4()),
        session_id=session,
        task_id=None,
        context_id=None,
    )
    assert bound["status"] == "ok"
    binding = bound["result"]
    trace_id = binding["trace_id"]

    def append(attributes, span_id=None, phase="started"):
        return client.call(
            "trace.append",
            schema_version=1,
            binding_id=binding["binding_id"],
            observation_id=str(uuid4()),
            observation={
                "schema_version": 1,
                "kind": "tool",
                "phase": phase,
                "span_id": span_id or str(uuid4()),
                "parent_span_id": None,
                "occurred_at": datetime.now(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "duration_ms": None,
                "attributes": attributes,
            },
        )

    spans = []
    started = time.monotonic()
    for index in range(600):
        span = str(uuid4())
        spans.append(span)
        attrs = {"name": "synthetic.large.operation"}
        for phase in ["started", "finished"]:
            reply = append(attrs, span, phase)
            assert reply["status"] == "ok", reply.get("code")
    assert (
        client.call(
            "trace.finish",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=binding["binding_id"],
            outcome="completed",
            reason="unknown",
        )["status"]
        == "ok"
    )
    source = rows(trace_id)
    assert len(source) == 1202
    deadline = time.monotonic() + 120
    while True:
        central = []
        for row in source:
            central.extend(
                read(
                    CORE,
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                    row[:3],
                )
            )
        states = read(
            ROOT / "agentd.sqlite3",
            "SELECT p.state FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
            (trace_id,),
        )
        if (
            sorted(source) == sorted(central)
            and len(states) == 1202
            and all(state == ("core_settled",) for state in states)
        ):
            break
        assert time.monotonic() < deadline, "large fixture settlement timeout"
        time.sleep(0.25)
    report = {
        "fixture": "synthetic_large_run_not_execution",
        "trace_id": trace_id,
        "operation_count": len(spans),
        "span_ids": spans,
        "emission_and_settlement_seconds": time.monotonic() - started,
        "source_core_exact": True,
        "all_core_settled": True,
        "event_count": len(source),
    }
finally:
    try:
        if session is not None:
            client.call("session.close", session_id=session)
    finally:
        admin.call("connector.revoke", connector_id=name)
report["owned_connector_revoked"] = True
(OUT / "result.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report), flush=True)
