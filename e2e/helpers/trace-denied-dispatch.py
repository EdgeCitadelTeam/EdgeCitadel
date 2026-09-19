"""S4: real jim-eq denial, idempotent receipt, no child, exact Core evidence."""

import json
import platform
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.storage_pair import attach_task_snapshot
from edgecitadel_agentd.service import socket_path_for

assert platform.node().lower() == "jim-eq", "Run real E2E on jim-eq only"
ROOT = Path("/root/.edgecitadel/agentd")
LEAF = Path("/root/.edgecitadel-hermes-leaf/agentd")
CORE = Path("/root/.edgecitadel/core/data/openclaw.db")
output = Path(sys.argv[1])
assert output.is_absolute() and output.is_dir(), "Use an existing owned directory"


def read(path, sql, args):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        db.execute("BEGIN")
        if path.name == "agentd.sqlite3":
            attach_task_snapshot(db, path)
        return db.execute(sql, args).fetchall()
    finally:
        db.close()


def source_events(trace_id):
    return read(
        ROOT / "agentd.sqlite3",
        "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json "
        "FROM trace_journal WHERE trace_id=? ORDER BY source_seq",
        (trace_id,),
    )


admin = AgentdClient(
    socket_path_for(ROOT), admin_token=(ROOT / "admin.token").read_text().strip()
)
name = "trace-denial-" + uuid4().hex[:12]
registration = admin.call(
    "connector.register",
    connector_id=name,
    host_type="codex",
    agent_id=name,
    capabilities=["edgecitadel_trace"],
)
client = AgentdClient(
    socket_path_for(ROOT), connector_id=name, token=registration["token"]
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
    dispatch_id = str(uuid4())
    request = dict(
        schema_version=1,
        request_id=dispatch_id,
        binding_id=binding["binding_id"],
        recipient_id="jim-eq-hermes",
        request="This denied request must never execute.",
        skill_id=None,
        deadline_at_ms=int(time.time() * 1000) + 60000,
    )
    reply = client.call("trace.dispatch", **request)
    assert reply["status"] == "error" and reply["code"] == "not_authorized"
    before = source_events(trace_id)
    assert client.call("trace.dispatch", **request) == reply
    assert source_events(trace_id) == before, "retry emitted duplicate evidence"
    denied = [
        json.loads(row[-1])
        for row in before
        if json.loads(row[-1])["kind"] in {"permission", "dispatch"}
    ]
    assert {(e["kind"], e["phase"]) for e in denied} == {
        ("permission", "denied"),
        ("dispatch", "denied"),
    }
    assert len(denied) == 2
    for event in denied:
        assert event["attributes"]["dispatch_id"] == dispatch_id
        assert event["attributes"]["reason"] == "permission_denied"
        assert event["attributes"]["grant_version"]
    finished = client.call(
        "trace.finish",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        outcome="failed",
        reason="unknown",
    )
    assert finished["status"] == "ok"
    rows = source_events(trace_id)
    deadline = time.monotonic() + 120
    while True:
        central = []
        for row in rows:
            central.extend(
                read(
                    CORE,
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,"
                    "COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r "
                    "LEFT JOIN trace_payloads p USING(ingest_seq) "
                    "WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                    row[:3],
                )
            )
        positions = read(
            ROOT / "agentd.sqlite3",
            "SELECT p.node_id,p.source_epoch,p.export_generation,p.export_seq,p.event_id,p.event_sha256,p.state "
            "FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id "
            "AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
            (trace_id,),
        )
        mappings = []
        for position in positions:
            mappings.extend(
                read(
                    CORE,
                    "SELECT node_id,source_epoch,export_generation,export_seq,event_id,event_sha256 "
                    "FROM trace_ingest_positions WHERE node_id=? AND source_epoch=? "
                    "AND export_generation=? AND export_seq=?",
                    position[:4],
                )
            )
        if (
            sorted(rows) == sorted(central)
            and len(positions) == len(rows)
            and all(p[-1] == "core_settled" for p in positions)
            and sorted(p[:-1] for p in positions) == sorted(mappings)
        ):
            break
        assert time.monotonic() < deadline, "denied telemetry settlement timeout"
        time.sleep(0.5)
    for source in (ROOT, LEAF):
        assert not read(
            source / "agentd.sqlite3",
            "SELECT task_id FROM tasks WHERE trace_id=? OR sender_id=?",
            (trace_id, name),
        )
    assert {json.loads(row[-1])["kind"] for row in rows} == {
        "run",
        "permission",
        "dispatch",
    }
    report = dict(
        scenario="S4",
        trace_id=trace_id,
        dispatch_id=dispatch_id,
        denied_before_child_creation=True,
        no_tasks_on_core_or_leaf=True,
        retry_same_receipt=True,
        retry_no_new_events=True,
        event_count=len(rows),
        event_tuples_exact=True,
        export_mappings_exact=True,
        all_core_settled=True,
    )
finally:
    try:
        if session is not None:
            client.call("session.close", session_id=session)
    finally:
        admin.call("connector.revoke", connector_id=name)

report["session_closed_and_connector_revoked"] = True
(output / "denied-dispatch-result.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report), flush=True)
