"""Bounded live Hermes task and exact source/Core telemetry reconciliation."""

import json
import platform
import sqlite3
import sys
import time
from pathlib import Path
from uuid import uuid4
from urllib.request import Request, urlopen

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for

assert platform.node().lower() == "jim-eq", "Run real E2E on jim-eq only"
ROOT = Path("/root/.edgecitadel/agentd")
LEAF = Path("/root/.edgecitadel-hermes-leaf/agentd")
OUTPUT_DIRECTORY = Path(sys.argv[1])
assert OUTPUT_DIRECTORY.is_absolute(), "Use an absolute owned output directory"
OUT = OUTPUT_DIRECTORY / "live-task-result.json"
CORE = Path("/root/.edgecitadel/core/data/openclaw.db")
OUTAGE = sys.argv[2:] == ["--collector-outage"]
assert not sys.argv[2:] or OUTAGE, "Unknown scenario option"
outage_started = False


def telemetry(action=None):
    if action is None:
        request = Request("http://127.0.0.1/api/system/status")
    else:
        token = next(
            line.split("=", 1)[1]
            for line in Path("/root/.edgecitadel/core/.env").read_text().splitlines()
            if line.startswith("EDGECITADEL_ADMIN_TOKEN=")
        )
        request = Request(
            "http://127.0.0.1/api/system/telemetry/control",
            data=json.dumps({"action": action}).encode(),
            headers={
                "Content-Type": "application/json",
                "X-EdgeCitadel-Admin-Token": token,
            },
        )
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def wait_marker(name):
    deadline = time.monotonic() + 45
    while not (OUTPUT_DIRECTORY / name).exists():
        assert time.monotonic() < deadline, "browser handshake timeout"
        time.sleep(0.1)


def connection(path):
    return sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)


def inspect(trace_id):
    rows, positions, core, mappings = [], [], [], []
    for directory in (ROOT, LEAF):
        with connection(directory / "agentd.sqlite3") as db:
            rows.extend(
                db.execute(
                    "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE trace_id=?",
                    (trace_id,),
                ).fetchall()
            )
            positions.extend(
                db.execute(
                    "SELECT p.node_id,p.source_epoch,p.export_generation,p.export_seq,p.event_id,p.event_sha256,p.state FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
                    (trace_id,),
                ).fetchall()
            )
    with connection(CORE) as db:
        for row in rows:
            got = db.execute(
                "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                row[:3],
            ).fetchone()
            if got is not None:
                core.append(got)
        for row in positions:
            got = db.execute(
                "SELECT node_id,source_epoch,export_generation,export_seq,event_id,event_sha256 FROM trace_ingest_positions WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
                row[:4],
            ).fetchone()
            if got is not None:
                mappings.append(got)
    return rows, positions, core, mappings


admin = AgentdClient(
    socket_path_for(ROOT), admin_token=(ROOT / "admin.token").read_text().strip()
)
name = "trace-e2e-" + uuid4().hex[:12]
registration = admin.call(
    "connector.register",
    connector_id=name,
    host_type="codex",
    agent_id=name,
    capabilities=[
        "edgecitadel_trace",
        "edgecitadel_delegate",
        "edgecitadel_task_status",
    ],
)
client = AgentdClient(
    socket_path_for(ROOT), connector_id=name, token=registration["token"]
)
session = client.call("session.open", lease_seconds=300)["session_id"]
try:
    deadline = time.monotonic() + 30
    while client.call("health")["transport"].get("ready_inbox_count", 0) < 1:
        assert time.monotonic() < deadline, "inbox readiness timeout"
        time.sleep(0.5)
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
    print(json.dumps({"stage": "bound", "trace_id": binding["trace_id"]}), flush=True)
    ready = OUTPUT_DIRECTORY / "client-ready"
    deadline = time.monotonic() + 60
    while not ready.exists():
        assert time.monotonic() < deadline, "client readiness timeout"
        time.sleep(0.1)
    if OUTAGE:
        health = telemetry()
        assert health["telemetry"]["state"] == "running"
        assert health["nats_connected"] and health["jetstream_stream_ok"]
        outage_started = True
        assert telemetry("stop")["state"] == "stopped"
        print(json.dumps({"stage": "collector_stopped"}), flush=True)
        wait_marker("outage-observed")
    marker = "JIM_EQ_TRACE_ACK_" + uuid4().hex[:12]
    request = dict(
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        recipient_id="jim-eq-hermes",
        request="Reply with exactly " + marker + ". Do not call tools or delegate.",
        skill_id=None,
        deadline_at_ms=int(time.time() * 1000) + 120000,
    )
    started = time.monotonic()
    reply = client.call("trace.dispatch", **request)
    assert reply["status"] == "ok"
    assert client.call("trace.dispatch", **request) == reply
    task_id = reply["result"]["task_id"]
    print(
        json.dumps(
            {"stage": "dispatched", "task_id": task_id, "trace_id": binding["trace_id"]}
        ),
        flush=True,
    )
    deadline = time.monotonic() + 130
    while True:
        task = client.call("task.get", task_id=task_id)
        if task["state"] in [
            "completed",
            "failed",
            "rejected",
            "cancelled",
            "expired",
            "undeliverable",
        ]:
            break
        assert time.monotonic() < deadline, "live task completion timeout"
        time.sleep(0.5)
    assert task["state"] == "completed", task["state"]
    assert task["result"]["body"].strip() == marker, "unexpected acknowledgment"
    finished = client.call(
        "trace.finish",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        outcome="completed",
        reason="unknown",
    )
    assert finished["status"] == "ok"
    elapsed = time.monotonic() - started
    if OUTAGE:
        health = telemetry()
        assert health["telemetry"]["state"] == "stopped"
        assert health["nats_connected"] and health["jetstream_stream_ok"]
        pending_rows, _, collected_rows, _ = inspect(binding["trace_id"])
        missing = len(pending_rows) - len(collected_rows)
        assert missing > 0, "expected uncollected execution evidence"
        print(
            json.dumps(
                {
                    "stage": "completed_while_offline",
                    "task_id": task_id,
                    "uncollected_events": missing,
                }
            ),
            flush=True,
        )
        wait_marker("resume-collector")
        telemetry("start")
    deadline = time.monotonic() + 120
    while True:
        rows, positions, core, mappings = inspect(binding["trace_id"])
        if (
            rows
            and sorted(rows) == sorted(core)
            and all(p[-1] == "core_settled" for p in positions)
            and sorted(p[:-1] for p in positions) == sorted(mappings)
        ):
            break
        assert time.monotonic() < deadline, "exact live telemetry settlement timeout"
        time.sleep(0.5)
    assert len({r[0] for r in rows}) == 2, "expected both live sources"
    report = dict(
        scenario="S6" if OUTAGE else "live_task",
        execution_completed_with_collector_stopped=OUTAGE,
        task_id=task_id,
        trace_id=binding["trace_id"],
        task_state=task["state"],
        acknowledgment_exact=True,
        dispatch_retry_same_result=True,
        task_latency_seconds=elapsed,
        event_count=len(rows),
        export_positions=len(positions),
        source_nodes=sorted({r[0] for r in rows}),
        event_tuples_exact=True,
        export_mappings_exact=True,
        all_core_settled=True,
        event_families=sorted({json.loads(r[-1])["kind"] for r in rows}),
    )
    OUT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)
finally:
    try:
        if outage_started:
            telemetry("start")
    finally:
        try:
            client.call("session.close", session_id=session)
        finally:
            admin.call("connector.revoke", connector_id=name)
