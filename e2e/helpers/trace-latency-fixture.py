"""Closed-loop synthetic pilot; not baseline traffic or actual tool execution."""

import hashlib
import json
import os
import platform
import signal
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for
from trace_render_receiver import RenderReceiver


def read(path, sql, args=()):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return db.execute(sql, args).fetchall()
    finally:
        db.close()


def main():
    if platform.node().lower() != "jim-eq":
        raise RuntimeError("jim-eq only")
    out = Path(sys.argv[1])
    if not out.is_absolute() or not out.is_dir():
        raise ValueError("private absolute output directory required")
    root = Path("/root/.edgecitadel/agentd")
    core = Path("/root/.edgecitadel/core/data/openclaw.db")
    admin = AgentdClient(
        socket_path_for(root), admin_token=(root / "admin.token").read_text().strip()
    )
    name = "trace-latency-pilot-" + uuid4().hex[:10]
    registered = admin.call(
        "connector.register",
        connector_id=name,
        host_type="codex",
        agent_id=name,
        capabilities=["edgecitadel_trace"],
    )
    client = AgentdClient(
        socket_path_for(root), connector_id=name, token=registered["token"]
    )

    def terminate(*_):
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, terminate)
    session = None
    receiver = None
    report = {"fixture": "closed_loop_synthetic_pilot_not_baseline_or_execution"}
    try:
        session = client.call("session.open", lease_seconds=300)["session_id"]
        response = client.call(
            "trace.bind",
            schema_version=1,
            request_id=str(uuid4()),
            session_id=session,
            task_id=None,
            context_id=None,
        )
        assert response["status"] == "ok"
        binding = response["result"]
        trace_id = binding["trace_id"]

        def rows():
            return read(
                root / "agentd.sqlite3",
                "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE trace_id=? ORDER BY source_seq",
                (trace_id,),
            )

        initial = json.loads(rows()[0][-1])
        receiver = RenderReceiver(capacity=10)
        config = {
            "scope": {
                "node_id": initial["node_id"],
                "source_epoch": initial["source_epoch"],
                "trace_id": trace_id,
            },
            "receiver": {"url": receiver.url, "token": receiver.token},
            "samples": 10,
        }
        with os.fdopen(
            os.open(out / "scope.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
            "w",
        ) as output:
            json.dump(config, output)
        print(json.dumps({"stage": "scope_ready", "trace_id": trace_id}), flush=True)
        if not receiver.ready.wait(150):
            raise TimeoutError("browser readiness timeout")
        for _ in range(5):
            span = str(uuid4())
            parts = [
                initial["node_id"],
                initial["source_epoch"],
                trace_id,
                initial["execution_attempt_id"] or "",
                span,
            ]
            identity = (
                "span:"
                + hashlib.sha256(
                    json.dumps(parts, separators=(",", ":")).encode()
                ).hexdigest()
            )
            for phase, state in [("started", "running"), ("finished", "finished")]:
                response = client.call(
                    "trace.append",
                    schema_version=1,
                    binding_id=binding["binding_id"],
                    observation_id=str(uuid4()),
                    observation={
                        "schema_version": 1,
                        "kind": "tool",
                        "phase": phase,
                        "span_id": span,
                        "parent_span_id": None,
                        "occurred_at": datetime.now(UTC)
                        .isoformat(timespec="milliseconds")
                        .replace("+00:00", "Z"),
                        "duration_ms": None,
                        "attributes": {"name": "synthetic.latency.pilot"},
                    },
                )
                assert response["status"] == "ok"
                event = json.loads(rows()[-1][-1])
                assert event["span_id"] == span and event["phase"] == phase
                receiver.expect(event["event_id"], identity, state)
                receiver.wait(event["event_id"], 30)
        response = client.call(
            "trace.finish",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=binding["binding_id"],
            outcome="completed",
            reason="unknown",
        )
        assert response["status"] == "ok"
        source = rows()
        assert len(source) == 12
        deadline = time.monotonic() + 60
        while True:
            central = []
            for row in source:
                central.extend(
                    read(
                        core,
                        "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                        row[:3],
                    )
                )
            states = read(
                root / "agentd.sqlite3",
                "SELECT p.state FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
                (trace_id,),
            )
            if (
                sorted(source) == sorted(central)
                and len(states) == 12
                and all(s == ("core_settled",) for s in states)
            ):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("exact settlement timeout")
            time.sleep(0.25)
        report.update(
            trace_id=trace_id,
            source_core_exact=True,
            all_core_settled=True,
            event_count=12,
        )
    finally:
        try:
            if receiver is not None:
                report["render"] = receiver.report()
                receiver.close()
        finally:
            try:
                if session is not None:
                    client.call("session.close", session_id=session)
            finally:
                admin.call("connector.revoke", connector_id=name)
                report["owned_connector_revoked"] = True
                (out / "fixture.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"stage": "complete", "event_count": 12}), flush=True)


if __name__ == "__main__":
    main()
