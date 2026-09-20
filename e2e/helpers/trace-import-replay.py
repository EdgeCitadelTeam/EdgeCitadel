"""Import through the installed CLI and reconcile real jim-eq source/Core records."""

import json
import platform
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for

assert platform.node().lower() == "jim-eq", "Real E2E runs on jim-eq only"
directory = Path(sys.argv[1])
assert directory.is_absolute()
state = Path("/var/lib/edgecitadel-leaf/state")
source_id = "archive-e2e-" + uuid4().hex
client = AgentdClient(
    socket_path_for(state / "agentd"),
    admin_token=(state / "agentd/admin.token").read_text().strip(),
)


def connect(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def execution_ids():
    with connect(state / "agentd/agentd-tasks.sqlite3") as db:
        return {
            table: db.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in ("tasks", "transport_outbox")
        }


def import_cli(expected_error=None):
    command = [
        "runuser",
        "-u",
        "edgecitadel-leaf",
        "--",
        "env",
        "XDG_RUNTIME_DIR=/run/user/993",
        "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/993/bus",
        f"EDGECITADEL_SUPERVISOR_PYTHON={state}/supervisor/bin/python",
        "/opt/edgecitadel/quota-1a150dc/bin/edgecitadel",
        "trace",
        "import",
        str(directory / "archive.jsonl"),
        "--source-id",
        source_id,
        "--state-dir",
        str(state),
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=45)
    if expected_error:
        assert result.returncode == 1 and expected_error in result.stderr
        assert "Traceback" not in result.stderr
        return None
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


before = execution_ids()
try:
    first = import_cli()
    assert first == import_cli(), "Retry changed the imported identities"
    archive = directory / "archive.jsonl"
    original = archive.read_bytes()
    changed = [json.loads(line) for line in original.splitlines()]
    changed[0]["observation"]["duration_ms"] = 123
    try:
        archive.write_text("".join(json.dumps(row) + "\n" for row in changed))
        import_cli(expected_error="idempotency_conflict")
    finally:
        archive.write_bytes(original)
    assert first["records"] == 8 and len(first["trace_ids"]) == 1
    trace = first["trace_ids"][0]
    assert before == execution_ids(), (
        "Import changed executable task or transport state"
    )
    deadline = time.monotonic() + 90
    while True:
        with connect(state / "agentd/trace/agentd.sqlite3") as db:
            rows = db.execute(
                "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json "
                "FROM trace_journal WHERE trace_id=? ORDER BY source_seq",
                (trace,),
            ).fetchall()
            positions = db.execute(
                "SELECT p.node_id,p.source_epoch,p.export_generation,p.export_seq,"
                "p.event_id,p.event_sha256,p.state FROM trace_spool p JOIN trace_journal j "
                "ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch "
                "AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
                (trace,),
            ).fetchall()
        assert len(rows) == 8, "Retry duplicated canonical evidence"
        assert all(
            json.loads(row[5])["evidence_kind"] == "historical_import" for row in rows
        )
        with connect(Path("/root/.edgecitadel/core/data/openclaw.db")) as db:
            collected = [
                db.execute(
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,"
                    "COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r "
                    "LEFT JOIN trace_payloads p USING(ingest_seq) "
                    "WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                    row[:3],
                ).fetchone()
                for row in rows
            ]
            mappings = [
                db.execute(
                    "SELECT node_id,source_epoch,export_generation,export_seq,event_id,event_sha256 "
                    "FROM trace_ingest_positions WHERE node_id=? AND source_epoch=? "
                    "AND export_generation=? AND export_seq=?",
                    row[:4],
                ).fetchone()
                for row in positions
            ]
        if (
            rows == collected
            and len(positions) == 8
            and all(
                row[6] == "core_settled" and row[:6] == mapping
                for row, mapping in zip(positions, mappings)
            )
        ):
            break
        assert time.monotonic() < deadline, "Source/Core settlement timed out"
        time.sleep(0.5)
    report = {
        "target": "jim-eq",
        "trace_id": trace,
        "source_id": source_id,
        "records": 8,
        "retry_deduplicated": True,
        "conflicting_retry_rejected": True,
        "execution_unchanged": True,
        "exact_source_core_settlement": True,
    }
finally:
    client.call(
        "trace.import.configure",
        import_source_id=source_id,
        agent_id="archive-e2e",
        enabled=False,
    )
report["grant_revoked"] = True
(directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report))
