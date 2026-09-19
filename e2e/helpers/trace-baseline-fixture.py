"""Ten synthetic agents on existing jim-eq; warmup and fixed-rate measured runs."""

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
from trace_latency_workload import baseline_slot
from trace_render_receiver import RenderReceiver


ROOT = Path("/root/.edgecitadel/agentd")
CORE = Path("/root/.edgecitadel/core/data/openclaw.db")


def read(path, query, parameters=()):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return db.execute(query, parameters).fetchall()
    finally:
        db.close()


def main():
    if platform.node().lower() != "jim-eq":
        raise RuntimeError("jim-eq only")
    out = Path(sys.argv[1])
    if not out.is_absolute() or not out.is_dir():
        raise ValueError("absolute private directory required")
    workload = json.loads((out / "workload.json").read_text())
    assert workload["mode"] == "baseline"
    admin = AgentdClient(
        socket_path_for(ROOT), admin_token=(ROOT / "admin.token").read_text().strip()
    )
    actors = []
    receiver = None
    report = {
        "fixture": "synthetic_ten_agents_one_source_not_actual_execution",
        "workload": workload,
    }

    def terminate(*_):
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, terminate)
    try:
        prefix = "trace-baseline-" + uuid4().hex[:10]
        for index in range(10):
            name = f"{prefix}-{index}"
            registered = admin.call(
                "connector.register",
                connector_id=name,
                host_type="codex",
                agent_id=name,
                capabilities=["edgecitadel_trace"],
            )
            actor = {"name": name, "session": None}
            actors.append(actor)
            actor["client"] = AgentdClient(
                socket_path_for(ROOT), connector_id=name, token=registered["token"]
            )
            actor["session"] = actor["client"].call("session.open", lease_seconds=300)[
                "session_id"
            ]
        node_id, source_epoch = read(
            ROOT / "agentd.sqlite3",
            "SELECT node_id,source_epoch FROM trace_sources WHERE active=1",
        )[0]
        receiver = RenderReceiver(capacity=workload["samples"])
        config = {
            "scope": {
                "node_id": node_id,
                "source_epoch": source_epoch,
                "agent_ids": [a["name"] for a in actors],
            },
            "receiver": {"url": receiver.url, "token": receiver.token},
            "samples": workload["samples"],
            "workload": workload,
        }
        with os.fdopen(
            os.open(out / "scope.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
            "w",
        ) as target:
            json.dump(config, target)
        print(json.dumps({"stage": "scope_ready", "agents": 10}), flush=True)
        if not receiver.ready.wait(150):
            raise TimeoutError("baseline browser readiness timeout")
        started = time.monotonic()
        next_renewal = started + 60
        max_lateness = 0
        trace_ids = []
        expected = []
        for index in range(workload["expected_events"]):
            scheduled = started + index / workload["event_rate"]
            time.sleep(max(0, scheduled - time.monotonic()))
            if time.monotonic() >= next_renewal:
                for actor in actors:
                    actor["client"].call(
                        "session.renew", session_id=actor["session"], lease_seconds=300
                    )
                next_renewal = time.monotonic() + 60
            cycle, agent, step = baseline_slot(index)
            actor = actors[agent]
            client = actor["client"]
            max_lateness = max(max_lateness, time.monotonic() - scheduled)
            if step == 0:
                response = client.call(
                    "trace.bind",
                    schema_version=1,
                    request_id=str(uuid4()),
                    session_id=actor["session"],
                    task_id=None,
                    context_id=None,
                )
                assert response["status"] == "ok"
                actor["binding"] = response["result"]
                actor["spans"] = [str(uuid4()) for _ in range(24)]
                trace_ids.append(actor["binding"]["trace_id"])
            elif step == 49:
                response = client.call(
                    "trace.finish",
                    schema_version=1,
                    request_id=str(uuid4()),
                    binding_id=actor["binding"]["binding_id"],
                    outcome="completed",
                    reason="unknown",
                )
                assert response["status"] == "ok"
            else:
                span = actor["spans"][(step - 1) // 2]
                phase = "started" if step % 2 else "finished"
                binding = actor["binding"]
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
                        "attributes": {"name": "synthetic.baseline.operation"},
                    },
                )
                assert response["status"] == "ok"
                if phase == "finished" and agent in workload["sampled_agents"]:
                    parts = [
                        node_id,
                        source_epoch,
                        binding["trace_id"],
                        binding["execution_attempt_id"],
                        span,
                    ]
                    identity = (
                        "span:"
                        + hashlib.sha256(
                            json.dumps(parts, separators=(",", ":")).encode()
                        ).hexdigest()
                    )
                    event_id = response["result"]["event_id"]
                    receiver.expect(
                        event_id,
                        identity,
                        "finished",
                        trace_id=binding["trace_id"],
                        lane=agent,
                        measured=cycle >= workload["warmup_cycles"],
                    )
                    expected.append(event_id)
            if (index + 1) % 500 == 0:
                progress = {
                    "emitted": index + 1,
                    "planned": workload["expected_events"],
                    "seconds": time.monotonic() - started,
                    "acks": len(receiver.report()["acks"]),
                }
                temporary = out / "progress.tmp"
                temporary.write_text(json.dumps(progress))
                temporary.replace(out / "progress.json")
        # Preserve the complete declared duration, including the last slot tail.
        time.sleep(max(0, started + workload["duration_s"] - time.monotonic()))
        report["emission"] = {
            "events": workload["expected_events"],
            "seconds": time.monotonic() - started,
            "max_schedule_lateness_ms": max_lateness * 1000,
            "target_event_rate": workload["event_rate"],
            "waited_for_render_during_emission": False,
        }
        assert len(set(trace_ids)) == workload["expected_runs"]
        assert len(expected) == workload["samples"]
        deadline = time.monotonic() + 120
        for event_id in expected:
            receiver.wait(event_id, max(0.01, deadline - time.monotonic()))
        names = tuple(actor["name"] for actor in actors)
        placeholders = ",".join("?" for _ in names)
        source = read(
            ROOT / "agentd.sqlite3",
            f"SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE agent_id IN ({placeholders}) ORDER BY source_seq",
            names,
        )
        assert len(source) == workload["expected_events"]
        # Each owned run must retain the declared 50 observations.
        counts = {}
        for row in source:
            trace_id = json.loads(row[-1])["trace_id"]
            counts[trace_id] = counts.get(trace_id, 0) + 1
        assert set(counts) == set(trace_ids) and set(counts.values()) == {50}
        deadline = time.monotonic() + 90
        while True:
            db = sqlite3.connect(f"file:{CORE}?mode=ro", uri=True)
            try:
                db.execute("BEGIN")
                central = [
                    db.execute(
                        "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                        row[:3],
                    ).fetchone()
                    for row in source
                ]
            finally:
                db.close()
            states = read(
                ROOT / "agentd.sqlite3",
                f"SELECT p.state FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.agent_id IN ({placeholders})",
                names,
            )
            if (
                source == central
                and len(states) == len(source)
                and all(s == ("core_settled",) for s in states)
            ):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("baseline exact settlement timeout")
            time.sleep(0.5)
        report.update(
            source_core_exact=True,
            all_core_settled=True,
            event_count=len(source),
            run_count=len(trace_ids),
            trace_id=None,
        )
    finally:
        cleanup_errors = []
        if receiver is not None:
            report["render"] = receiver.report()
            try:
                receiver.close()
            except Exception as error:
                cleanup_errors.append(error)
        for actor in actors:
            try:
                if actor["session"] is not None:
                    actor["client"].call("session.close", session_id=actor["session"])
            except Exception as error:
                cleanup_errors.append(error)
            try:
                admin.call("connector.revoke", connector_id=actor["name"])
            except Exception as error:
                cleanup_errors.append(error)
        report["owned_connector_revoked"] = not cleanup_errors
        (out / "fixture.json").write_text(json.dumps(report, indent=2) + "\n")
        if cleanup_errors:
            raise ExceptionGroup("baseline cleanup incomplete", cleanup_errors)
    print(
        json.dumps({"stage": "complete", "event_count": report["event_count"]}),
        flush=True,
    )


if __name__ == "__main__":
    main()
