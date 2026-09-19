"""S1 on jim-eq: three owned Hermes adapters with bound model/tool observation."""

import json
import os
import platform
import pwd
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

import yaml

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for

assert platform.node().lower() == "jim-eq", "Run real E2E on jim-eq only"
OUT = Path(sys.argv[1])
assert OUT.is_absolute() and OUT.is_dir(), "Use an existing owned output directory"
ASSETS = Path("/opt/edgecitadel/quota-1a150dc/share/edgecitadel")
STATE = Path("/var/lib/edgecitadel-leaf/state")
ROOT = Path("/var/lib/edgecitadel-core/state/agentd")
CORE = Path("/root/.edgecitadel/core/data/openclaw.db")
PYTHON = "/opt/hermes-agent/venv/bin/python"
OVERLAY = STATE / "supervisor/runtime-source/src"
DEPENDENCIES = STATE / "supervisor/lib/python3.12/site-packages"
assert (OVERLAY / "edgecitadel_agentd").is_dir(), (
    "Prepared Leaf runtime source is missing"
)
assert (DEPENDENCIES / "nats").is_dir(), (
    "Prepared Leaf runtime dependencies are missing"
)
CLI = "/opt/edgecitadel/quota-1a150dc/bin/edgecitadel"
BROWSER = sys.argv[2:] == ["--browser"]
assert not sys.argv[2:] or BROWSER, "Unknown scenario option"
nonce = uuid4().hex[:8]
identity = pwd.getpwuid(STATE.stat().st_uid)
assert identity.pw_uid != 0, "Live Leaf must use its dedicated service UID"
fixture_parent = STATE / "qualification"
fixture_parent.mkdir(mode=0o700, exist_ok=True)
os.chown(fixture_parent, identity.pw_uid, identity.pw_gid)
fixture = fixture_parent / ("trace-workers-" + nonce)
fixture.mkdir(mode=0o700)
os.chown(fixture, identity.pw_uid, identity.pw_gid)
workers = []
root_client = None
session = None
admin = AgentdClient(
    socket_path_for(ROOT), admin_token=(ROOT / "admin.token").read_text().strip()
)
leaf_admin = AgentdClient(
    socket_path_for(STATE / "agentd"),
    admin_token=(STATE / "agentd/admin.token").read_text().strip(),
)


def command(argv, env=None):
    if argv[0] == CLI:
        env = {
            **(env or os.environ),
            "HOME": str(STATE.parent),
            "XDG_RUNTIME_DIR": f"/run/user/{identity.pw_uid}",
            "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{identity.pw_uid}/bus",
        }
        argv = ["runuser", "-u", identity.pw_name, "--", *argv]
    with (OUT / "provision.log").open("a") as log:
        subprocess.run(argv, env=env, stdout=log, stderr=log, check=True, timeout=120)


def read(path, sql, values):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return db.execute(sql, values).fetchall()
    finally:
        db.close()


def emit(stage, **fields):
    print(json.dumps({"stage": stage, **fields}), flush=True)


try:
    for suffix in "abc":
        name = f"hermes-s1-{nonce}-{suffix}"
        agent = f"jim-eq-s1-{nonce}-{suffix}"
        directory = OUT / suffix
        directory.mkdir(mode=0o700)
        profile = directory / "profile"
        profile.mkdir(mode=0o700)
        for filename in ["config.yaml", ".env", "auth.json"]:
            shutil.copy2(Path("/root/.hermes") / filename, profile / filename)
            (profile / filename).chmod(0o600)
        config = yaml.safe_load((profile / "config.yaml").read_text())
        config["mcp_servers"] = {}
        config.setdefault("platform_toolsets", {})["api_server"] = [
            "terminal",
            "edgecitadel-scoped",
        ]
        (profile / "config.yaml").write_text(yaml.safe_dump(config))
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        token = directory / "http.token"
        token.write_text(secrets.token_urlsafe(40) + "\n")
        token.chmod(0o600)
        service_directory = fixture / suffix
        service_directory.mkdir(mode=0o700)
        os.chown(service_directory, identity.pw_uid, identity.pw_gid)
        service_token = service_directory / "http.token"
        shutil.copy2(token, service_token)
        os.chown(service_token, identity.pw_uid, identity.pw_gid)
        package = service_directory / "package"
        shutil.copytree(ASSETS / "agent-packages/hermes", package)
        manifest = yaml.safe_load((package / "plugin.yaml").read_text())
        manifest["metadata"].update(name=name, version="0.2.0")
        manifest["agents"][0]["id"] = agent
        manifest["permissions"]["network"]["outbound"] = [base]
        (package / "plugin.yaml").write_text(yaml.safe_dump(manifest))
        binding = package / "skills/hermes-reasoner/binding.yaml"
        data = yaml.safe_load(binding.read_text())
        data["requires"]["network"] = [base]
        binding.write_text(yaml.safe_dump(data))
        card = package / "edgecitadel_hermes_plugin/config.yaml"
        data = yaml.safe_load(card.read_text())
        data.update(agent_id=agent, name=agent)
        card.write_text(yaml.safe_dump(data))
        env = {
            **os.environ,
            "EDGECITADEL_STATE_DIR": str(STATE),
            "HERMES_BASE_URL": base,
            "HERMES_TOKEN_FILE": str(service_token),
        }
        command(
            [
                str(STATE / "supervisor/bin/python"),
                "-m",
                "edgecitadel_supervisor",
                "lock",
                str(package),
            ],
            env,
        )
        for path in [package, *package.rglob("*")]:
            os.lchown(path, identity.pw_uid, identity.pw_gid)
        package_id = "edgecitadel." + name
        worker = {
            "agent": agent,
            "package": package_id,
            "process": None,
            "installed": False,
            "profile": profile,
        }
        workers.append(worker)
        command(
            [CLI, "agent", "install", "--yes", "--keep-disabled", str(package)], env
        )
        worker["installed"] = True
        connector = "managed-" + agent
        registration = leaf_admin.call(
            "connector.register",
            connector_id=connector,
            host_type="managed-agent",
            agent_id=agent,
            capabilities=["reasoning.chat"],
        )
        credential = STATE / "connectors" / (connector + ".token")
        credential.write_text(registration["token"] + "\n")
        credential.chmod(0o600)
        os.chown(credential, identity.pw_uid, identity.pw_gid)
        wrapper_env = {
            **os.environ,
            "HERMES_HOME": str(profile),
            "EDGECITADEL_SCHEMA_DIR": str(ASSETS / "schemas"),
            "PYTHONPATH": ":".join(map(str, (OVERLAY, DEPENDENCIES, package))),
        }
        with (directory / "server.log").open("w") as log:
            process = subprocess.Popen(
                [
                    PYTHON,
                    "-m",
                    "edgecitadel_hermes_plugin.server",
                    "--state-dir",
                    str(STATE),
                    "--connector-id",
                    connector,
                    "--agent-id",
                    agent,
                    "--token-file",
                    str(token),
                    "--port",
                    str(port),
                ],
                env=wrapper_env,
                stdout=log,
                stderr=log,
            )
        worker["process"] = process
        deadline = time.monotonic() + 30
        while True:
            assert process.poll() is None, (
                "owned Hermes wrapper exited; inspect private server log"
            )
            try:
                with urlopen(
                    Request(
                        base + "/v1/models",
                        headers={
                            "Authorization": "Bearer " + token.read_text().strip()
                        },
                    ),
                    timeout=1,
                ) as response:
                    assert response.status == 200
                break
            except OSError:
                assert time.monotonic() < deadline, (
                    "owned Hermes wrapper startup timeout"
                )
                time.sleep(0.2)
        command([CLI, "agent", "start", package_id], env)
        emit("worker_ready", agent_id=agent)

    name = "trace-s1-" + nonce
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
    root_client = AgentdClient(
        socket_path_for(ROOT), connector_id=name, token=registration["token"]
    )
    session = root_client.call("session.open", lease_seconds=300)["session_id"]
    deadline = time.monotonic() + 30
    while root_client.call("health")["transport"].get("ready_inbox_count", 0) < 1:
        assert time.monotonic() < deadline, "root inbox readiness timeout"
        time.sleep(0.2)
    bound = root_client.call(
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
    emit("bound", trace_id=trace_id)
    if BROWSER:
        deadline = time.monotonic() + 45
        while not (OUT / "client-ready").exists():
            assert time.monotonic() < deadline, "browser readiness timeout"
            time.sleep(0.1)
    tasks = []
    for worker in workers:
        marker = "S1_" + worker["agent"].replace("-", "_")
        reply = root_client.call(
            "trace.dispatch",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=binding["binding_id"],
            recipient_id=worker["agent"],
            request=f"Call the terminal tool once with this exact command: sleep 8 && printf '{marker}'. Wait for it to finish. Then reply with exactly {marker}. Do not delegate or perform any other actions.",
            skill_id=None,
            deadline_at_ms=int(time.time() * 1000) + 120000,
        )
        assert reply["status"] == "ok"
        tasks.append((reply["result"]["task_id"], marker))
    emit("dispatched", trace_id=trace_id, task_ids=[t[0] for t in tasks])
    deadline = time.monotonic() + 140
    while True:
        states = [root_client.call("task.get", task_id=task_id) for task_id, _ in tasks]
        for task in states:
            assert task["state"] not in {
                "failed",
                "rejected",
                "cancelled",
                "expired",
                "undeliverable",
            }, "worker failed; inspect private task record"
        if all(task["state"] == "completed" for task in states):
            break
        assert time.monotonic() < deadline, "three-worker completion timeout"
        time.sleep(0.25)
    for task, (_, marker) in zip(states, tasks, strict=True):
        assert task["result"]["body"].strip() == marker, (
            "worker acknowledgment mismatch"
        )
    for worker, (_, marker) in zip(workers, tasks, strict=True):
        tool_results = read(
            worker["profile"] / "state.db",
            "SELECT content FROM messages WHERE role='tool' AND tool_name='terminal'",
            (),
        )
        assert len(tool_results) == 1, "expected exactly one terminal action per worker"
        result = json.loads(tool_results[0][0])
        assert (
            result.get("exit_code") == 0 and result.get("output", "").strip() == marker
        ), "terminal action did not independently produce the acknowledgment"
    finished = root_client.call(
        "trace.finish",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        outcome="completed",
        reason="unknown",
    )
    assert finished["status"] == "ok"
    rows = []
    for source in [ROOT, STATE / "agentd"]:
        rows.extend(
            read(
                source / "trace/agentd.sqlite3",
                "SELECT node_id,source_epoch,event_id,source_seq,event_sha256,event_json FROM trace_journal WHERE trace_id=?",
                (trace_id,),
            )
        )
    events = [json.loads(row[-1]) for row in rows]
    for task_id, _ in tasks:
        observed = {(e["kind"], e["phase"]) for e in events if e["task_id"] == task_id}
        assert {
            ("model", "started"),
            ("model", "finished"),
            ("tool", "started"),
            ("tool", "finished"),
        } <= observed, "missing actual model/tool instrumentation"
    tools = [
        e
        for e in events
        if e["kind"] == "tool" and e["attributes"].get("name") == "terminal"
    ]
    starts = [e for e in tools if e["phase"] == "started"]
    ends = [e for e in tools if e["phase"] == "finished"]
    assert len(starts) == len(ends) == 3
    assert len({(e["node_id"], e["source_epoch"]) for e in tools}) == 1
    assert max(e["source_seq"] for e in starts) < min(e["source_seq"] for e in ends), (
        "three tool actions did not overlap in the source's causal sequence"
    )
    assert all(e["duration_ms"] >= 7000 for e in ends), "requested wait did not execute"
    deadline = time.monotonic() + 120
    while True:
        central = []
        for row in rows:
            central.extend(
                read(
                    CORE,
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.source_seq,r.event_sha256,COALESCE(p.event_json,NULLIF(r.event_json,'')) FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.node_id=? AND r.source_epoch=? AND r.event_id=?",
                    row[:3],
                )
            )
        positions = []
        for source in [ROOT, STATE / "agentd"]:
            positions.extend(
                read(
                    source / "trace/agentd.sqlite3",
                    "SELECT p.node_id,p.source_epoch,p.export_generation,p.export_seq,p.event_id,p.event_sha256,p.state "
                    "FROM trace_spool p JOIN trace_journal j ON j.node_id=p.node_id "
                    "AND j.source_epoch=p.source_epoch AND j.event_id=p.journal_event_id WHERE j.trace_id=?",
                    (trace_id,),
                )
            )
        mappings = []
        for position in positions:
            mappings.extend(
                read(
                    CORE,
                    "SELECT node_id,source_epoch,export_generation,export_seq,event_id,event_sha256 "
                    "FROM trace_ingest_positions WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
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
        assert time.monotonic() < deadline, "source/Core reconciliation timeout"
        time.sleep(0.5)
    report = {
        "scenario": "S1",
        "trace_id": trace_id,
        "task_ids": [t[0] for t in tasks],
        "workers": [w["agent"] for w in workers],
        "event_count": len(rows),
        "source_core_events_exact": True,
        "export_mappings_exact": True,
        "all_core_settled": True,
        "acknowledgments_exact": True,
        "actual_model_tool_events_per_worker": True,
        "terminal_results_verified_locally": True,
        "three_tool_actions_overlapped": True,
        "root_outcome_explicit": True,
    }
finally:
    cleanup_errors = []
    try:
        if root_client is not None:
            try:
                if session is not None:
                    root_client.call("session.close", session_id=session)
            finally:
                admin.call("connector.revoke", connector_id="trace-s1-" + nonce)
    except Exception as error:
        cleanup_errors.append(error)
    for worker in reversed(workers):
        try:
            if worker["installed"]:
                command(
                    [CLI, "agent", "remove", worker["package"]],
                    {**os.environ, "EDGECITADEL_STATE_DIR": str(STATE)},
                )
        except Exception as error:
            cleanup_errors.append(error)
        try:
            process = worker["process"]
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        except Exception as error:
            cleanup_errors.append(error)
    if cleanup_errors:
        raise RuntimeError(
            "owned worker cleanup failed; inspect private logs"
        ) from cleanup_errors[0]

    shutil.rmtree(fixture)

report["owned_workers_removed"] = True
(OUT / "result.json").write_text(json.dumps(report, indent=2) + "\n")
emit("complete", **report)
