"""Actual daemon admission/layout gate on an owned jim-eq user-quota filesystem."""

import ctypes
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time


def worker(state, no_new_privs):
    if no_new_privs:
        assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.service import main

    raise SystemExit(main(["--state-dir", str(state)]))


def main(output=None):
    from test_trace_linux_quota import owned_volume
    from edgecitadel_agentd.client import AgentdClient, AgentdClientError
    from edgecitadel_agentd.service import socket_path_for

    report = {}
    with owned_volume() as (root, state, _, device):
        state.chmod(0o700)
        (root / "node.json").write_text('{"agent_id":"owned-quota-node"}')
        (root / "node.json").chmod(0o644)
        # The production layout requires a real directory, not the old probe's symlink.
        (state / "trace").unlink()
        (state / "trace").mkdir(mode=0o700)
        os.chown(state / "trace", 65534, 65534)
        script = root / "daemon-probe.py"
        shutil.copyfile(__file__, script)
        script.chmod(0o644)
        environment = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "EDGECITADEL_TRACE_SYNC": "1",
        }

        def command(no_new_privs=True):
            return [
                sys.executable,
                str(script),
                "--worker",
                str(state),
                str(int(no_new_privs)),
            ]

        def refuse(reason, *, no_new_privs=True, uid=65534):
            result = subprocess.run(
                command(no_new_privs),
                user=uid,
                group=uid,
                extra_groups=[],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert result.returncode == 1 and reason in result.stderr, result.stderr
            assert not (state / "process.json").exists()
            assert not socket_path_for(state).exists()
            assert not (state / "trace/agentd.sqlite3").exists()
            assert not (state / "agentd-tasks.sqlite3").exists()

        refuse("separate filesystems")
        report["ordinary_directory_refused_before_admission"] = True
        subprocess.run(
            ["mount", "--bind", str(root / "fs/trace"), str(state / "trace")],
            check=True,
        )
        process = None
        try:
            refuse("no_new_privs", no_new_privs=False)
            refuse("non-root", uid=0)
            report["privileged_or_escalatable_writer_refused"] = True
            process = subprocess.Popen(
                command(),
                user=65534,
                group=65534,
                extra_groups=[],
                cwd=root,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            client = AgentdClient(socket_path_for(state), timeout=0.5)
            deadline = time.monotonic() + 15
            while True:
                assert process.poll() is None, process.stderr.read()
                try:
                    health = client.call("health")
                    if health["telemetry"]["state"] == "unconfigured":
                        break
                except AgentdClientError:
                    pass
                assert time.monotonic() < deadline, "daemon/sync did not become ready"
                time.sleep(0.02)
            admin = AgentdClient(
                socket_path_for(state),
                admin_token=(state / "admin.token").read_text().strip(),
            )
            registration = admin.call(
                "connector.register",
                connector_id="owned",
                host_type="codex",
                agent_id="owned",
                capabilities=["edgecitadel_delegate"],
            )
            connector = AgentdClient(
                socket_path_for(state),
                connector_id="owned",
                token=registration["token"],
            )
            task = connector.call(
                "task.create",
                recipient_id="remote",
                payload={"body": "owned quota task"},
            )
            with closing(
                sqlite3.connect(
                    (state / "trace/agentd.sqlite3").as_uri() + "?mode=ro", uri=True
                )
            ) as db:
                db.execute("BEGIN")
                assert (
                    db.execute(
                        "SELECT count(*) FROM sqlite_schema WHERE name='tasks'"
                    ).fetchone()[0]
                    == 0
                )
                events = db.execute("SELECT event_json FROM trace_journal").fetchall()
                assert events and all(
                    json.loads(row[0])["node_id"] == "owned-quota-node"
                    for row in events
                )
                assert db.execute("SELECT count(*) FROM trace_spool").fetchone()[
                    0
                ] == len(events)
            with closing(
                sqlite3.connect(
                    (state / "agentd-tasks.sqlite3").as_uri() + "?mode=ro", uri=True
                )
            ) as db:
                assert (
                    db.execute("SELECT task_id FROM tasks").fetchone()[0]
                    == task["task_id"]
                )
            assert (state / "payload.key").is_file()
            assert not (state / "trace/payload.key").exists()
            assert not (state / "agentd.sqlite3").exists()
            report.update(
                daemon_ready=True,
                command_and_sync_handles_opened=True,
                task_trace_export_present=True,
                source_identity_correct=True,
                trace_events=len(events),
                key_outside_trace=True,
                schema_version=health["schema_version"],
            )
            process.terminate()
            _, errors = process.communicate(timeout=15)
            assert process.returncode == 0, errors
            process = None
            protected = [
                state / "trace/agentd.sqlite3",
                state / "agentd-tasks.sqlite3",
                state / "payload.key",
            ]
            before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
            from edgecitadel_agentd import trace_quota

            fd = os.open(state / "trace", os.O_RDONLY | os.O_DIRECTORY)
            try:
                trace_quota._query(fd, 0x800003 << 8, 0, trace_quota._QuotaState())
                result = subprocess.run(
                    command(),
                    user=65534,
                    group=65534,
                    extra_groups=[],
                    cwd=root,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
                assert result.returncode == 1 and "both be active" in result.stderr, (
                    result.stderr
                )
                assert not socket_path_for(state).exists()
                assert {
                    p: hashlib.sha256(p.read_bytes()).hexdigest() for p in protected
                } == before
                report["quota_disabled_restart_refused_without_database_changes"] = True
            finally:
                trace_quota._query(fd, 0x800002 << 8, 0, trace_quota._QuotaState())
                os.close(fd)
        finally:
            if process is not None:
                if process.poll() is None:
                    process.terminate()
                try:
                    _, errors = process.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)
                    raise
                assert process.returncode == 0, errors
            subprocess.run(["umount", str(state / "trace")], check=True)
    report["scratch_removed"] = not root.exists()
    report["test_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["scope"] = (
        "Owned real daemon and quota boundary; no broker, adapter execution, reservations or full M4 acceptance."
    )
    if output:
        output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def test_native_storage_layout(tmp_path):
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("explicit jim-eq native daemon quota gate required")
    main(tmp_path / "native-layout.json")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), bool(int(sys.argv[3])))
    else:
        main(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
