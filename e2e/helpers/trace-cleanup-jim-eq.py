"""Reclaim completed E2E fixtures on the authorized jim-eq deployment only."""

import argparse
import fcntl
import json
import os
import platform
import pwd
import re
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from trace_cleanup import purge_source, source_plan, validate_source


def run(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError(f"maintenance_command_failed: {result.stderr}")
    return result


def source_db(root):
    db = sqlite3.connect(
        f"file:{root}/trace/agentd.sqlite3?mode=rw", uri=True, timeout=30
    )
    db.execute("PRAGMA foreign_keys=ON")
    db.execute(
        "ATTACH DATABASE ? AS task_state",
        (f"file:{root}/agentd-tasks.sqlite3?mode=rw",),
    )
    return db


def systemctl(uid, *args):
    user = pwd.getpwuid(uid).pw_name
    return run(
        [
            "runuser",
            "-u",
            user,
            "--",
            "env",
            f"XDG_RUNTIME_DIR=/run/user/{uid}",
            f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus",
            "systemctl",
            "--user",
            *args,
        ]
    )


def compact_source(root):
    path = root / "trace/agentd.sqlite3"
    target = path.with_name("e2e-compacted.sqlite3")
    if target.exists():
        raise RuntimeError("previous_compaction_requires_inspection")
    try:
        with sqlite3.connect(path) as db:
            db.execute("VACUUM INTO ?", (str(target),))
        with sqlite3.connect(f"file:{target}?mode=ro", uri=True) as db:
            if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise RuntimeError("compact_integrity_failed")
        target.chmod(0o600)
        with target.open("rb") as file:
            os.fsync(file.fileno())
        os.replace(target, path)
        fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        target.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--trace-id", action="append", default=[])
    parser.add_argument("--protect-trace-id", action="append", default=[])
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    if any(not re.fullmatch(r"[0-9a-f]{32}", trace) for trace in args.trace_id):
        parser.error("trace-id must be an explicitly owned canonical trace ID")
    if platform.node().lower() != "jim-eq":
        raise RuntimeError("jim-eq only")
    if args.source:
        manifest = json.loads(args.manifest.read_text())
        db = source_db(args.source)
        before = db.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        removed = purge_source(
            db,
            manifest["traces"],
            manifest["agents"],
            protected=manifest.get("protected", []),
        )
        db.close()
        compact_source(args.source)
        print(
            json.dumps(
                {"source_events": removed, "before": before, "after": before - removed}
            )
        )
        return
    if os.geteuid() != 0:
        raise RuntimeError("operator maintenance requires root")
    with open("/run/edgecitadel-e2e-cleanup.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        maintain(args.trace_id, args.protect_trace_id, plan_only=args.plan_only)


def maintain(extra_traces, protected=(), *, plan_only=False):
    roots = [
        Path(f"/var/lib/edgecitadel-{role}/state/agentd") for role in ("core", "leaf")
    ]
    core = Path("/root/.edgecitadel/core/data/openclaw.db")
    paths = [core, *(root / "trace/agentd.sqlite3" for root in roots)]
    before = sum(path.stat().st_blocks * 512 for path in paths)
    stopped = []
    core_stopped = False
    directory = Path(
        tempfile.mkdtemp(prefix="edgecitadel-e2e-cleanup-", dir="/var/tmp")
    )
    directory.chmod(0o755)
    backups = directory / "backups"
    backups.mkdir(mode=0o700)
    try:
        for name in ("trace_cleanup.py", "trace-cleanup-jim-eq.py"):
            shutil.copyfile(Path(__file__).with_name(name), directory / name)
            (directory / name).chmod(0o644)
        for root in roots:
            uid = root.stat().st_uid
            units = systemctl(
                uid,
                "list-units",
                "--type=service",
                "--state=running",
                "--no-legend",
                "--plain",
            ).stdout
            names = [
                line.split()[0]
                for line in units.splitlines()
                if line.split()[0].startswith("edgecitadel-agentd-")
            ]
            if len(names) != 1:
                raise RuntimeError("expected_one_running_agentd_per_source")
            systemctl(uid, "stop", names[0])
            stopped.append((uid, names[0]))
        # Stopped sources cannot race an eligibility check or republish old data.
        plans = []
        for root in roots:
            db = source_db(root)
            plans.append(source_plan(db))
            db.close()
        manifest = {
            key: sorted({value for plan in plans for value in plan[key]})
            for key in ("agents", "traces")
        }
        if set(extra_traces) & set(protected):
            raise ValueError("explicit_deletion_overlaps_protected_trace")
        manifest["protected"] = sorted(set(protected))
        manifest["traces"] = sorted(
            (set(manifest["traces"]) | set(extra_traces)) - set(protected)
        )
        manifest["tasks"] = []
        for root in roots:
            db = source_db(root)
            validate_source(db, manifest["traces"])
            manifest["tasks"].extend(
                row[0]
                for row in db.execute(
                    "SELECT task_id FROM task_state.tasks WHERE trace_id IN (SELECT id FROM owned_traces)"
                )
            )
            db.close()
        (directory / "scope.json").write_text(json.dumps(manifest))
        (directory / "scope.json").chmod(0o644)
        print(
            json.dumps({"maintenance_directory": str(directory), "plan": manifest}),
            flush=True,
        )
        if plan_only:
            return
        run(["docker", "stop", "edgecitadel-aggregator-1"])
        core_stopped = True
        # Writers are stopped. Preserve every attached payload/projection database,
        # task terminal state and source receipts in a private, retryable backup.
        for label, root in [
            ("core", core.parent),
            *[(f"source-{i}", root) for i, root in enumerate(roots)],
        ]:
            destination = backups / label
            shutil.copytree(
                root,
                destination,
                symlinks=True,
                ignore=shutil.ignore_patterns("*.sock", "writer.lock"),
            )
        (directory / "phase.json").write_text(
            json.dumps({"phase": "backed_up", "scope": manifest})
        )
        result = run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--volumes-from",
                "edgecitadel-aggregator-1",
                "-v",
                f"{directory}:/maintenance:ro",
                "-e",
                "PYTHONPATH=/app",
                "--entrypoint",
                "python",
                "edgecitadel-aggregator",
                "/maintenance/trace_cleanup.py",
                "/maintenance/scope.json",
            ]
        )
        report = {
            "core": [json.loads(line) for line in result.stdout.splitlines()],
            "sources": [],
        }
        (directory / "phase.json").write_text(
            json.dumps({"phase": "core_purged", "report": report})
        )
        for root in roots:
            user = pwd.getpwuid(root.stat().st_uid).pw_name
            result = run(
                [
                    "runuser",
                    "-u",
                    user,
                    "--",
                    "python3",
                    str(directory / "trace-cleanup-jim-eq.py"),
                    "--source",
                    str(root),
                    "--manifest",
                    str(directory / "scope.json"),
                ]
            )
            report["sources"].append(json.loads(result.stdout))
        after = sum(path.stat().st_blocks * 512 for path in paths)
        report.update(
            owned_agents=len(manifest["agents"]),
            owned_traces=len(manifest["traces"]),
            before_bytes=before,
            after_bytes=after,
            reclaimed_bytes=before - after,
        )
        for path in paths:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as check:
                if check.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise RuntimeError("cleanup_integrity_failed")
        (directory / "phase.json").write_text(
            json.dumps({"phase": "purged_pending_sync_verification", "report": report})
        )
        print(json.dumps(report))
    finally:
        try:
            if core_stopped:
                run(["docker", "start", "edgecitadel-aggregator-1"])
        finally:
            try:
                for uid, name in reversed(stopped):
                    systemctl(uid, "start", name)
            finally:
                print(
                    json.dumps({"retained_maintenance_directory": str(directory)}),
                    flush=True,
                )


if __name__ == "__main__":
    main()
