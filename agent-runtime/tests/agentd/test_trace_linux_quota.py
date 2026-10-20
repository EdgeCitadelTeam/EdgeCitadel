"""Opt-in jim-eq user-quota qualification with synthetic reserved slots.

Run directly as root on jim-eq, or set RUN_AGENTD_USER_QUOTA=1 for pytest.
Live service stores are never opened. This is native enforcement feasibility, not
AgentdStore reservation/migration integration or full M4 acceptance.
"""

from contextlib import contextmanager
import os
import platform
import ctypes
import errno
import hashlib
import json
from pathlib import Path
import select
import signal
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

WIDTH = 16 * 1024
TASKS = 128
PAGES = 32 * 1024
QUOTA_BYTES = 256 * 1024 * 1024


class Quota(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_uint64)
        for name in (
            "hard",
            "soft",
            "space",
            "ihard",
            "isoft",
            "inodes",
            "btime",
            "itime",
        )
    ] + [("valid", ctypes.c_uint32)]


def quota(device, *, set_limit=False):
    value = Quota()
    if set_limit:
        value.hard = QUOTA_BYTES // 1024
        value.ihard = 128
        value.valid = 5  # QIF_BLIMITS | QIF_ILIMITS, Linux quota.h
    libc = ctypes.CDLL(None, use_errno=True)
    libc.quotactl.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_void_p,
    ]
    libc.quotactl.restype = ctypes.c_int
    operation = 0x800008 if set_limit else 0x800007
    if libc.quotactl(operation << 8, os.fsencode(device), 65534, ctypes.byref(value)):
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return value


@contextmanager
def owned_volume():
    assert platform.node().lower() == "jim-eq"
    assert os.geteuid() == 0
    root = Path(tempfile.mkdtemp(prefix="edgecitadel-quota-", dir="/var/tmp"))
    root.chmod(0o755)
    image, mount, work = root / "fixture.img", root / "fs", root / "work"
    mount.mkdir()
    work.mkdir()
    os.chown(work, 65534, 65534)
    with image.open("xb") as handle:
        os.posix_fallocate(handle.fileno(), 0, 512 * 1024 * 1024)
    subprocess.run(
        [
            "mkfs.ext4",
            "-q",
            "-F",
            "-O",
            "quota",
            "-E",
            "quotatype=usrquota,nodiscard",
            str(image),
        ],
        check=True,
    )
    device = subprocess.check_output(
        ["losetup", "--find", "--show", str(image)], text=True
    ).strip()
    mounted = False
    try:
        subprocess.run(
            ["mount", "-o", "usrquota,nosuid,nodev,noexec", device, str(mount)],
            check=True,
        )
        mounted = True
        trace_directory = mount / "trace"
        trace_directory.mkdir()
        quota(device, set_limit=True)
        assert quota(device).hard * 1024 == QUOTA_BYTES
        os.chown(trace_directory, 65534, 65534)
        trace_directory.chmod(0o700)
        (work / "trace").symlink_to(trace_directory, target_is_directory=True)
        script = root / "probe.py"
        shutil.copyfile(__file__, script)
        script.chmod(0o644)
        yield root, work, script, device
    finally:
        if mounted:
            subprocess.run(["umount", str(mount)], check=True)
        if any(
            str(root) in line
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
        ):
            raise RuntimeError(
                "owned descendant mount remains; retain fixture for cleanup"
            )
        subprocess.run(["losetup", "--detach", device], check=True)
        shutil.rmtree(root)


def connect(root):
    db = sqlite3.connect(root / "trace/trace.db", timeout=0)
    try:
        assert db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        db.execute("PRAGMA synchronous=EXTRA")
        db.execute("PRAGMA temp_store=MEMORY")
        db.execute("PRAGMA cache_spill=OFF")
        db.execute("ATTACH DATABASE ? AS task_state", (str(root / "tasks.db"),))
        db.execute("PRAGMA task_state.journal_mode=DELETE")
        db.execute("PRAGMA task_state.synchronous=EXTRA")
        assert db.execute("PRAGMA main.journal_mode=DELETE").fetchone()[0] == "delete"
        db.execute("PRAGMA main.synchronous=EXTRA")
        assert db.execute(f"PRAGMA main.max_page_count={PAGES}").fetchone()[0] == PAGES
        assert db.execute("PRAGMA main.page_size").fetchone()[0] == 4096
        return db
    except BaseException:
        db.close()
        raise


def envelope(task, phase):
    body = json.dumps({"task": task, "phase": phase}, sort_keys=True).encode()
    # Rewrite every page. No mutable secondary index or variable-length column
    # participates; this is the narrow mechanism whose feasibility is tested.
    return body + hashlib.shake_256(body).digest(WIDTH - len(body))


def snapshot(db):
    return (
        list(db.execute("SELECT * FROM tasks ORDER BY id")),
        list(db.execute("SELECT * FROM main.slots ORDER BY id")),
    )


def files(root):
    return {
        str(path.relative_to(root)): {
            "length": path.stat().st_size,
            "allocated": path.stat().st_blocks * 512,
        }
        for path in [*root.iterdir(), *(root / "trace").rglob("*")]
        if path.is_file()
    }


def rewrite(db, phase):
    db.execute("BEGIN IMMEDIATE")
    db.execute("UPDATE tasks SET state=?", (phase,))
    for task in range(TASKS):
        changed = db.execute(
            "UPDATE main.slots SET body=? WHERE id=?",
            (envelope(task, phase), task),
        )
        assert changed.rowcount == 1


def child(root, boundary):
    db = connect(root)
    try:
        rewrite(db, "completed")
        if boundary == "after_commit":
            db.commit()
        print(boundary, flush=True)
        # Parent owns the lifetime and kills at this explicit boundary.
        while True:
            time.sleep(1)
    finally:
        db.close()


def kill_at(root, boundary):
    with subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            str(root),
            boundary,
            str(PAGES),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            ready, _, _ = select.select([process.stdout], [], [], 20)
            assert ready, "child did not reach boundary"
            line = process.stdout.readline().strip()
            if line != boundary:
                process.wait(timeout=5)
                raise AssertionError(f"child failed: {line}; {process.stderr.read()}")
            observed = files(root)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        assert process.returncode == -signal.SIGKILL
        return observed


def exercise(root):
    result = {
        "scope": "Kernel-enforced Linux user quota, attached rollback and fixed slots; not AgentdStore integration",
        "quota_target_bytes": QUOTA_BYTES,
        "trace_main_owns_superjournal": True,
        "writer_uid": os.geteuid(),
        "sqlite_version": sqlite3.sqlite_version,
        "trace_page_ceiling": PAGES,
        "slot_bytes": WIDTH,
        "reserved_completions": TASKS,
    }
    db = connect(root)
    try:
        db.execute(
            "CREATE TABLE task_state.tasks(id INTEGER PRIMARY KEY,state TEXT NOT NULL)"
        )
        db.execute(
            f"CREATE TABLE main.slots(id INTEGER PRIMARY KEY,body BLOB NOT NULL CHECK(length(body)={WIDTH}))"
        )
        db.execute(
            "CREATE TABLE main.pressure(id INTEGER PRIMARY KEY,body BLOB NOT NULL)"
        )
        # Reservation and authority are committed together, unlike the
        # historical two-commit authority/task handoff experiment.
        with db:
            db.execute("BEGIN IMMEDIATE")
            for task in range(TASKS):
                db.execute("INSERT INTO tasks VALUES(?, 'queued')", (task,))
                db.execute(
                    "INSERT INTO main.slots VALUES(?,?)",
                    (task, envelope(task, "queued")),
                )
        pending = snapshot(db)
        inserted = 0
        while True:
            try:
                with db:
                    db.execute(
                        "INSERT INTO main.pressure(body) VALUES(?)",
                        (b"x" * (1024 * 1024),),
                    )
                inserted += 1
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode == sqlite3.SQLITE_FULL
                break
        # Use smaller rows to consume the remaining gap at the page limit.
        small = 0
        while True:
            try:
                with db:
                    db.execute(
                        "INSERT INTO main.pressure(body) VALUES(?)",
                        (b"y" * WIDTH,),
                    )
                small += 1
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode == sqlite3.SQLITE_FULL
                break
        result["pressure_rows"] = {"one_mib": inserted, "sixteen_kib": small}
        result["trace_pages_at_full"] = db.execute("PRAGMA main.page_count").fetchone()[
            0
        ]
        assert snapshot(db) == pending
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("INSERT INTO tasks VALUES(?, 'queued')", (TASKS,))
                db.execute(
                    "INSERT INTO main.slots VALUES(?,?)",
                    (TASKS, envelope(TASKS, "queued")),
                )
        except sqlite3.OperationalError as error:
            assert error.sqlite_errorcode == sqlite3.SQLITE_FULL
        else:
            raise AssertionError("new reservation unexpectedly fitted")
        assert snapshot(db) == pending
        result["trace_full_rolls_back_task_admission"] = True
    finally:
        db.close()

    result["before_commit_kill_files"] = kill_at(root, "before_commit")
    db = connect(root)
    try:
        assert snapshot(db) == pending
        result["precommit_sigkill_preserves_both_databases"] = True
        # A pinned trace reader can prevent the coordinated commit. The
        # caller must roll back BOTH files before retrying the operation.
        reader = sqlite3.connect(root / "trace/trace.db", timeout=0)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT body FROM slots LIMIT 1").fetchone()
            try:
                with db:
                    rewrite(db, "completed")
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode == sqlite3.SQLITE_BUSY
            else:
                raise AssertionError("pinned reader did not block commit")
            assert not db.in_transaction
            assert snapshot(db) == pending
            result["pinned_reader_refusal_rolls_back_both"] = True
        finally:
            reader.close()
    finally:
        db.close()

    result["after_commit_kill_files"] = kill_at(root, "after_commit")
    db = connect(root)
    try:
        tasks, slots = snapshot(db)
        assert tasks == [(i, "completed") for i in range(TASKS)]
        assert slots == [(i, envelope(i, "completed")) for i in range(TASKS)]
        assert (
            db.execute("PRAGMA main.page_count").fetchone()[0]
            == result["trace_pages_at_full"]
        )
        result["postcommit_sigkill_preserves_exact_completions"] = True
        result["completion_without_trace_page_growth"] = True
        for schema in ("main", "task_state"):
            assert db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] == "ok"
        # Overwrite every pressure page in one transaction to observe a
        # near-whole-database journal, then roll it back. This is an
        # observation, not a proof of all filesystem allocation peaks.
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute("UPDATE main.pressure SET body=randomblob(length(body))")
            result["whole_trace_rewrite_refused"] = False
        except sqlite3.OperationalError as error:
            assert error.sqlite_errorcode in (
                sqlite3.SQLITE_FULL,
                sqlite3.SQLITE_IOERR_WRITE,
            )
            result["whole_trace_rewrite_refused"] = True
            result["whole_trace_rewrite_sqlite_code"] = error.sqlite_errorcode
        result["whole_trace_rewrite_files"] = files(root)
        db.rollback()
        assert snapshot(db) == (tasks, slots)
        result["final_files"] = files(root)
        result["filesystem"] = {
            "total_bytes": os.statvfs(root / "trace").f_blocks
            * os.statvfs(root / "trace").f_frsize,
            "available_bytes": os.statvfs(root / "trace").f_bavail
            * os.statvfs(root / "trace").f_frsize,
        }
    finally:
        db.close()
    return result


def worker(root):
    assert os.geteuid() == 65534
    assert (
        int(
            next(
                line.split()[1]
                for line in Path("/proc/self/status").read_text().splitlines()
                if line.startswith("CapEff:")
            ),
            16,
        )
        == 0
    )
    # Direct allocation must hit EDQUOT while the larger fixture filesystem still has space.
    target = root / "trace/pressure.bin"
    written = 0
    with target.open("xb", buffering=0) as handle:
        try:
            os.chown(target, 0, 0)
        except PermissionError:
            pass
        else:
            raise AssertionError("writer escaped quota by transferring ownership")
        for size in (1024 * 1024, 4096, 1024):
            try:
                while written < 300 * 1024 * 1024:
                    written += handle.write(b"x" * size)
                    os.fsync(handle.fileno())
            except OSError as error:
                assert error.errno == errno.EDQUOT
            else:
                raise AssertionError("user hard quota did not refuse allocation")
        assert (
            os.statvfs(root.parent / "fs").f_bavail
            * os.statvfs(root.parent / "fs").f_frsize
            > 64 * 1024 * 1024
        )
        target.unlink()
        # The kernel must keep charging an unlinked open file, including against
        # a separate unprivileged writer. Named-file sampling cannot do this.
        code = """import errno, os, sys
with open(sys.argv[1], 'xb', buffering=0) as output:
    try:
        output.write(b'x' * 4096)
        os.fsync(output.fileno())
    except OSError as error:
        assert error.errno == errno.EDQUOT
    else:
        raise AssertionError('unlinked allocation escaped quota')
"""
        subprocess.run(
            [sys.executable, "-c", code, str(root / "trace/second.bin")], check=True
        )
    (root / "trace/second.bin").unlink()
    result = exercise(root)
    result["direct_allocation_refused_edquot"] = True
    result["writer_cannot_transfer_quota_ownership"] = True
    result["unlinked_file_charged_against_second_writer"] = True
    result["direct_bytes_written_before_refusal"] = written
    result["scope"] = (
        "Native user allocation quota and simplified atomic slots; not AgentdStore or completion reservation integration"
    )
    (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")


def main(output=None):
    with owned_volume() as (root, work, script, device):
        result = subprocess.run(
            ["/usr/bin/python3", str(script), "--worker", str(work)],
            user=65534,
            group=65534,
            extra_groups=[],
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise AssertionError(result.stderr)
        report = json.loads((work / "result.json").read_text())
        measured = quota(device)
        report["quota_hard_bytes"] = measured.hard * 1024
        report["quota_inode_hard_limit"] = measured.ihard
        assert measured.ihard == 128
        report["quota_allocated_bytes_after_recovery"] = measured.space
        assert measured.hard * 1024 == QUOTA_BYTES
        assert measured.space <= QUOTA_BYTES
        report["quota_includes_directory"] = True
        report["shared_fixture_filesystem_bytes"] = 512 * 1024 * 1024
        report["excluded_infrastructure"] = (
            "Shared filesystem global metadata and test loop backing file; quota covers user-charged file/directory allocations, including SQLite journals and superjournal."
        )
    report["scratch_removed"] = not root.exists()
    report["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (output or Path(__file__).with_name("m4-linux-user-quota.json")).write_text(
        json.dumps(report, indent=2) + "\n"
    )
    print(json.dumps(report, indent=2))


def test_user_quota_atomic_slots(tmp_path):
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("explicit jim-eq user-quota qualification opt-in required")
    main(tmp_path / "user-quota.json")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        PAGES = int(sys.argv[4])
        child(Path(sys.argv[2]), sys.argv[3])
    elif len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]))
    else:
        main()
