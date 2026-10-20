"""Native quota admission checks; owned ext4 fixture on jim-eq only.

Run this file directly as root on jim-eq, or opt in with RUN_AGENTD_USER_QUOTA=1.
No live service file or quota is changed. This is enforcement verification, not
production daemon integration or completion-reservation qualification.
"""

import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def worker(work, case):
    import trace_quota

    os.umask(0o077)
    if case != "privilege_escalation_allowed":
        libc = ctypes.CDLL(None, use_errno=True)
        assert libc.prctl(38, 1, 0, 0, 0) == 0  # PR_SET_NO_NEW_PRIVS
    trace = (work / "trace").resolve(strict=True)
    task = trace if case == "same_filesystem" else work
    report = {"case": case}
    if case == "accounting_only":
        fd = os.open(trace, os.O_RDONLY | os.O_DIRECTORY)
        try:
            state, usage = trace_quota._read_quota(fd, os.getuid())
        finally:
            os.close(fd)
        assert state.flags & 3 == 1
        assert usage.hard * 1024 == 256 * 1024 * 1024
        report["configured_limit_without_enforcement"] = True
    try:
        value = trace_quota.verify_trace_quota(trace, task)
    except trace_quota.TraceQuotaError as error:
        report.update(accepted=False, reason=str(error))
    else:
        report.update(
            accepted=True,
            uid=value.uid,
            hard_bytes=value.hard_bytes,
            hard_inodes=value.hard_inodes,
            allocated_bytes=value.allocated_bytes,
        )
    print(json.dumps(report))


def main(output=None):
    from test_trace_linux_quota import Quota, owned_volume, quota

    source = (
        Path(__file__).resolve().parents[2] / "src/edgecitadel_agentd/trace_quota.py"
    )
    results = []
    with owned_volume() as (root, work, _, device):
        work.chmod(0o700)
        script = root / "verify.py"
        shutil.copyfile(__file__, script)
        shutil.copyfile(source, root / "trace_quota.py")
        script.chmod(0o644)
        (root / "trace_quota.py").chmod(0o644)

        def run(case, *, accepted=False, reason=None, privileged=False):
            result = subprocess.run(
                ["/usr/bin/python3", str(script), "--worker", str(work), case],
                user=0 if privileged else 65534,
                group=0 if privileged else 65534,
                extra_groups=[],
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert result.returncode == 0, result.stderr
            report = json.loads(result.stdout)
            assert report["accepted"] is accepted, report
            if reason:
                assert reason in report["reason"], report
            results.append(report)

        def control(command, value=None):
            # The root test controller changes only its owned fixture. Production
            # verification uses read-only commands from the unprivileged worker.
            libc = ctypes.CDLL(None, use_errno=True)
            libc.syscall.argtypes = [
                ctypes.c_long,
                ctypes.c_int,
                ctypes.c_uint,
                ctypes.c_uint,
                ctypes.c_void_p,
            ]
            libc.syscall.restype = ctypes.c_long
            fd = os.open(root / "fs", os.O_RDONLY | os.O_DIRECTORY)
            try:
                if libc.syscall(
                    443,
                    fd,
                    command << 8,
                    65534,
                    ctypes.byref(value) if value is not None else None,
                ):
                    number = ctypes.get_errno()
                    raise OSError(number, os.strerror(number))
            finally:
                os.close(fd)

        run("qualified", accepted=True)
        run("same_filesystem", reason="separate filesystems")
        run("privilege_escalation_allowed", reason="no_new_privs")
        run("root_writer", reason="non-root", privileged=True)
        trace = root / "fs/trace"
        foreign = trace / "foreign.sqlite3"
        foreign.write_bytes(b"must not become an uncharged trace database")
        try:
            run("foreign_owner", reason="service UID and quota filesystem")
        finally:
            foreign.unlink()
        alias = trace / "outside"
        alias.symlink_to(work)
        os.lchown(alias, 65534, 65534)
        try:
            run("symlink", reason="single-link")
        finally:
            alias.unlink()
        control(0x800003)  # Q_QUOTAOFF
        try:
            run("accounting_only", reason="both be active")
        finally:
            control(0x800002)  # Q_QUOTAON, hidden quota inode
        run("reenabled", accepted=True)
        unlimited = Quota()
        unlimited.ihard, unlimited.valid = 128, 5
        control(0x800008, unlimited)  # Q_SETQUOTA
        try:
            assert quota(device).hard == 0
            run("unlimited", reason="hard limits")
        finally:
            quota(device, set_limit=True)
        run("reprovisioned", accepted=True)
    report = {
        "cases": results,
        "scratch_removed": not root.exists(),
        "production_module_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "scope": "Native enforcement verification; daemon wiring, reservations and full M4 acceptance remain open.",
    }
    assert report["scratch_removed"]
    if output:
        output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def test_native_trace_quota_admission(tmp_path):
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("explicit jim-eq user-quota qualification opt-in required")
    main(tmp_path / "quota-admission.json")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), sys.argv[3])
    else:
        main(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
