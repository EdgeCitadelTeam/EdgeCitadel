#!/usr/bin/env python3
"""Administrator provisioning of a new dedicated Linux trace-quota account.

Requires matching Agent runtime dependencies in a UID-readable environment.
Creates new resources only; interrupted provisioning is retained for inspection.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import pwd
import re
import shutil
import subprocess
import sys
from pathlib import Path

from edgecitadel_agentd import trace_quota


_STORAGE = Path("/var/lib/edgecitadel-trace")
_UNITS = Path("/etc/systemd/system")


def _run(*args: str) -> None:
    subprocess.run(args, check=True)


def _mount_unit(path: Path) -> str:
    return subprocess.check_output(
        ["systemd-escape", "--path", "--suffix=mount", str(path)], text=True
    ).strip()


def provision(name: str) -> dict[str, object]:
    if os.geteuid() != 0 or platform.system() != "Linux":
        raise RuntimeError("provisioning requires a Linux administrator")
    if (
        platform.machine() not in {"x86_64", "aarch64"}
        or ctypes.sizeof(ctypes.c_long) != 8
    ):
        raise RuntimeError("provisioning requires supported 64-bit Linux")
    if not re.fullmatch(r"[a-z][a-z0-9]{0,20}", name):
        raise ValueError(
            "name must be 1–21 lowercase letters/digits, starting with a letter"
        )
    account = "edgecitadel-" + name
    home = Path("/var/lib") / account
    state = home / "state"
    agentd = state / "agentd"
    trace = agentd / "trace"
    image = _STORAGE / (name + ".ext4")
    mount = _STORAGE / ("mount-" + name)
    volume_unit, trace_unit = _mount_unit(mount), _mount_unit(trace)
    try:
        pwd.getpwnam(account)
    except KeyError:
        pass
    else:
        raise RuntimeError(
            "service account already exists; inspect instead of reprovisioning"
        )
    for path in (home, image, mount, _UNITS / volume_unit, _UNITS / trace_unit):
        if path.exists() or path.is_symlink():
            raise RuntimeError(f"provisioning destination already exists: {path}")
    for program in ("mkfs.ext4", "useradd", "systemctl", "loginctl"):
        if shutil.which(program) is None:
            raise RuntimeError(f"required administrator command is missing: {program}")
    if _STORAGE.exists() or _STORAGE.is_symlink():
        info = _STORAGE.lstat()
        if (
            _STORAGE.is_symlink()
            or not _STORAGE.is_dir()
            or info.st_uid != 0
            or info.st_mode & 0o077
        ):
            raise RuntimeError("trace image directory must be private and root-owned")
    else:
        _STORAGE.mkdir(mode=0o700)
    _run(
        "useradd",
        "--system",
        "--user-group",
        "--home-dir",
        str(home),
        "--create-home",
        "--shell",
        "/usr/sbin/nologin",
        account,
    )
    identity = pwd.getpwnam(account)
    home.chmod(0o700)
    for directory in (state, agentd, trace):
        directory.mkdir(mode=0o700)
        os.chown(directory, identity.pw_uid, identity.pw_gid)
    mount.mkdir(mode=0o700)
    # This administrator-owned allocation includes shared ext4 infrastructure.
    # The service UID's independent hard allocation limit remains exactly 256 MiB.
    descriptor = os.open(image, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        os.posix_fallocate(output.fileno(), 0, 512 * 1024 * 1024)
        os.fsync(output.fileno())
    _run(
        "mkfs.ext4",
        "-q",
        "-F",
        "-O",
        "quota",
        "-E",
        "quotatype=usrquota,nodiscard",
        str(image),
    )
    (_UNITS / volume_unit).write_text(
        "[Unit]\nDescription=EdgeCitadel administrator-owned trace filesystem\n\n"
        f"[Mount]\nWhat={image}\nWhere={mount}\nType=ext4\n"
        "Options=loop,usrquota,nosuid,nodev,noexec\n\n[Install]\nWantedBy=local-fs.target\n"
    )
    _run("systemctl", "daemon-reload")
    _run("systemctl", "enable", "--now", volume_unit)
    descriptor = os.open(mount, os.O_RDONLY | os.O_DIRECTORY)
    try:
        status, current = trace_quota._read_quota(descriptor, identity.pw_uid)
        if (
            status.flags & 3 != 3
            or current.space
            or current.inodes
            or current.hard
            or current.inode_hard
        ):
            raise RuntimeError(
                "new filesystem/UID does not have empty enforced user accounting"
            )
        limit = trace_quota._Quota(
            hard=trace_quota.TRACE_QUOTA_BYTES // 1024,
            inode_hard=trace_quota.TRACE_QUOTA_INODES,
            valid=5,
        )
        trace_quota._query(descriptor, 0x800008 << 8, identity.pw_uid, limit)
    finally:
        os.close(descriptor)
    private_trace = mount / "trace"
    private_trace.mkdir(mode=0o700)
    os.chown(private_trace, identity.pw_uid, identity.pw_gid)
    (_UNITS / trace_unit).write_text(
        "[Unit]\nDescription=EdgeCitadel private trace allocation\n"
        f"Requires={volume_unit}\nAfter={volume_unit}\n\n"
        f"[Mount]\nWhat={private_trace}\nWhere={trace}\nType=none\nOptions=bind\n\n"
        "[Install]\nWantedBy=local-fs.target\n"
    )
    _run("systemctl", "daemon-reload")
    _run("systemctl", "enable", "--now", trace_unit)
    # Use the actual service UID, not root's ability to query its quota. The
    # writer verifier independently checks capabilities, ownership and isolation.
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import ctypes,dataclasses,json,sys; from pathlib import Path; "
            "from edgecitadel_agentd.trace_quota import verify_trace_quota; "
            "assert ctypes.CDLL(None).prctl(38,1,0,0,0)==0; "
            "print(json.dumps(dataclasses.asdict(verify_trace_quota(Path(sys.argv[1]),Path(sys.argv[2])))))",
            str(trace),
            str(agentd),
        ],
        user=identity.pw_uid,
        group=identity.pw_gid,
        extra_groups=[],
        cwd=home,
        capture_output=True,
        text=True,
        check=True,
    )
    _run("loginctl", "enable-linger", account)
    return {
        "account": account,
        "uid": identity.pw_uid,
        "gid": identity.pw_gid,
        "state_directory": str(state),
        "image": str(image),
        "mount_units": [volume_unit, trace_unit],
        "quota": json.loads(probe.stdout),
        "daemon_started": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    print(json.dumps(provision(args.name), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
