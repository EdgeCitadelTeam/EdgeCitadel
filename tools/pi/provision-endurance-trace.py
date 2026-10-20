#!/usr/bin/env python3
"""One-time root provisioning for the actual EdgeCitadel Linux trace contract.
Creates only a dedicated 512 MiB image and its mount; never formats a real disk.
Run on NetSys-Endurance: sudo python3 /home/zhonghao/edgecitadel-pi/provision-trace.py
"""

import ctypes
import os
import pwd
import subprocess
from pathlib import Path

assert os.geteuid() == 0, "Run with sudo"
assert os.uname().nodename.lower() in {"netsys-endurance", "endurance"}, "Wrong host"
user = pwd.getpwnam("zhonghao")
state = Path("/home/zhonghao/.edgecitadel/agentd")
assert state.is_dir() and state.stat().st_uid == user.pw_uid
trace = state / "trace"
assert not trace.is_symlink()
root = Path("/var/lib/edgecitadel-endurance")
root.mkdir(mode=0o700, exist_ok=True)
assert root.stat().st_uid == 0
image = root / "trace.ext4"
trace.mkdir(mode=0o700, exist_ok=True)
if not os.path.ismount(trace):
    assert not any(trace.iterdir()), "Refusing to cover existing trace data"
    if not image.exists():
        fd = os.open(image, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.posix_fallocate(fd, 0, 512 * 1024 * 1024)
        finally:
            os.close(fd)
        subprocess.run(
            [
                "/usr/sbin/mkfs.ext4",
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
    subprocess.run(
        ["mount", "-o", "loop,usrquota,nosuid,nodev,noexec", str(image), str(trace)],
        check=True,
    )
assert image.stat().st_uid == 0
os.chown(trace, user.pw_uid, user.pw_gid)
trace.chmod(0o700)
# The empty ext4 recovery directory is unnecessary in this dedicated SQLite volume.
lost = trace / "lost+found"
if lost.exists():
    assert lost.is_dir() and not any(lost.iterdir())
    lost.rmdir()


class Quota(ctypes.Structure):
    _fields_ = [
        (k, ctypes.c_uint64)
        for k in ("hard", "soft", "space", "ihard", "isoft", "inodes", "btime", "itime")
    ] + [("valid", ctypes.c_uint32)]


q = Quota(hard=256 * 1024, ihard=128, valid=5)
libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long
fd = os.open(trace, os.O_RDONLY | os.O_DIRECTORY)
try:
    rc = libc.syscall(
        ctypes.c_long(443),
        ctypes.c_int(fd),
        ctypes.c_uint(0x800008 << 8),
        ctypes.c_uint(user.pw_uid),
        ctypes.byref(q),
    )
    if rc:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
finally:
    os.close(fd)
entry = f"{image} {trace} ext4 loop,usrquota,nosuid,nodev,noexec,nofail 0 0"
fstab = Path("/etc/fstab")
text = fstab.read_text()
existing = [
    line
    for line in text.splitlines()
    if line.strip() and not line.lstrip().startswith("#") and str(trace) in line.split()
]
assert not existing or existing == [entry], (
    "Conflicting existing mount entry; retained unchanged"
)
if not existing:
    with fstab.open("a") as f:
        f.write("\n# EdgeCitadel Endurance trace quota volume\n" + entry + "\n")
subprocess.run(["systemctl", "daemon-reload"], check=True)
# Exercise the production verifier with the actual non-root service identity.
subprocess.run(
    [
        "runuser",
        "-u",
        "zhonghao",
        "--",
        "setpriv",
        "--inh-caps=-all",
        "--ambient-caps=-all",
        "--no-new-privs",
        "/home/zhonghao/edgecitadel-pi/venv/bin/python",
        "-m",
        "edgecitadel_agentd.storage_setup",
        "--state-dir",
        str(state),
    ],
    check=True,
)
print("Trace storage verified. Ready to start the production Endurance Pi agent.")
