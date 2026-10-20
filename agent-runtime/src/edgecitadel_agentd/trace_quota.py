"""Read-only verification of the qualified Linux user-quota boundary.

This verifies kernel enforcement, not free completion capacity. The caller must
hold storage ownership through verification/open and supply separate task storage.
Administrator changes to mounts or quotas invalidate the deployment contract.
"""

from __future__ import annotations

import ctypes
import os
import platform
import stat
from dataclasses import dataclass
from pathlib import Path

TRACE_QUOTA_BYTES = 256 * 1024 * 1024
TRACE_QUOTA_INODES = 128
# Linux UAPI: x86-64 syscall_64.tbl and asm-generic/unistd.h (aarch64).
_QUOTACTL_FD = 443
_GET_QUOTA = 0x800007 << 8  # QCMD(Q_GETQUOTA, USRQUOTA); limits use 1024-byte units.
_GET_STATE = 0x5808 << 8  # QCMD(Q_XGETQSTATV, USRQUOTA), also supported by ext4.
_USER_ACCOUNTING_AND_ENFORCEMENT = 3


class TraceQuotaError(RuntimeError):
    """The configured trace storage cannot prove the supported contract."""


@dataclass(frozen=True)
class TraceQuota:
    uid: int
    device: int
    hard_bytes: int
    allocated_bytes: int
    hard_inodes: int
    allocated_inodes: int


class _Quota(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_uint64)
        for name in (
            "hard",
            "soft",
            "space",
            "inode_hard",
            "inode_soft",
            "inodes",
            "block_time",
            "inode_time",
        )
    ] + [("valid", ctypes.c_uint32)]


class _QuotaState(ctypes.Structure):
    # fs_quota_statv version 1 is 160 bytes; only its version/flags prefix is used.
    _fields_ = [
        ("version", ctypes.c_uint8),
        ("padding", ctypes.c_uint8),
        ("flags", ctypes.c_uint16),
        ("remaining", ctypes.c_uint8 * 156),
    ]


def _query(fd: int, command: int, uid: int, value: ctypes.Structure) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    syscall = libc.syscall
    syscall.argtypes = [
        ctypes.c_long,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.c_uint,
        ctypes.c_void_p,
    ]
    syscall.restype = ctypes.c_long
    if syscall(_QUOTACTL_FD, fd, command, uid, ctypes.byref(value)) != 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))


def _read_quota(fd: int, uid: int) -> tuple[_QuotaState, _Quota]:
    state, quota = _QuotaState(version=1), _Quota()
    _query(fd, _GET_STATE, 0, state)
    _query(fd, _GET_QUOTA, uid, quota)
    return state, quota


def _writer_uid() -> int:
    if (
        platform.system() != "Linux"
        or platform.machine() not in {"x86_64", "aarch64"}
        or ctypes.sizeof(ctypes.c_long) != 8
    ):
        raise TraceQuotaError("trace quota requires supported 64-bit Linux")
    fields = {
        key: value.split()
        for line in Path("/proc/thread-self/status").read_text().splitlines()
        if ":" in line
        for key, value in [line.split(":", 1)]
    }
    ids = [int(value) for value in fields["Uid"]]
    if len(ids) != 4 or not ids[0] or len(set(ids)) != 1:
        raise TraceQuotaError(
            "trace writer requires one non-root real/effective/saved/filesystem UID"
        )
    if any(
        int(fields[name][0], 16) for name in ("CapInh", "CapPrm", "CapEff", "CapAmb")
    ):
        raise TraceQuotaError(
            "trace writer must have no inheritable, permitted, effective or ambient capabilities"
        )
    if fields["NoNewPrivs"] != ["1"]:
        raise TraceQuotaError("trace writer requires no_new_privs")
    return ids[0]


def _filesystem_type(fd: int) -> str:
    mount_id = next(
        line.split()[1]
        for line in Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
        if line.startswith("mnt_id:")
    )
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        if before.split()[0] == mount_id:
            return after.split()[0]
    raise TraceQuotaError("trace filesystem mount is not visible")


def _verify_contents(fd: int, uid: int, device: int) -> None:
    # The fixed inode ceiling bounds this scan. Do not traverse unowned links or
    # foreign filesystems; a differently-owned SQLite file would evade the UID quota.
    pending = [os.dup(fd)]
    seen = 0
    try:
        while pending:
            current = pending.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        seen += 1
                        if seen >= TRACE_QUOTA_INODES:
                            raise TraceQuotaError(
                                "trace directory exceeds its inode inventory bound"
                            )
                        info = entry.stat(follow_symlinks=False)
                        if info.st_uid != uid or info.st_dev != device:
                            raise TraceQuotaError(
                                "trace entries must belong to the service UID and quota filesystem"
                            )
                        if stat.S_ISDIR(info.st_mode):
                            pending.append(
                                os.open(
                                    entry.name,
                                    os.O_RDONLY
                                    | os.O_DIRECTORY
                                    | os.O_NOFOLLOW
                                    | os.O_CLOEXEC,
                                    dir_fd=current,
                                )
                            )
                        elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                            raise TraceQuotaError(
                                "trace entries must be private directories or single-link regular files"
                            )
            finally:
                os.close(current)
    finally:
        for current in pending:
            os.close(current)


def verify_trace_quota(trace_directory: Path, task_directory: Path) -> TraceQuota:
    """Refuse absent, accounting-only, bypassable or misattributed enforcement.

    This function never creates directories, changes quota limits or drops user
    privileges. Provisioning is an administrator responsibility. Q_XGETQSTATV
    checks enforcement separately from the quota record's configured hard limit.
    Both queries use the same opened directory descriptor, not a device pathname.
    """
    descriptors = []
    try:
        uid = _writer_uid()
        for directory in (trace_directory, task_directory):
            if not directory.is_absolute():
                raise TraceQuotaError("storage directories must be absolute")
            descriptors.append(
                os.open(
                    directory,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                )
            )
        trace_fd, task_fd = descriptors
        trace, task = os.fstat(trace_fd), os.fstat(task_fd)
        if trace.st_dev == task.st_dev:
            raise TraceQuotaError("trace and task storage require separate filesystems")
        if any(info.st_uid != uid or info.st_mode & 0o077 for info in (trace, task)):
            raise TraceQuotaError(
                "storage directories must be private and owned by the service UID"
            )
        if _filesystem_type(trace_fd) != "ext4":
            raise TraceQuotaError(
                "trace storage requires the qualified ext4 filesystem"
            )
        state, quota = _read_quota(trace_fd, uid)
        if (
            state.version != 1
            or state.flags & _USER_ACCOUNTING_AND_ENFORCEMENT
            != _USER_ACCOUNTING_AND_ENFORCEMENT
        ):
            raise TraceQuotaError(
                "trace user quota accounting and enforcement must both be active"
            )
        if quota.valid & 15 != 15:
            raise TraceQuotaError("trace quota limits and usage are incomplete")
        if (
            quota.hard * 1024 != TRACE_QUOTA_BYTES
            or quota.inode_hard != TRACE_QUOTA_INODES
        ):
            raise TraceQuotaError(
                "trace quota requires 256 MiB and 128 inode hard limits"
            )
        if quota.space > TRACE_QUOTA_BYTES or quota.inodes > TRACE_QUOTA_INODES:
            raise TraceQuotaError("trace quota is already over its hard limit")
        _verify_contents(trace_fd, uid, trace.st_dev)
        return TraceQuota(
            uid,
            trace.st_dev,
            quota.hard * 1024,
            quota.space,
            quota.inode_hard,
            quota.inodes,
        )
    except (OSError, KeyError, ValueError, StopIteration, IndexError) as error:
        raise TraceQuotaError(
            "trace quota enforcement could not be verified"
        ) from error
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
