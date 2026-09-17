"""Protected mounted-volume session records; no mount or application activation.

A trusted root provisioner must attest the physical volume before publishing.
Callers retain a lifecycle lease during verification and database use. A supplied
expected ticket comes from a fresh trusted launcher, never from disk discovery.
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from uuid import UUID

from .core_storage import StorageUnavailable, read_manifest, storage_lease

MAX_SESSION_BYTES = 4096


@dataclass(frozen=True)
class MountSession:
    mount_generation: str
    storage_generation: str
    volume_uuid: str
    device: int
    filesystem_bytes: int
    state: str = "active"
    version: int = 1

    def __post_init__(self) -> None:
        try:
            for value in (
                self.mount_generation,
                self.storage_generation,
                self.volume_uuid,
            ):
                if not isinstance(value, str) or str(UUID(value)) != value:
                    raise ValueError
            if (
                type(self.version) is not int
                or self.version != 1
                or self.state not in ("prepared", "active")
            ):
                raise ValueError
            if any(
                type(value) is not int or value <= 0
                for value in (self.device, self.filesystem_bytes)
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise StorageUnavailable("session_record_invalid") from None


def decode_session(encoded: bytes) -> MountSession:
    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value = dict(pairs)
        if len(value) != len(pairs):
            raise ValueError
        return value

    try:
        if len(encoded) > MAX_SESSION_BYTES:
            raise ValueError
        record = json.loads(encoded, object_pairs_hook=object_pairs)
        if not isinstance(record, dict) or set(record) != set(
            MountSession.__dataclass_fields__
        ):
            raise ValueError
        return MountSession(**record)
    except (ValueError, TypeError, RecursionError):
        raise StorageUnavailable("session_record_invalid") from None


def _protected(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected_type(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise StorageUnavailable("session_authority_unprotected")


def _read(path: Path) -> MountSession:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise StorageUnavailable("session_authority_unprotected")
        return decode_session(source.read(MAX_SESSION_BYTES + 1))


def _locations(root: Path, mount: Path, expected: MountSession) -> tuple[Path, Path]:
    for path in (root, mount, mount / "authority"):
        _protected(path, directory=True)
    for name in ("storage.json", "lifecycle.lock", "collector.lock"):
        _protected(root / name)
    if (
        mount.stat().st_dev == root.stat().st_dev
        or mount.stat().st_dev != expected.device
    ):
        raise StorageUnavailable("session_mount_unavailable")
    info = os.statvfs(mount)
    if info.f_blocks * info.f_frsize != expected.filesystem_bytes:
        raise StorageUnavailable("session_capacity_mismatch")
    if read_manifest(root).generation != expected.storage_generation:
        raise StorageUnavailable("session_storage_mismatch")
    return (root / "session.json", mount / "authority/session.json")


def verify_session(root: Path, mount: Path, expected: MountSession) -> None:
    """Check protected records against fresh launcher authority under a lease."""
    if os.geteuid() == 0:
        raise StorageUnavailable("session_application_privileged")
    if expected.state != "active":
        raise StorageUnavailable("session_unavailable")
    try:
        paths = _locations(root, mount, expected)
        for path in paths:
            temporary = path.with_suffix(".tmp")
            if temporary.exists() or temporary.is_symlink():
                raise StorageUnavailable("session_publication_pending")
            if _read(path) != expected:
                raise StorageUnavailable("session_mismatch")
    except OSError:
        raise StorageUnavailable("session_unavailable") from None


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish_record(path: Path, record: MountSession) -> None:
    temporary = path.with_suffix(".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, "w") as out:
        os.fchmod(out.fileno(), 0o644)
        json.dump(asdict(record), out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)
    _sync_dir(path.parent)


def publish_session(root: Path, mount: Path, session: MountSession) -> None:
    """Publish a fresh session on an already-attested mount, or recover a fence.

    Does not attach storage, infer volume UUID, initialize a database, or launch
    a writer. Root must supply a new generation after checking physical identity.
    Invalid surviving records refuse before cleanup. Process-kill recovery may
    retry with a new generation; hardware power loss needs separate qualification.
    """
    if os.geteuid() != 0 or session.state != "active":
        raise StorageUnavailable("session_provisioner_required")
    try:
        with storage_lease(root, "maintenance"):
            paths = _locations(root, mount, session)
            temporaries = []
            for path in paths:
                for candidate in (path, path.with_suffix(".tmp")):
                    if candidate.exists() or candidate.is_symlink():
                        old = _read(candidate)
                        if (old.storage_generation, old.volume_uuid) != (
                            session.storage_generation,
                            session.volume_uuid,
                        ):
                            raise StorageUnavailable("session_lineage_mismatch")
                        if old.mount_generation == session.mount_generation:
                            raise StorageUnavailable("session_generation_reused")
                        if candidate.suffix == ".tmp":
                            temporaries.append(candidate)
            for temporary in temporaries:
                temporary.unlink()
                _sync_dir(temporary.parent)
            _publish_record(paths[0], replace(session, state="prepared"))
            _publish_record(paths[1], session)
            _publish_record(paths[0], session)
    except OSError:
        raise StorageUnavailable("session_publication_unavailable") from None
