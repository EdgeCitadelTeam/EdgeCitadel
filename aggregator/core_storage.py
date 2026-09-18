"""Provisioned Core manifest and leases; callers must still attest database roles.

No provisioning or implicit filesystem creation occurs here. The authority root
and its ancestors must be controlled by the provisioner. Hold a lifecycle lease
while resolving paths and for the full lifetime of any resulting connection.
These primitives are not yet wired into application startup.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal
from uuid import UUID

MAX_MANIFEST_BYTES = 4096
Role = Literal["trace", "mirror"]
Lease = Literal["reader", "collector", "maintenance"]


class StorageUnavailable(RuntimeError):
    """Fixed, non-sensitive reason for refusing provisioned storage."""


def _regular_fd(path: Path) -> int:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise StorageUnavailable("storage_file_unavailable") from None
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise StorageUnavailable("storage_file_unavailable")
    return fd


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _relative_file(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("invalid path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in ("", ".", "..") for p in value.split("/")):
        raise ValueError("invalid path")
    return value


@dataclass(frozen=True)
class StorageManifest:
    """Persistent pair identity; generation is not a mount token or history epoch."""

    generation: str
    trace: str
    mirror: str

    def path(self, root: Path, role: Role) -> Path:
        """Resolve an existing regular file without opening SQLite or creating it.

        Database identity, persistent write fences, physical volume identity and
        mount-session authority are separate checks required before use.
        """
        if role not in ("trace", "mirror"):
            raise ValueError("invalid storage role")
        root = Path(root)
        parts = PurePosixPath(getattr(self, role)).parts
        try:
            if root.is_symlink() or not root.is_dir():
                raise StorageUnavailable("storage_path_unavailable")
            path = root
            for part in parts:
                path = path / part
                if path.is_symlink():
                    raise StorageUnavailable("storage_path_unavailable")
            fd = _regular_fd(path)
            os.close(fd)
        except OSError:
            raise StorageUnavailable("storage_path_unavailable") from None
        return path

    def paths(self, root: Path) -> dict[Role, Path]:
        """Resolve both roles and reject aliases before any SQLite connection."""
        paths: dict[Role, Path] = {
            "trace": self.path(root, "trace"),
            "mirror": self.path(root, "mirror"),
        }
        try:
            identities = [(p.stat().st_dev, p.stat().st_ino) for p in paths.values()]
        except OSError:
            raise StorageUnavailable("storage_path_unavailable") from None
        if identities[0] == identities[1]:
            raise StorageUnavailable("storage_roles_overlap")
        return paths


def read_manifest(root: Path) -> StorageManifest:
    """Read a bounded, exact-version ACTIVE record under a lifecycle lease."""
    fd = _regular_fd(Path(root) / "storage.json")
    try:
        with os.fdopen(fd, "rb") as source:
            encoded = source.read(MAX_MANIFEST_BYTES + 1)
    except OSError:
        raise StorageUnavailable("storage_file_unavailable") from None
    try:
        if len(encoded) > MAX_MANIFEST_BYTES:
            raise ValueError("oversize manifest")
        record = json.loads(encoded, object_pairs_hook=_object)
        if not isinstance(record, dict) or set(record) != {
            "version",
            "state",
            "generation",
            "trace",
            "mirror",
        }:
            raise ValueError("unknown manifest fields")
        if type(record["version"]) is not int or record["version"] != 1:
            raise ValueError("unknown version")
        if record["state"] != "active":
            raise StorageUnavailable("storage_activation_pending")
        generation = record["generation"]
        if not isinstance(generation, str) or str(UUID(generation)) != generation:
            raise ValueError("invalid generation")
        trace, mirror = (_relative_file(record[role]) for role in ("trace", "mirror"))
        if trace == mirror:
            raise ValueError("overlapping roles")
        return StorageManifest(generation, trace, mirror)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise StorageUnavailable("storage_manifest_invalid") from None


@contextmanager
def storage_lease(root: Path, kind: Lease) -> Iterator[None]:
    """Lock pre-existing stable inodes; do not create, chmod, or unlink them.

    Readers and the collector share lifecycle ownership. Maintenance excludes
    every owner; the collector additionally owns a separate exclusive writer
    lease. Locks are always acquired lifecycle first and released in reverse.
    """
    if kind not in ("reader", "collector", "maintenance"):
        raise ValueError("invalid storage lease")
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise StorageUnavailable("storage_authority_unavailable")
    descriptors = []
    try:
        lifecycle = _regular_fd(root / "lifecycle.lock")
        descriptors.append(lifecycle)
        operation = fcntl.LOCK_EX if kind == "maintenance" else fcntl.LOCK_SH
        try:
            fcntl.flock(lifecycle, operation | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StorageUnavailable("storage_owner_busy") from None
        if kind == "collector":
            writer = _regular_fd(root / "collector.lock")
            descriptors.append(writer)
            try:
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise StorageUnavailable("storage_owner_busy") from None
        yield
    finally:
        for fd in reversed(descriptors):
            os.close(fd)
