"""User-managed, fixed-size macOS trace volume. No same-user tamper guarantee."""

from __future__ import annotations

import json
import os
import plistlib
import stat
import subprocess
from pathlib import Path
from uuid import uuid4

from .trace_quota import TRACE_QUOTA_BYTES, TraceQuotaError

IMAGE_BYTES = 512 * 1024 * 1024
MANIFEST = "trace-volume.json"
IMAGE = "trace-volume.dmg"


def _command(*args: str) -> bytes:
    try:
        return subprocess.run(
            args, check=True, capture_output=True, timeout=120, umask=0o077
        ).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise TraceQuotaError(f"macOS storage command failed: {args[0]}") from error


def _plist(*args: str) -> dict:
    try:
        result = plistlib.loads(_command(*args))
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (ValueError, plistlib.InvalidFileException) as error:
        raise TraceQuotaError("invalid macOS storage diagnostics") from error


def _private(path: Path, *, directory: bool = False) -> os.stat_result:
    info = path.lstat()
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (
        not valid_type
        or info.st_uid != os.geteuid()
        or info.st_mode & 0o077
        or (not directory and info.st_nlink != 1)
    ):
        raise TraceQuotaError(
            "storage requires private, owned directories and single-link files"
        )
    return info


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _record(state: Path, value: dict) -> None:
    temporary = state / (MANIFEST + ".tmp")
    fd = os.open(
        temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(fd, "w") as target:
        json.dump(value, target, sort_keys=True)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, state / MANIFEST)
    _sync(state)


def _read(state: Path) -> dict:
    _private(state, directory=True)
    _private(state / MANIFEST)
    try:
        value = json.loads((state / MANIFEST).read_text())
        if value["version"] != 1 or value["phase"] not in {
            "creating",
            "mounting",
            "ready",
        }:
            raise ValueError
        if not isinstance(value["label"], str) or not value["label"].startswith(
            "ECTrace-"
        ):
            raise ValueError
        return value
    except (ValueError, KeyError, TypeError) as error:
        raise TraceQuotaError("invalid trace volume manifest") from error


def _image(state: Path, record: dict) -> None:
    info = _private(state / IMAGE)
    if (
        info.st_size != IMAGE_BYTES
        or info.st_blocks * 512 < IMAGE_BYTES
        or [info.st_dev, info.st_ino] != record.get("image_identity")
    ):
        raise TraceQuotaError("trace image is substituted, sparse or incorrectly sized")
    image = _plist("/usr/bin/hdiutil", "imageinfo", "-plist", str(state / IMAGE))
    if (
        image.get("Format") != "UDRW"
        or image.get("Size Information", {}).get("Total Bytes") != IMAGE_BYTES
    ):
        raise TraceQuotaError("trace image must be a fixed 512 MiB UDRW image")


def _volume(state: Path, record: dict) -> dict:
    trace = state / "trace"
    if trace.is_symlink() or not os.path.ismount(trace):
        raise TraceQuotaError("trace volume is not mounted")
    volume = _plist("/usr/sbin/diskutil", "info", "-plist", str(trace))
    images = _plist("/usr/bin/hdiutil", "info", "-plist").get("images", [])
    matches = [
        image
        for image in images
        if any(
            entity.get("dev-entry") == volume.get("DeviceNode")
            and entity.get("mount-point") == str(trace)
            for entity in image.get("system-entities", [])
        )
    ]
    if (
        len(matches) != 1
        or Path(matches[0].get("image-path", "")).resolve() != state / IMAGE
        or matches[0].get("owner-uid") != os.geteuid()
    ):
        raise TraceQuotaError(
            "trace mount does not belong to the recorded backing image"
        )
    if (
        volume.get("MountPoint") != str(trace)
        or volume.get("FilesystemType") != "hfs"
        or volume.get("FilesystemName") != "Journaled HFS+"
        or volume.get("GlobalPermissionsEnabled") is not True
        or volume.get("WritableVolume") is not True
        or volume.get("VolumeName") != record["label"]
        or not TRACE_QUOTA_BYTES < volume.get("TotalSize", 0) <= IMAGE_BYTES
    ):
        raise TraceQuotaError(
            "trace mount format, ownership mode or capacity is invalid"
        )
    if record["phase"] == "ready" and (
        volume.get("VolumeUUID") != record.get("volume_uuid")
        or volume.get("TotalSize") != record.get("volume_bytes")
    ):
        raise TraceQuotaError("trace volume identity or size changed")
    if trace.stat().st_dev == state.stat().st_dev:
        raise TraceQuotaError("trace and task storage require separate filesystems")
    return volume


def verify_macos_storage(trace: Path, state: Path) -> dict:
    """Read-only admission. Never mount or fall back to an ordinary directory."""
    try:
        if (
            not state.is_absolute()
            or state.resolve() != state
            or trace != state / "trace"
        ):
            raise TraceQuotaError(
                "storage paths must use the canonical absolute layout"
            )
        record = _read(state)
        if record["phase"] != "ready":
            raise TraceQuotaError("trace storage setup is incomplete")
        _image(state, record)
        volume = _volume(state, record)
        _private(trace, directory=True)
        for path in (state / "payload.key", state / "agentd-tasks.sqlite3"):
            if path.exists() or path.is_symlink():
                if _private(path).st_dev == trace.stat().st_dev:
                    raise TraceQuotaError(
                        "task state and key must remain outside trace storage"
                    )
        for path in trace.iterdir():
            if path.name.startswith(("agentd.sqlite3", "writer.lock")):
                _private(path)
        return {
            "backend": "macos_fixed_image",
            "mount_verified": True,
            "trust_boundary": "user_managed",
            "physical_limit_bytes": IMAGE_BYTES,
            "volume_bytes": volume["TotalSize"],
            "available_bytes": volume["FreeSpace"],
            "admission_limit_bytes": TRACE_QUOTA_BYTES,
            "inode_limit": None,
            "volume_uuid": record["volume_uuid"],
            "migration_status": "complete",
        }
    except (OSError, KeyError, TypeError) as error:
        raise TraceQuotaError("macOS trace storage could not be verified") from error


def mount_macos_storage(state: Path) -> dict:
    """Startup mounts only provisioned images, under the service writer lock."""
    record = _read(state)
    if record["phase"] != "ready":
        raise TraceQuotaError("run storage setup to complete trace provisioning")
    _image(state, record)
    trace = state / "trace"
    if not os.path.ismount(trace):
        _private(trace, directory=True)
        if any(trace.iterdir()):
            raise TraceQuotaError("unmounted trace directory contains unrelated state")
        _command(
            "/usr/bin/hdiutil",
            "attach",
            "-plist",
            "-nobrowse",
            "-owners",
            "on",
            "-mountpoint",
            str(trace),
            str(state / IMAGE),
        )
    return verify_macos_storage(trace, state)


def setup_macos_storage(state: Path) -> dict:
    """Idempotent provisioning; caller holds the state-directory writer lock."""
    state = state.resolve(strict=True)
    _private(state, directory=True)
    trace, image = state / "trace", state / IMAGE
    marker = state / MANIFEST
    if not marker.exists() and not marker.is_symlink():
        working = state / "trace-volume-working.dmg"
        if (
            image.exists()
            or image.is_symlink()
            or trace.is_symlink()
            or os.path.ismount(trace)
            or working.exists()
            or working.is_symlink()
        ):
            raise TraceQuotaError("refusing unrelated existing trace storage")
        if trace.exists() and any(trace.iterdir()):
            raise TraceQuotaError("refusing nonempty trace mount directory")
        _record(
            state,
            {"version": 1, "phase": "creating", "label": "ECTrace-" + uuid4().hex[:16]},
        )
    record = _read(state)
    if record["phase"] == "ready":
        return mount_macos_storage(state)
    if record["phase"] == "creating":
        # An interrupted hdiutil create leaves its owned working image. Never
        # overwrite a final image without its recorded inode identity.
        temporary = state / "trace-volume-working.dmg"
        if image.exists() or image.is_symlink():
            raise TraceQuotaError("unrecorded final image requires investigation")
        if temporary.exists() or temporary.is_symlink():
            _private(temporary)
            temporary.unlink()
        _command(
            "/usr/bin/hdiutil",
            "create",
            "-size",
            "512m",
            "-fs",
            "HFS+J",
            "-type",
            "UDIF",
            "-volname",
            record["label"],
            "-o",
            str(temporary),
        )
        temporary.chmod(0o600)
        _sync(temporary)
        info = temporary.stat()
        record.update(phase="mounting", image_identity=[info.st_dev, info.st_ino])
        _record(state, record)
        os.replace(temporary, image)
        _sync(state)
    if not image.exists() and (state / "trace-volume-working.dmg").exists():
        temporary = state / "trace-volume-working.dmg"
        info = _private(temporary)
        if [info.st_dev, info.st_ino] != record["image_identity"]:
            raise TraceQuotaError("working image identity changed")
        os.replace(temporary, image)
        _sync(state)
    _image(state, record)
    if not os.path.ismount(trace):
        trace.mkdir(mode=0o700, exist_ok=True)
        _private(trace, directory=True)
        if any(trace.iterdir()):
            raise TraceQuotaError("refusing nonempty trace mount directory")
        _command(
            "/usr/bin/hdiutil",
            "attach",
            "-plist",
            "-nobrowse",
            "-owners",
            "on",
            "-mountpoint",
            str(trace),
            str(image),
        )
    volume = _volume(state, record)
    trace.chmod(0o700)
    record.update(
        phase="ready",
        volume_uuid=volume["VolumeUUID"],
        volume_bytes=volume["TotalSize"],
    )
    _record(state, record)
    return verify_macos_storage(trace, state)
