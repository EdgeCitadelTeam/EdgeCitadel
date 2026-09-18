"""Persistent private Core cursor key; explicit startup provisioning only."""

from __future__ import annotations

import os
import secrets
import stat
import tempfile
from pathlib import Path


def load_or_create_key(path: Path) -> bytes:
    """Atomically publish a complete key without replacing a concurrent winner.

    The caller owns the existing parent directory. Invalid, public or symlinked
    keys fail closed rather than silently invalidating previously issued cursors.
    """
    temporary = None
    try:
        try:
            descriptor = os.open(
                path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
            )
        except FileNotFoundError:
            descriptor, temporary = tempfile.mkstemp(
                prefix=".trace-key-", dir=path.parent
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(secrets.token_bytes(32))
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
            os.unlink(temporary)
            temporary = None
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            descriptor = os.open(
                path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
            )
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ValueError("trace_cursor_key_unavailable")
            key = stream.read(33)
            if len(key) != 32:
                raise ValueError("trace_cursor_key_unavailable")
            return key
    except OSError:
        raise ValueError("trace_cursor_key_unavailable") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            except OSError:
                raise ValueError("trace_cursor_key_unavailable") from None
