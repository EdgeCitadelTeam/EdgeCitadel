import json
from dataclasses import FrozenInstanceError, asdict
from uuid import uuid4

import pytest

from aggregator.core_storage import StorageUnavailable
from aggregator.core_storage_session import (
    MountSession,
    decode_session,
    publish_session,
)


def ticket():
    return MountSession(str(uuid4()), str(uuid4()), str(uuid4()), 64, 64 * 1024**2)


def test_session_roundtrip_and_immutable():
    session = ticket()
    assert decode_session(json.dumps(asdict(session)).encode()) == session
    with pytest.raises(FrozenInstanceError):
        session.device = 65


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 2},
        {"state": "unknown"},
        {"mount_generation": "bad"},
        {"storage_generation": None},
        {"volume_uuid": 1},
        {"device": True},
        {"device": -1},
        {"filesystem_bytes": 0},
        {"unknown": 1},
    ],
)
def test_invalid_session(change):
    record = {**asdict(ticket()), **change}
    with pytest.raises(StorageUnavailable, match="session_record_invalid"):
        decode_session(json.dumps(record).encode())


@pytest.mark.parametrize(
    "encoded",
    [
        b"{",
        b"[]",
        b" " * 4097,
        b"[" * 2000 + b"]" * 2000,
        b'{"version":1,"version":1}',
        b"\xff",
    ],
)
def test_corrupt_session(encoded):
    with pytest.raises(StorageUnavailable, match="session_record_invalid"):
        decode_session(encoded)


def test_application_cannot_publish(tmp_path, monkeypatch):
    monkeypatch.setattr("aggregator.core_storage_session.os.geteuid", lambda: 65534)
    with pytest.raises(StorageUnavailable, match="session_provisioner_required"):
        publish_session(tmp_path, tmp_path / "absent", ticket())
    assert list(tmp_path.iterdir()) == []


def test_root_application_cannot_verify(tmp_path, monkeypatch):
    from aggregator.core_storage_session import verify_session

    monkeypatch.setattr("aggregator.core_storage_session.os.geteuid", lambda: 0)
    with pytest.raises(StorageUnavailable, match="session_application_privileged"):
        verify_session(tmp_path, tmp_path / "absent", ticket())
    assert list(tmp_path.iterdir()) == []


def test_publication_is_readable_even_with_restrictive_umask(tmp_path):
    import os
    import stat

    from aggregator.core_storage_session import _publish_record

    previous = os.umask(0o077)
    try:
        path = tmp_path / "session.json"
        session = ticket()
        _publish_record(path, session)
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
        assert decode_session(path.read_bytes()) == session
        assert not path.with_suffix(".tmp").exists()
    finally:
        os.umask(previous)
