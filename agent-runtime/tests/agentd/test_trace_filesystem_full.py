"""Opt-in real ENOSPC on an owned, bounded macOS disk-image filesystem."""

import errno
import json
import os
import sqlite3
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from test_trace_crash import append_request, prepare, snapshot

from edgecitadel_agentd.service import PROTOCOL_VERSION, dispatch
from edgecitadel_agentd.trace_producer import RuntimeTrace
from edgecitadel_agentd.trace_retention import maintain_capacity

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin" or os.environ.get("RUN_AGENTD_FILESYSTEM_FULL") != "1",
    reason="requires explicit owned macOS disk-image ENOSPC opt-in",
)


@pytest.fixture
def owned_volume(tmp_path):
    image = tmp_path / "owned-full.dmg"
    mount = tmp_path / "mounted"
    mount.mkdir()
    subprocess.run(
        [
            "/usr/bin/hdiutil",
            "create",
            "-size",
            "64m",
            "-fs",
            "HFS+",
            "-volname",
            "OwnedTraceFull",
            str(image),
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    subprocess.run(
        [
            "/usr/bin/hdiutil",
            "attach",
            "-nobrowse",
            "-mountpoint",
            str(mount),
            str(image),
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    try:
        # Never write the filler unless this is a separate, size-limited mount.
        assert mount.stat().st_dev != tmp_path.stat().st_dev
        stats = os.statvfs(mount)
        assert 0 < stats.f_blocks * stats.f_frsize <= 128 * 1024 * 1024
        yield mount
    finally:
        subprocess.run(
            ["/usr/bin/hdiutil", "detach", str(mount)],
            check=True,
            capture_output=True,
            timeout=30,
        )


def exhaust(volume):
    filler = volume / "owned-filler"
    with filler.open("xb", buffering=0) as output:
        for block in (b"x" * (1024 * 1024), b"x" * 4096):
            while True:
                try:
                    output.write(block)
                except OSError as error:
                    assert error.errno == errno.ENOSPC
                    break
        os.fsync(output.fileno())
    return filler


def test_filesystem_full_dispatch_rolls_back_then_retries_once(owned_volume):
    store, token, _session, binding = prepare(
        owned_volume / "state/agentd/agentd.sqlite3"
    )
    try:
        store.append_trace(
            node_id="owned-edge",
            connector_id="owner",
            token=token,
            params=append_request(binding),
        )
        store._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        params = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "binding_id": binding["binding_id"],
            "recipient_id": "worker",
            "request": "x" * 4096,
            "skill_id": None,
            "deadline_at_ms": None,
        }
        before = snapshot(store)
        filler = exhaust(owned_volume)
        with pytest.raises(sqlite3.Error):
            store.dispatch_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=params
            )
        assert snapshot(store) == before
        # Expiration must not detach payloads if its mandatory loss marker cannot
        # be committed to the genuinely full filesystem.
        with pytest.raises(sqlite3.Error), store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            maintain_capacity(
                store._connection,
                now_ms=int(time.time() * 1000),
                expire_before_ms=int(time.time() * 1000) + 1,
            )
        assert snapshot(store) == before
        filler.unlink()
        reply = store.dispatch_trace(
            node_id="owned-edge", connector_id="owner", token=token, params=params
        )
        assert (
            store.dispatch_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=params
            )
            == reply
        )
        assert (
            store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM transport_outbox"
            ).fetchone()[0]
            == 1
        )
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


@pytest.mark.asyncio
async def test_filesystem_full_optional_effect_once_and_loss_after_recovery(
    owned_volume,
):
    store, token, _session, binding = prepare(
        owned_volume / "state/agentd/agentd.sqlite3"
    )

    class Client:
        def call(self, operation, **params):
            return dispatch(
                store,
                {
                    "version": PROTOCOL_VERSION,
                    "operation": operation,
                    "params": params,
                    "connector_id": "owner",
                    "token": token,
                },
            )

    trace = RuntimeTrace(Client())
    trace.binding_id = binding["binding_id"]
    try:
        store._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        filler = exhaust(owned_volume)
        effects = []
        async with trace.operation("tool", "owned-filesystem-full-effect"):
            effects.append("once")
        assert effects == ["once"] and trace.dropped_observations == 2
        filler.unlink()
        await trace.report_loss()
        events = [
            json.loads(row[0])
            for row in store._connection.execute(
                "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            )
        ]
        assert len(events) == 1 and events[0]["attributes"]["dropped_observations"] == 2
        assert effects == ["once"]
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()
