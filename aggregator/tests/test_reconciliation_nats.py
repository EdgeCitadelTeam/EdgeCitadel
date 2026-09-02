"""Opt-in real-NATS proof for max-delivery task reconciliation."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
from nats.aio.client import Client as NATS

from aggregator import database as db
from aggregator.aggregator import MessageRouter
from aggregator.jetstream_bootstrap import ensure_consumer, ensure_stream


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_RECONCILIATION_NATS_INTEGRATION") != "1",
    reason="set RUN_RECONCILIATION_NATS_INTEGRATION=1 to run owned NATS integration",
)


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.asyncio
async def test_real_max_delivery_advisory_emits_correlated_failure(
    tmp_path: Path,
    envelope_schema_path: Path,
    card_schema_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = shutil.which("nats-server")
    if executable is None:
        pytest.skip("nats-server is not installed")
    port = _unused_port()
    monitor_port = _unused_port()
    process = subprocess.Popen(
        [
            executable,
            "-js",
            "-p",
            str(port),
            "-m",
            str(monitor_port),
            "-sd",
            str(tmp_path / "jetstream"),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    nc = NATS()
    try:
        deadline = time.monotonic() + 10
        while True:
            if process.poll() is not None:
                pytest.fail("owned nats-server exited during startup")
            try:
                await nc.connect(
                    servers=[f"nats://127.0.0.1:{port}"],
                    connect_timeout=0.2,
                    allow_reconnect=False,
                )
                break
            except Exception:  # noqa: BLE001 - retry bounded owned test service.
                if time.monotonic() >= deadline:
                    pytest.fail("owned nats-server did not become ready")
                await asyncio.sleep(0.05)

        database_path = tmp_path / "aggregator.sqlite3"
        monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
        db.init_db(str(database_path))
        router = MessageRouter(
            db_path=str(database_path),
            envelope_schema=envelope_schema_path,
            card_schema=card_schema_path,
        )
        router.nc = nc
        router.js = nc.jetstream()
        await nc.subscribe(
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>",
            cb=router.on_advisory,
        )
        result_sub = await nc.subscribe("agents.edgecitadel-system.outbox")
        await nc.flush()

        await ensure_stream(router.js, "worker-1")
        await ensure_stream(router.js, "aggregator")
        await ensure_consumer(
            router.js,
            "worker-1",
            ack_wait_sec=1,
            max_ack_pending=1,
            max_deliver=2,
        )
        worker = await router.js.pull_subscribe(
            "agents.worker-1.inbox", durable="worker-1_inbox"
        )
        task_id = "10000000-0000-4000-8000-000000000001"
        source = {
            "v": 1,
            "id": "20000000-0000-4000-8000-000000000001",
            "type": "command",
            "sender_id": "aggregator",
            "recipient_id": "worker-1",
            "task_id": task_id,
            "timestamp": "2026-01-01T00:00:00.000Z",
            "payload": {"body": "work"},
        }
        await router.js.publish("agents.worker-1.inbox", json.dumps(source).encode())
        first = (await worker.fetch(batch=1, timeout=5))[0]
        await first.nak()
        second = (await worker.fetch(batch=1, timeout=5))[0]
        await second.nak()
        # A pull request after the final NAK makes the server evaluate the
        # delivery ceiling and emit the advisory; no third delivery is legal.
        try:
            await worker.fetch(batch=1, timeout=2)
        except Exception:  # noqa: BLE001 - the expected API is a timeout.
            pass

        result_message = await result_sub.next_msg(timeout=10)
        result = json.loads(result_message.data)
        assert result["task_id"] == task_id
        assert result["recipient_id"] == "aggregator"
        assert result["task_state"] == "failed"
        assert result["payload"] == {
            "error": "recipient_unavailable",
            "recipient_id": "worker-1",
            "trigger": "max_deliveries",
        }
        assert db.count_poison_by_agent() == {"worker-1": 1}
    finally:
        if not nc.is_closed:
            await nc.close()
        process.terminate()
        process.wait(timeout=5)
