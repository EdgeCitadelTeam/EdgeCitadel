from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from nats.aio.client import Client as NATS

from aggregator.jetstream_bootstrap import ensure_consumer, ensure_stream
from scripts.research.benchmark_core import register_envelope, result_envelope


AGENT_ID = "bench-crash"


def _read_counter(path: Path) -> int:
    if not path.exists():
        return 0
    return int(path.read_text().strip() or "0")


def _increment_counter(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    value = _read_counter(path) + 1
    path.write_text(f"{value}\n")
    return value


async def _connect() -> NATS:
    nc = NATS()
    kwargs = {
        "servers": [os.environ.get("NATS_URL", "nats://127.0.0.1:4222")],
        "connect_timeout": 3,
    }
    token = os.environ.get("NATS_TOKEN")
    if token:
        kwargs["token"] = token
    await nc.connect(**kwargs)
    return nc


async def run(mode: str, counter: Path) -> int:
    nc = await _connect()
    try:
        js = nc.jetstream()
        await ensure_stream(js, AGENT_ID)
        await ensure_consumer(js, AGENT_ID, ack_wait_sec=2, max_deliver=3)
        await nc.publish(
            f"agents.{AGENT_ID}.register",
            json.dumps(
                register_envelope(
                    AGENT_ID,
                    metadata={
                        "runtime.kind": "native",
                        "runtime.roles": ["worker"],
                        "runtime.heartbeat_interval_sec": 30,
                        "runtime.deployment": "test",
                        "runtime.tags": ["benchmark", "crash-fixture"],
                    },
                )
            ).encode(),
        )
        await nc.flush()

        sub = await js.pull_subscribe(
            subject=f"agents.{AGENT_ID}.inbox",
            durable=f"{AGENT_ID}_inbox",
        )
        msgs = await sub.fetch(batch=1, timeout=30)
        msg = msgs[0]
        env = json.loads(msg.data)
        side_effect_count = _increment_counter(counter)
        if mode == "crash-first":
            return 91

        out = result_envelope(
            sender_id=AGENT_ID,
            recipient_id=env["sender_id"],
            task_id=env["task_id"],
            task_state="completed",
            payload={
                "body": "redelivery complete",
                "side_effect_count": side_effect_count,
            },
            context_id=env.get("context_id"),
        )
        data = json.dumps(out).encode()
        await js.publish(
            f"agents.{env['sender_id']}.inbox",
            data,
            headers={"Nats-Msg-Id": out["id"]},
        )
        await nc.publish(f"agents.{AGENT_ID}.outbox", data)
        await msg.ack()
        await nc.flush()
        return 0
    finally:
        await nc.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["crash-first", "complete"], required=True)
    parser.add_argument("--counter", required=True)
    args = parser.parse_args()
    return asyncio.run(run(args.mode, Path(args.counter)))


if __name__ == "__main__":
    raise SystemExit(main())
