"""Owned real NATS leaf fixture on jim-eq; never modifies shared streams."""

import asyncio
import json
import platform
import socket
import subprocess
import tempfile
from pathlib import Path

import nats
from nats.js.api import ConsumerConfig, AckPolicy


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def main():
    assert platform.node().lower() == "jim-eq"
    binary = "/root/.edgecitadel-hermes-leaf/runtime/nats-server/v2.14.6/nats-server"
    with tempfile.TemporaryDirectory(prefix="detailed-trace-broker-") as temporary:
        root = Path(temporary)
        cp, lp, leaf_port = port(), port(), port()
        common = 'authorization { users: [{user: owned, password: owned, permissions: {publish: {allow: ["work.>", "_INBOX.>", "$JS.>", "trace.>"]}, subscribe: {allow: ["work.>", "_INBOX.>", "$JS.>", "trace.>"]}}}] }\n'
        (root / "core.conf").write_text(
            f'listen: 127.0.0.1:{cp}\nserver_name: owned-core\njetstream {{store_dir: "{root}/js"}}\nleafnodes {{listen: "127.0.0.1:{leaf_port}"}}\n'
            + common
        )
        (root / "leaf.conf").write_text(
            f'listen: 127.0.0.1:{lp}\nserver_name: owned-leaf\nleafnodes {{remotes: [{{url: "nats://owned:owned@127.0.0.1:{leaf_port}"}}]}}\n'
            + common
        )
        processes = []
        connections = []
        try:
            for name in ("core", "leaf"):
                processes.append(
                    subprocess.Popen(
                        [binary, "-c", str(root / f"{name}.conf")],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )
                )
            errors = []

            async def error_cb(error):
                errors.append(
                    "permission"
                    if "permissions violation" in str(error).lower()
                    else type(error).__name__
                )

            await asyncio.sleep(0.3)
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError(process.stderr.read().decode())
            for p in (cp, lp):
                nc = await nats.connect(
                    f"nats://127.0.0.1:{p}",
                    user="owned",
                    password="owned",
                    error_cb=error_cb,
                    reconnect_time_wait=0.1,
                    max_reconnect_attempts=20,
                )
                connections.append(nc)
            core, leaf = connections
            from edgecitadel_agentd.transport import AgentdNatsTransport

            observer = object.__new__(AgentdNatsTransport)
            observer._trace = None
            await observer._configure_broker_trace(leaf, "owned-node")
            assert observer._broker_tracing is False
            js = core.jetstream()
            await js.add_stream(name="OWNED", subjects=["work.>"])
            await js.add_consumer(
                "OWNED",
                ConsumerConfig(
                    durable_name="owned",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.2,
                    filter_subject="work.task",
                ),
            )
            sub = await js.pull_subscribe("work.task", durable="owned", stream="OWNED")
            traces = []

            async def trace_cb(msg):
                traces.append(json.loads(msg.data))

            await core.subscribe("trace.>", cb=trace_cb)

            async def ready_reply(message):
                await message.respond(b"ready")

            await core.subscribe("_INBOX.owned_ready", cb=ready_reply)
            await core.flush()

            async def wait_route():
                for _ in range(50):
                    try:
                        reply = await leaf.request(
                            "_INBOX.owned_ready", b"", timeout=0.2
                        )
                        if reply.data == b"ready":
                            return
                    except (nats.errors.NoRespondersError, nats.errors.TimeoutError):
                        pass
                    await asyncio.sleep(0.1)
                raise AssertionError("owned leaf route did not become ready")

            await wait_route()
            ack = await leaf.jetstream().publish(
                "work.task",
                b"owned harmless payload",
                headers={
                    "Nats-Msg-Id": "owned-logical-message",
                    "Nats-Trace-Dest": "trace.owned",
                    "Nats-Trace-Only": "false",
                },
            )
            assert ack.stream == "OWNED" and ack.seq == 1
            first = (await sub.fetch(1, timeout=2))[0]
            assert (
                first.data == b"owned harmless payload"
                and first.metadata.num_delivered == 1
            )
            await first.nak()
            second = (await sub.fetch(1, timeout=2))[0]
            assert second.metadata.num_delivered == 2
            await second.ack_sync()
            dup = await leaf.jetstream().publish(
                "work.task",
                b"owned harmless payload",
                headers={"Nats-Msg-Id": "owned-logical-message"},
            )
            assert dup.duplicate and dup.seq == ack.seq
            await leaf.publish("forbidden.publish", b"owned")
            await leaf.subscribe("forbidden.subscribe")
            await leaf.flush()
            await asyncio.sleep(0.5)
            assert errors.count("permission") >= 2
            servers = {t["server"]["name"] for t in traces}
            assert {"owned-core", "owned-leaf"} <= servers, servers
            count = len(traces)
            await core.publish("work.telemetry", b"no trace headers")
            await asyncio.sleep(0.3)
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError(process.stderr.read().decode())
            assert len(traces) == count, "telemetry unexpectedly traced"
            processes[1].terminate()
            processes[1].wait(timeout=10)
            await asyncio.sleep(0.3)
            assert not leaf.is_connected
            processes[1] = subprocess.Popen(
                [binary, "-c", str(root / "leaf.conf")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            for _ in range(50):
                if leaf.is_connected:
                    break
                await asyncio.sleep(0.1)
            assert leaf.is_connected
            await wait_route()
            recovered = await leaf.jetstream().publish("work.reconnected", b"owned")
            assert recovered.stream == "OWNED"
            print(
                json.dumps(
                    {
                        "trace_permission_failure_is_optional": True,
                        "leaf_traversal": True,
                        "reconnect": True,
                        "publish_ack": True,
                        "redelivery": True,
                        "consumer_ack": True,
                        "duplicate_publish": True,
                        "denied_publish_and_subscribe": True,
                        "normal_delivery_preserved": True,
                        "telemetry_not_recursive": True,
                        "servers": sorted(servers),
                        "broker_events": len(traces),
                    }
                )
            )
        finally:
            for nc in connections:
                await nc.close()
            for process in reversed(processes):
                process.terminate()
                process.wait(timeout=10)


asyncio.run(main())
