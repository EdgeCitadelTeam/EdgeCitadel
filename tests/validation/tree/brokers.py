"""Owned loopback-only Core + two Leaf processes; no EdgeCitadel imports."""
from __future__ import annotations

import asyncio
import json
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import nats
from nats.errors import NoServersError, TimeoutError as NatsTimeout
from nats.js.errors import NoStreamResponseError

NAMES = ("core", "a", "b")
SUBJECTS = {
    "core": ["agents.core.inbox"],
    "a": ["agents.a1.inbox", "agents.a2.inbox"],
    "b": ["agents.b1.inbox"],
}
STREAM = "AGENT_INBOX"
UNAVAILABLE = (NoStreamResponseError, NatsTimeout)


class Tree:
    def __init__(self, binary: Path, directory: Path):
        self.binary, self.directory = binary, directory
        self.processes, self.clients, self.js, self.logs = {}, {}, {}, {}
        self.all_clients = []
        self.errors = []
        self.configs, self.ports, self.monitors, self.tokens = {}, {}, {}, {}
        # Hold reservations together to avoid duplicate ephemeral ports within a run.
        sockets = []
        try:
            for _ in range(7):
                sock = socket.socket()
                sockets.append(sock)
                sock.bind(("127.0.0.1", 0))
            ports = [sock.getsockname()[1] for sock in sockets]
            leaf_port = ports[-1]
            password = secrets.token_hex(16)
            for i, name in enumerate(NAMES):
                self.ports[name], self.monitors[name] = ports[2*i:2*i+2]
                self.tokens[name] = secrets.token_hex(16)
                config = (
                    f'server_name: "tree-{name}"\n'
                    f'listen: "127.0.0.1:{self.ports[name]}"\n'
                    f'http: "127.0.0.1:{self.monitors[name]}"\n'
                    f'authorization {{token: "{self.tokens[name]}"}}\n'
                    'jetstream {\n'
                    + (f'domain: "TREE_{name.upper()}"\n' if name != "core" else "")
                    + f'store_dir: {json.dumps(str(directory / name))}\n}}\n'
                )
                if name == "core":
                    config += (
                        f'leafnodes {{listen: "127.0.0.1:{leaf_port}"\n'
                        f'authorization {{username: "leaf", password: "{password}"}}\n}}\n'
                    )
                else:
                    config += (
                        'leafnodes {reconnect: "100ms"\nremotes: [{url: '
                        f'"nats-leaf://leaf:{password}@127.0.0.1:{leaf_port}"'
                        '}]\n}\n'
                    )
                path = directory / f"{name}.conf"
                path.write_text(config)
                path.chmod(0o600)
                self.configs[name] = path
        finally:
            for sock in sockets:
                sock.close()
        # Another OS process may race to bind a released port: fail with broker logs.
        # Never connect to an external endpoint or count startup failure as a skip.

    def start(self, name: str) -> None:
        if name in self.processes and self.processes[name].poll() is None:
            raise RuntimeError(f"Already running: {name}")
        log = (self.directory / f"{name}.log").open("ab")
        self.logs.setdefault(name, []).append(log)
        self.processes[name] = subprocess.Popen(
            [str(self.binary), "-c", str(self.configs[name])], stdout=log, stderr=log
        )

    def stop(self, name: str) -> None:
        process = self.processes[name]
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError(f"Broker {name} required SIGKILL")

    async def connect(self, name: str) -> None:
        async def record_error(error):
            self.errors.append({"broker": name, "error": type(error).__name__})

        deadline = time.monotonic() + 10
        while True:
            if self.processes[name].poll() is not None:
                raise RuntimeError(f"Broker {name} exited; inspect its log")
            try:
                client = await nats.connect(
                    f"nats://127.0.0.1:{self.ports[name]}", token=self.tokens[name],
                    connect_timeout=0.25, allow_reconnect=False, max_reconnect_attempts=1, reconnect_time_wait=0.05, error_cb=record_error,
                )
                break
            except (OSError, NoServersError, NatsTimeout):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.05)
        self.all_clients.append(client)
        self.clients[name] = client
        self.js[name] = client.jetstream(domain=None if name == "core" else f"TREE_{name.upper()}")

    async def wait_links(self, expected: dict[str, int]) -> None:
        deadline = time.monotonic() + 10
        observed = {}
        while time.monotonic() < deadline:
            for name in expected:
                if self.processes[name].poll() is not None:
                    raise RuntimeError(f"Broker {name} exited during readiness check")
                try:
                    url = f"http://127.0.0.1:{self.monitors[name]}/leafz"
                    with urllib.request.urlopen(url, timeout=0.25) as response:
                        observed[name] = json.load(response)["leafnodes"]
                except (OSError, urllib.error.URLError):
                    observed[name] = None
            if observed == expected:
                return
            await asyncio.sleep(0.05)
        raise AssertionError(f"Leaf links: expected {expected}, got {observed}")

    async def setup(self) -> None:
        for name in NAMES:
            self.start(name)
        for name in NAMES:
            await self.connect(name)
            await self.js[name].add_stream(
                name=STREAM, subjects=SUBJECTS[name], storage="file", duplicate_window=120,
            )
        await self.wait_links({"core": 2, "a": 1, "b": 1})
        # Round-trip barriers verify actual cross-Leaf subscription propagation.
        for sender, receiver in (("a", "b"), ("b", "a"), ("core", "a"), ("a", "core")):
            await self.barrier(sender, receiver)

    async def barrier(self, sender: str, receiver: str) -> None:
        subject = f"validation.ready.{secrets.token_hex(8)}"
        async def reply(message):
            await message.respond(b"ready")
        subscription = await self.clients[receiver].subscribe(subject, cb=reply)
        await self.clients[receiver].flush()
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    result = await self.clients[sender].request(subject, timeout=0.25)
                    if result.data != b"ready":
                        raise AssertionError("Readiness payload mismatch")
                    return
                except (nats.errors.NoRespondersError, NatsTimeout):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.05)
        finally:
            await subscription.unsubscribe()

    async def publish(self, sender, subject, message_id):
        return await self.js[sender].publish(
            subject, message_id.encode(), headers={"Nats-Msg-Id": message_id}, timeout=1,
        )

    async def snapshot(self, names=NAMES):
        result = {}
        for name in names:
            info = await self.js[name].stream_info(STREAM)
            records = []
            for seq in range(1, info.state.last_seq + 1):
                message = await self.js[name].get_msg(STREAM, seq=seq)
                records.append({"subject": message.subject, "body": message.data.decode(),
                                "message_id": message.headers["Nats-Msg-Id"]})
            if len(records) != info.state.messages:
                raise AssertionError("Stream count and stored records disagree")
            result[name] = records
        return result

    async def close(self):
        errors = []
        for client in self.all_clients:
            try:
                await client.close()
            except Exception as error:
                errors.append(str(error))
        for name in reversed(NAMES):
            if name in self.processes:
                try:
                    self.stop(name)
                except Exception as error:
                    errors.append(str(error))
        for logs in self.logs.values():
            for log in logs:
                log.close()
        if errors:
            raise RuntimeError("Cleanup failed: " + "; ".join(errors))
