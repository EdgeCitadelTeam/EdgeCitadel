"""Ground-truth services of one run: effect ledger, worker events, readiness.

One process per run, on the controller's host, so every ground-truth time is
on one clock (the monitor runs on the same host in both modes):

- ``POST /ledger``: append an effect record (``attempted``, ``held``,
  ``committed``, ``denied``). The ledger is the actuator: an effect counts as
  committed when its ``committed`` record is appended. Records are idempotent
  per (effect, issuer, phase), so a resumed worker cannot actuate twice.
- ``POST /events``: append a worker event; ``GET /events?after=<seq>`` feeds the
  controller (alert triggers, quiescence).
- ``GET /ready?task=<id>``: pushed items that landed for a planned task, with
  the number of distinct delivery invocations. It carries no content.
- ``POST /spawn``: reserve one undeclared invocation under the run's cap.
- ``POST /mark``: the controller's own time stamps (run start, alert).

Every request must carry the run's ``X-Harness-Token``; records are appended to
JSON-lines files in the run directory and flushed one by one.
"""

from __future__ import annotations

import argparse
import hmac
import json
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

MAX_BODY = 1 << 20


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class RunServices:
    """State and files of one run; thread-safe."""

    def __init__(self, directory: Path, *, max_spawn: int) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.max_spawn = max_spawn
        self._lock = threading.Lock()
        self._files = {
            name: (directory / f"{name}.jsonl").open("a", encoding="utf-8")
            for name in ("events", "ledger", "marks")
        }
        self._events: list[dict[str, Any]] = []
        self._ledger: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._ready: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        self._spawned: set[str] = set()

    def close(self) -> None:
        with self._lock:
            for handle in self._files.values():
                handle.close()

    def _append(self, name: str, record: dict[str, Any]) -> None:
        handle = self._files[name]
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        handle.flush()

    def event(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            stamped = {**record, "seq": len(self._events) + 1, "t_ms": now_ms()}
            self._events.append(stamped)
            self._append("events", stamped)
            if record.get("kind") == "delivered":
                self._ready[str(record["target"])][str(record["item"])].add(
                    str(record["rt"])
                )
        return {"seq": stamped["seq"], "t_ms": stamped["t_ms"]}

    def ledger(self, record: dict[str, Any]) -> dict[str, Any]:
        key = (str(record["effect_id"]), str(record["issuer"]), str(record["phase"]))
        with self._lock:
            existing = self._ledger.get(key)
            if existing is not None:
                return {"t_ms": existing["t_ms"], "repeated": True}
            stamped = {**record, "t_ms": now_ms()}
            self._ledger[key] = stamped
            self._append("ledger", stamped)
        return {"t_ms": stamped["t_ms"], "repeated": False}

    def ready(self, task: str) -> dict[str, Any]:
        with self._lock:
            return {
                "items": {
                    item: len(rts) for item, rts in self._ready.get(task, {}).items()
                }
            }

    def spawn(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            ident = str(record["id"])
            allowed = ident in self._spawned or len(self._spawned) < self.max_spawn
            if allowed:
                self._spawned.add(ident)
            return {"allowed": allowed, "n": len(self._spawned)}

    def mark(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            stamped = {**record, "t_ms": now_ms()}
            self._append("marks", stamped)
        return {"t_ms": stamped["t_ms"]}

    def events_after(self, seq: int, limit: int) -> dict[str, Any]:
        with self._lock:
            chunk = self._events[seq : seq + limit]
        return {"events": chunk, "next": seq + len(chunk), "t_ms": now_ms()}


def make_handler(services: RunServices, token: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return  # the records themselves are the log

        def _reply(self, code: int, value: Any) -> None:
            body = json.dumps(value).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            given = self.headers.get("X-Harness-Token", "")
            if hmac.compare_digest(given.encode(), token.encode()):
                return True
            self._reply(401, {"error": "invalid token"})
            return False

        def do_GET(self) -> None:  # noqa: N802
            if not self._authorized():
                return
            url = urlsplit(self.path)
            query = parse_qs(url.query)
            if url.path == "/ready" and "task" in query:
                self._reply(200, services.ready(query["task"][0]))
            elif url.path == "/events":
                after = int(query.get("after", ["0"])[0])
                limit = min(int(query.get("limit", ["5000"])[0]), 50000)
                self._reply(200, services.events_after(after, limit))
            elif url.path == "/clock":
                self._reply(200, {"t_ms": now_ms()})
            elif url.path == "/health":
                self._reply(200, {"ok": True})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if not self._authorized():
                return
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY:
                self._reply(400, {"error": "body required"})
                return
            try:
                record = json.loads(self.rfile.read(length))
                if not isinstance(record, dict):
                    raise ValueError("object expected")
                route = {
                    "/events": services.event,
                    "/ledger": services.ledger,
                    "/spawn": services.spawn,
                    "/mark": services.mark,
                }.get(urlsplit(self.path).path)
                if route is None:
                    self._reply(404, {"error": "not found"})
                    return
                self._reply(200, route(record))
            except (ValueError, KeyError) as error:
                self._reply(400, {"error": f"invalid record: {type(error).__name__}"})

    return Handler


def serve(directory: Path, host: str, port: int, token: str, max_spawn: int) -> None:
    services = RunServices(directory, max_spawn=max_spawn)
    server = ThreadingHTTPServer((host, port), make_handler(services, token))
    server.daemon_threads = True
    (directory / "services.ready").write_text(
        json.dumps({"port": server.server_address[1]})
    )
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        services.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.services")
    parser.add_argument("--dir", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--max-spawn", type=int, default=200)
    args = parser.parse_args(argv)
    token = (args.dir / "services.token").read_text().strip()
    serve(args.dir, args.host, args.port, token, args.max_spawn)


if __name__ == "__main__":
    main()
