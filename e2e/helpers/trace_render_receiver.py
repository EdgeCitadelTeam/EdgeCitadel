"""Private host-loopback acknowledgments on the Core host's monotonic clock."""

from __future__ import annotations

import hmac
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class RenderReceiver:
    def __init__(self, capacity=4096):
        if not 1 <= capacity <= 100_000:
            raise ValueError("invalid capacity")
        self.capacity = capacity
        self.token = secrets.token_urlsafe(32)
        self._lock = threading.Lock()
        self._expected = {}
        self._acks = {}
        self._failure = None
        self.ready = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(2)

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.handle_request()

            def do_POST(self):
                self.handle_request()

            def handle_request(self):
                received_ns = time.monotonic_ns()
                self.connection.settimeout(2)
                if not hmac.compare_digest(
                    self.headers.get("Authorization", ""), "Bearer " + owner.token
                ):
                    self.send_error(401)
                    return
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 <= size <= 256 or self.headers.get("Transfer-Encoding"):
                        raise ValueError("invalid body size")
                    body = self.rfile.read(size)
                    if len(body) != size:
                        raise ValueError("incomplete body")
                    if self.command == "GET" and self.path == "/next" and not body:
                        with owner._lock:
                            value = next(
                                (
                                    v
                                    for k, v in owner._expected.items()
                                    if k not in owner._acks
                                ),
                                None,
                            )
                        self.respond(200, value)
                    elif self.command == "POST" and self.path == "/ready" and not body:
                        owner.ready.set()
                        self.respond(200, {})
                    elif self.command == "POST" and self.path == "/ack":
                        value = json.loads(body)
                        if not isinstance(value, dict) or set(value) != {"event_id"}:
                            raise ValueError("invalid acknowledgment")
                        identity = value["event_id"]
                        if not isinstance(identity, str):
                            raise ValueError("invalid identity")
                        with owner._lock:
                            if identity not in owner._expected:
                                raise ValueError("unknown identity")
                            if identity in owner._acks:
                                self.respond(409, {})
                                return
                            owner._acks[identity] = received_ns
                        self.respond(200, {})
                    else:
                        self.respond(404, {})
                except (ValueError, OSError):
                    with owner._lock:
                        owner._failure = owner._failure or "invalid_ack_request"
                    self.respond(400, {})

            def respond(self, status, value):
                data = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def expect(self, event_id, node_id, state, *, trace_id=None, lane=0, measured=True):
        with self._lock:
            if event_id in self._expected:
                raise ValueError("duplicate expected identity")
            if len(self._expected) >= self.capacity:
                self._failure = self._failure or "capacity_exceeded"
                raise ValueError("receiver capacity exceeded")
            self._expected[event_id] = {
                "event_id": event_id,
                "node_id": node_id,
                "state": state,
                "trace_id": trace_id,
                "lane": lane,
                "measured": measured,
            }

    def wait(self, event_id, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if event_id in self._acks:
                    return
            time.sleep(0.01)
        with self._lock:
            self._failure = self._failure or "render_timeout"
        raise TimeoutError("render acknowledgment timeout")

    def report(self):
        with self._lock:
            return {
                "valid": self._failure is None,
                "failure": self._failure,
                "expected": list(self._expected),
                "eligible": [
                    key for key, value in self._expected.items() if value["measured"]
                ],
                "acks": dict(self._acks),
            }

    def close(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()
        if self.thread.is_alive():
            raise RuntimeError("render receiver did not stop")
