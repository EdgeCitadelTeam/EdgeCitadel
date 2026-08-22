"""Tests for smoke.py — uses HTTP mocks via http.server in a thread."""
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
SMOKE = REPO_ROOT / "deploy" / "lib" / "smoke.py"


class _MockHandler(BaseHTTPRequestHandler):
    """Mock /api/command/{agent} and /api/messages?task_id=..."""
    posted_task_id = None
    poll_count = 0

    def log_message(self, *_, **__):
        pass

    def do_POST(self):
        path = self.path.split("?")[0]
        if path.startswith("/api/command/"):
            length = int(self.headers.get("Content-Length", 0))
            json.loads(self.rfile.read(length))
            _MockHandler.posted_task_id = "mock-task-id"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"task_id": _MockHandler.posted_task_id}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/messages"):
            _MockHandler.poll_count += 1
            # Return result envelope on the 2nd poll
            if _MockHandler.poll_count >= 2 and _MockHandler.posted_task_id:
                resp = [{
                    "type": "result",
                    "task_id": _MockHandler.posted_task_id,
                    "payload": {"body": "pong (mock)"},
                }]
            else:
                resp = []
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(resp).encode())
        else:
            self.send_response(404)
            self.end_headers()


class TestSmoke(unittest.TestCase):
    def setUp(self):
        _MockHandler.posted_task_id = None
        _MockHandler.poll_count = 0
        self.server = HTTPServer(("127.0.0.1", 0), _MockHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)

    def test_round_trip_succeeds(self):
        r = subprocess.run(
            [sys.executable, str(SMOKE), "--base-url", f"http://127.0.0.1:{self.port}",
             "--agent", "gemma-1", "--timeout", "10"],
            capture_output=True, text=True
        )
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout} stderr={r.stderr}")
        self.assertIn("round-trip ✓", r.stdout)


if __name__ == "__main__":
    unittest.main()
