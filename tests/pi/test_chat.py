"""HTTP-boundary checks for the terminal client's durable submission behavior."""

import importlib.util
import json
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "pi_chat", Path(__file__).parents[2] / "tools/pi/chat.py"
)
chat = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(chat)


@pytest.fixture
def server():
    state = {"posts": [], "lose_receipt": False, "result": "completed"}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, value):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(value).encode())

        def do_POST(self):
            state["posts"].append(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            )
            if state["lose_receipt"]:
                self.send_error(502)
            else:
                self.reply({"task_id": "parent-1"})

        def do_GET(self):
            if self.path.startswith("/api/messages?"):
                self.reply(
                    [
                        {
                            "id": "result-1",
                            "type": "result",
                            "sender_id": "jim-eq-pi",
                            "task_state": state["result"],
                            "payload": {"body": "actual output"},
                        }
                    ]
                )
            else:
                self.reply([])

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield state, f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()
    httpd.server_close()
    thread.join()


def test_lost_receipt_survives_reopen_and_explicit_retry_preserves_intent(
    tmp_path, server
):
    state, base = server
    state["lose_receipt"] = True
    path = tmp_path / "chat.json"
    first = chat.Client(path, base)
    with pytest.raises(urllib.error.HTTPError):
        first.submit("Read the report")
    reopened = chat.Client(path, base)
    assert reopened.wait() is False
    assert len(state["posts"]) == 1  # Observation does not resubmit an unknown receipt.
    with pytest.raises(ValueError, match="unresolved"):
        reopened.submit("Start another task")
    state["lose_receipt"] = False
    reopened.retry()
    assert state["posts"][0] == state["posts"][1]
    assert reopened.wait() is True
    assert reopened.state["history"][0]["user"] == "Read the report"
    assert chat.Client(path, base).state["pending"] is None


def test_reopen_accepted_task_only_polls_and_preserves_conversation(tmp_path, server):
    state, base = server
    path = tmp_path / "chat.json"
    client = chat.Client(path, base)
    client.submit("Inspect hostname")
    session = client.state["session"]
    reopened = chat.Client(path, base)
    reopened.retry()  # A receipt already exists; no POST.
    assert reopened.wait() is True
    assert len(state["posts"]) == 1
    reopened.submit("Repeat the hostname")
    assert state["posts"][1]["args"]["session_id"] == session
    assert (
        state["posts"][0]["args"]["request_id"]
        != state["posts"][1]["args"]["request_id"]
    )


def test_failed_result_is_not_success_and_terminal_controls_are_removed(
    tmp_path, server, capsys
):
    state, base = server
    state["result"] = "failed"
    client = chat.Client(tmp_path / "chat.json", base)
    client.submit("Run a task")
    assert client.wait() is False
    assert client.state["pending"] is None
    assert "[failed]" in capsys.readouterr().out
    assert "\x1b" not in chat.display("\x1b]52;c;clipboard\x07")
    assert "\x07" not in chat.display("\x1b]52;c;clipboard\x07")
