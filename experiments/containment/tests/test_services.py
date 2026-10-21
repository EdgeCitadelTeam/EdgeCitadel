"""The ground-truth services over HTTP, through the worker's own client."""

from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer

import pytest
from det_worker.services import ServicesClient, ServicesUnavailable

from harness.services import RunServices, make_handler


@pytest.fixture
def served(tmp_path):
    services = RunServices(tmp_path, max_spawn=2)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(services, "secret"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    yield url, services, tmp_path
    server.shutdown()
    server.server_close()
    services.close()


def test_ledger_records_each_effect_phase_once(served):
    url, _, directory = served
    client = ServicesClient(url, "secret")
    record = {"effect_id": "E_B", "issuer": "B.t0", "phase": "committed", "labels": []}
    first = client.ledger(record)
    again = client.ledger(record)  # a resumed worker repeating its actuation
    assert again == {"t_ms": first["t_ms"], "repeated": True}
    lines = (directory / "ledger.jsonl").read_text().splitlines()
    assert [json.loads(line)["phase"] for line in lines] == ["committed"]


def test_readiness_counts_distinct_delivery_invocations(served):
    url, _, _ = served
    client = ServicesClient(url, "secret")
    for rt in ("d-1", "d-1", "d-2"):  # d-1 resumed once: one landing, not two
        client.event({"kind": "delivered", "target": "B.t0", "item": "X@v1", "rt": rt})
    client.event({"kind": "delivered", "target": "R.agg", "item": "rA", "rt": "d-3"})
    assert client.ready("B.t0") == {"X@v1": 2}
    assert client.ready("nobody") == {}


def test_spawn_cap_admits_repeats_of_reserved_ids(served):
    url, _, _ = served
    client = ServicesClient(url, "secret")
    assert client.spawn({"id": "nv|a|1"}) and client.spawn({"id": "nv|a|2"})
    assert not client.spawn({"id": "nv|a|3"})
    assert client.spawn({"id": "nv|a|1"})  # the same spawn retried after a pause


def test_requests_need_the_run_token(served):
    url, _, _ = served
    with pytest.raises(ServicesUnavailable):
        ServicesClient(url, "wrong", timeout=1).event({"kind": "start"})


def test_event_feed_is_ordered_and_resumable(served):
    url, services, _ = served
    client = ServicesClient(url, "secret")
    for i in range(5):
        client.event({"kind": "step", "i": i})
    first = services.events_after(0, 3)
    rest = services.events_after(first["next"], 100)
    assert [e["i"] for e in first["events"] + rest["events"]] == list(range(5))
    assert [e["seq"] for e in first["events"]] == [1, 2, 3]


def post(url: str, body: dict, headers: dict | None = None) -> tuple[int, object]:
    import urllib.error
    import urllib.request

    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=5) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, None


@pytest.fixture
def sink(tmp_path):
    """The services with the gateway's sink, a dry-run Home Assistant token and
    an upstream Home Assistant stand-in that records what reached it."""
    from http.server import BaseHTTPRequestHandler

    received = []

    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *args):
            return

        def do_POST(self):  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.append((self.path, self.headers["Authorization"], body))
            status = 200 if body.get("entity_id") != "light.broken" else 500
            self.send_response(status)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"[]")

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    servers = [upstream]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    urls = {}
    services = RunServices(tmp_path, max_spawn=2)
    for name, options in {
        "dry": {"ha_token": "ha-secret"},
        "lab": {"ha_upstream": f"http://127.0.0.1:{upstream.server_address[1]}"},
    }.items():
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            make_handler(services, "secret", sink_token="sink-tok", **options),
        )
        servers.append(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        urls[name] = f"http://127.0.0.1:{server.server_address[1]}"
    yield urls, services, tmp_path, received
    for server in servers:
        server.shutdown()
        server.server_close()
    services.close()


def ledger(directory) -> list[dict]:
    lines = (directory / "ledger.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


def attempt(services: RunServices, effect: str) -> None:
    services.ledger(
        {
            "run": "r",
            "effect_id": effect,
            "issuer": "B.t0",
            "class": "act_b",
            "phase": "attempted",
            "labels": ["fault:x"],
            "value": "v",
        }
    )


def test_the_gateway_sink_records_each_committed_effect_once(sink):
    urls, services, directory, _ = sink
    attempt(services, "E_B")
    gateway = {
        "effect_id": "g-1",
        "node_id": "edge-2",
        "committed_at_ms": 5,
        "data": {"ec_run": "r", "ec_effect": "E_B", "ec_issuer": "B.t0"},
    }
    assert post(f"{urls['dry']}/sink/wrong", gateway)[0] == 401
    assert post(f"{urls['dry']}/sink/sink-tok", gateway)[0] == 200
    assert post(f"{urls['dry']}/sink/sink-tok", gateway)[0] == 200  # adapter retry
    [committed] = [r for r in ledger(directory) if r["phase"] == "committed"]
    # The worker's record of the attempt, with the gateway's commit.
    assert committed["labels"] == ["fault:x"] and committed["attributed"]
    assert committed["gateway"]["node_id"] == "edge-2" and committed["sink"] == "http"


def test_the_home_assistant_front_forwards_without_the_workers_keys(sink):
    urls, services, directory, received = sink
    attempt(services, "E_B")
    attempt(services, "E_C")
    call = {
        "ec_run": "r",
        "ec_effect": "E_B",
        "ec_issuer": "B.t0",
        "entity_id": "light.lamp",
    }
    path = "/api/services/light/turn_on"
    # Dry run: the run's own token stands in for Home Assistant's.
    assert post(f"{urls['dry']}/ha/sink-tok{path}", call)[0] == 401
    bearer = {"Authorization": "Bearer ha-secret"}
    assert post(f"{urls['dry']}/ha/sink-tok{path}", call, bearer)[0] == 200
    # Lab: forwarded to Home Assistant as it is, minus the worker's keys.
    lab = {"Authorization": "Bearer real"}
    failing = {**call, "ec_effect": "E_C", "entity_id": "light.broken"}
    assert post(f"{urls['lab']}/ha/sink-tok{path}", failing, lab)[0] == 500
    assert received == [(path, "Bearer real", {"entity_id": "light.broken"})]
    committed = [r for r in ledger(directory) if r["phase"] == "committed"]
    # Only what Home Assistant accepted counts as performed.
    assert [(r["effect_id"], r["sink"]) for r in committed] == [
        ("E_B", "homeassistant")
    ]
    assert committed[0]["gateway"]["service"] == "light/turn_on"


def test_scheduled_effects_stay_pending_until_the_sink_commits(served):
    url, services, _ = served
    client = ServicesClient(url, "secret")
    for effect, due in (("E_1", 5000), ("E_2", 7000)):
        client.ledger(
            {"effect_id": effect, "issuer": "B.t0", "phase": "scheduled", "due_ms": due}
        )
    assert services.pending_effects()["due_ms"] == [5000, 7000]
    client.ledger({"effect_id": "E_1", "issuer": "B.t0", "phase": "committed"})
    client.ledger({"effect_id": "E_2", "issuer": "B.t0", "phase": "denied"})
    assert services.pending_effects()["due_ms"] == []


@pytest.fixture
def intake():
    """A stand-in for the monitor's alert intake: records each post."""
    from http.server import BaseHTTPRequestHandler

    posts = []

    class Intake(BaseHTTPRequestHandler):
        def log_message(self, *args):
            return

        def do_POST(self):  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            posts.append(({k.lower(): v for k, v in self.headers.items()}, body))
            reply = json.dumps({"alert_id": "a-1"}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Intake)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/api/containment/alerts", posts
    server.shutdown()
    server.server_close()


def wait_for(probe, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("timed out")


@pytest.mark.parametrize("armed_first", [True, False])
def test_an_alert_is_posted_once_on_receipt_of_its_produce_event(
    served, intake, armed_first
):
    """Armed before or after the produce event arrives, the services mark and
    post the alert once, timed from the event's receipt; another item, a
    non-task invocation and a second produce do not trigger it."""
    url, services, directory = served
    alert_url, posts = intake
    client = ServicesClient(url, "secret")
    services.arm_alert({"item": "sched@v1"})
    alert = {"kind": "operator", "attempt_id": "att-1", "root": {"attempt_id": "att-1"}}
    arm = {"alert": alert, "url": alert_url, "headers": {"x-admin": "tok"}}
    if armed_first:
        services.arm_alert(arm)
    client.event({"kind": "produce", "item": "other@v1", "inv_kind": "task"})
    client.event({"kind": "produce", "item": "sched@v1", "inv_kind": "rogue"})
    # The worker's client returns nothing; the services stamp the event.
    produced = services.event(
        {"kind": "produce", "item": "sched@v1", "inv_kind": "task"}
    )
    client.event({"kind": "produce", "item": "sched@v1", "inv_kind": "task"})
    if not armed_first:
        assert services.alert_state()["triggered_ms"] == produced["t_ms"]
        assert not posts  # nothing to post yet
        services.arm_alert(arm)
    state = wait_for(lambda: (s := services.alert_state()).get("done_ms") and s)
    assert state["triggered_ms"] == produced["t_ms"] <= state["marked_ms"]
    assert state["trigger_seq"] == produced["seq"]
    assert state["reply"] == {"alert_id": "a-1"}
    assert "headers" not in state and "url" not in state
    ((headers, body),) = posts
    assert body == alert and headers["x-admin"] == "tok"
    marks = [json.loads(line) for line in (directory / "marks.jsonl").open()]
    assert marks == [
        {
            "name": "alert",
            "posted": True,
            "on_receipt": True,
            "triggered_ms": produced["t_ms"],
            "t_ms": state["marked_ms"],
        }
    ]
    assert "tok" not in (directory / "marks.jsonl").read_text()
    services.arm_alert(arm)  # armed again: still one post
    assert len(posts) == 1


def test_an_uncontained_run_only_marks_its_alert_time(served, intake):
    url, services, directory = served
    _, posts = intake
    services.arm_alert({"item": "sched@v1", "post": False})
    produced = services.event(
        {"kind": "produce", "item": "sched@v1", "inv_kind": "task"}
    )
    state = wait_for(lambda: (s := services.alert_state()).get("done_ms") and s)
    assert state["triggered_ms"] == produced["t_ms"] and state["post"] is False
    mark = json.loads((directory / "marks.jsonl").read_text())
    assert mark["name"] == "alert" and mark["posted"] is False
    assert posts == []
