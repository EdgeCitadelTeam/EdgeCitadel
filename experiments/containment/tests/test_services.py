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
