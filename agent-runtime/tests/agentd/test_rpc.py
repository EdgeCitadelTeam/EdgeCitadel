from __future__ import annotations

import json
from io import StringIO

from edgecitadel_agentd import rpc


def test_rpc_forwards_nested_operation_params_without_consuming_connector_id(
    tmp_path, monkeypatch
):
    captured = {}

    class FakeClient:
        def __init__(
            self,
            socket_path,
            *,
            connector_id=None,
            token=None,
            admin_token=None,
        ):
            captured["auth"] = {
                "socket_path": socket_path,
                "connector_id": connector_id,
                "token": token,
                "admin_token": admin_token,
            }

        def call(self, operation, **params):
            captured["call"] = {"operation": operation, "params": params}
            return {"token": "connector-token"}

    request = {
        "operation": "connector.register",
        "params": {
            "connector_id": "codex-local",
            "host_type": "codex",
            "agent_id": "edge-one-codex",
        },
        "admin_token": "admin-secret",
    }
    output = StringIO()
    monkeypatch.setattr(rpc, "AgentdClient", FakeClient)
    monkeypatch.setattr(rpc.sys, "stdin", StringIO(json.dumps(request)))
    monkeypatch.setattr(rpc.sys, "stdout", output)

    assert rpc.main(["--state-dir", str(tmp_path)]) == 0
    assert captured["auth"] == {
        "socket_path": rpc.socket_path_for(tmp_path),
        "connector_id": None,
        "token": None,
        "admin_token": "admin-secret",
    }
    assert captured["call"] == {
        "operation": "connector.register",
        "params": {
            "connector_id": "codex-local",
            "host_type": "codex",
            "agent_id": "edge-one-codex",
        },
    }
    assert json.loads(output.getvalue()) == {
        "ok": True,
        "result": {"token": "connector-token"},
    }
