import pytest
from edgecitadel_plugin_runtime.agent_card import build_card

YAML = """
agent_id: shell-1
name: shell-1
description: Shell executor.
version: 0.1.0
runtime:
  kind: native
  roles: [worker]
  tags: [dev]
  heartbeat_interval_sec: 30
skills:
  - id: shell.exec
    name: shell-exec
    description: Run a shell command.
    tags: [shell]
capabilities:
  streaming: false
"""


def test_card_has_required_a2a_fields(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(YAML)
    card = build_card(p)
    assert card["name"] == "shell-1"
    assert card["version"] == "0.1.0"
    assert card["url"] == "nats://edgecitadel/agents.shell-1.inbox"
    assert card["provider"]["organization"] == "EdgeCitadel"
    assert "securitySchemes" in card


def test_card_declares_nats_binding_extension(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(YAML)
    card = build_card(p)
    exts = card["capabilities"]["extensions"]
    assert any(
        e["uri"] == "https://edgecitadel.local/ext/nats-binding/v1" for e in exts
    )


def test_card_metadata_vocabulary(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(YAML)
    card = build_card(p)
    md = card["metadata"]
    assert md["runtime.kind"] == "native"
    assert md["runtime.roles"] == ["worker"]
    assert md["runtime.heartbeat_interval_sec"] == 30
    assert "runtime.tags" in md and md["runtime.tags"] == ["dev"]


def test_card_includes_supervisor_ownership_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("EDGECITADEL_NODE_ID", "edge-one")
    monkeypatch.setenv("EDGECITADEL_PLUGIN_ID", "edgecitadel.shell")
    p = tmp_path / "config.yaml"
    p.write_text(YAML)

    metadata = build_card(p)["metadata"]

    assert metadata["edgecitadel.node_id"] == "edge-one"
    assert metadata["edgecitadel.plugin_id"] == "edgecitadel.shell"


def test_bridge_requires_upstream(tmp_path):
    bridge_yaml = YAML.replace("kind: native", "kind: bridge")
    p = tmp_path / "c.yaml"
    p.write_text(bridge_yaml)
    with pytest.raises(ValueError, match="upstream"):
        build_card(p)


def test_card_emits_conformance_default_l1(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(YAML)
    card = build_card(p)
    assert card["metadata"]["runtime.conformance"] == "L1"


def test_card_emits_conformance_when_declared(tmp_path):
    yaml_with_l2 = YAML.replace(
        "heartbeat_interval_sec: 30\n",
        "heartbeat_interval_sec: 30\n  conformance: L2\n",
    )
    assert yaml_with_l2 != YAML, "replace target not found in YAML fixture"
    p = tmp_path / "config.yaml"
    p.write_text(yaml_with_l2)
    card = build_card(p)
    assert card["metadata"]["runtime.conformance"] == "L2"


def test_card_rejects_invalid_conformance(tmp_path):
    yaml_bad = YAML.replace(
        "heartbeat_interval_sec: 30\n",
        "heartbeat_interval_sec: 30\n  conformance: L4\n",
    )
    assert yaml_bad != YAML, "replace target not found in YAML fixture"
    p = tmp_path / "config.yaml"
    p.write_text(yaml_bad)
    with pytest.raises(ValueError, match="runtime.conformance"):
        build_card(p)
