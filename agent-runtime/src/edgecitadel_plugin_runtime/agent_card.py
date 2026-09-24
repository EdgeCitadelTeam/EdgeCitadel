"""A2A v1.0 Agent Card factory from per-agent YAML config."""

from __future__ import annotations
import os
import socket
from pathlib import Path
from urllib.parse import urlsplit
import yaml


NATS_EXT_URI = "https://edgecitadel.local/ext/nats-binding/v1"


def _nats_address(value: object) -> str | None:
    """Publish only host/port, even when a supplied URL contains credentials."""
    if not isinstance(value, str):
        return None
    try:
        endpoint = urlsplit(value)
        if endpoint.scheme not in {"nats", "tls"} or not endpoint.hostname:
            return None
        host = endpoint.hostname
        port = endpoint.port or 4222
    except ValueError:
        return None
    if any(character.isspace() or ord(character) < 32 for character in host):
        return None
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def topology_metadata(node: dict) -> dict[str, str]:
    """Expose configuration identity, never credentials or inferred liveness."""
    values = {
        "edgecitadel.node_id": str(node["agent_id"]),
        "edgecitadel.host_name": socket.gethostname(),
        "edgecitadel.messaging_mode": str(node.get("messaging_mode", "single-client")),
    }
    for field, source in (
        ("edgecitadel.nats_address", "plugin_nats_url"),
        ("edgecitadel.core_address", "upstream_nats_url"),
    ):
        if address := _nats_address(node.get(source)):
            values[field] = address
    if node.get("messaging_mode") == "nats_leaf":
        values["edgecitadel.leaf_id"] = "edgecitadel-" + str(node["agent_id"])
        values["edgecitadel.jetstream_domain"] = str(node["jetstream_domain"])
    return values


def build_card(config_path: str | Path) -> dict[str, object]:
    cfg = yaml.safe_load(Path(config_path).read_text())
    agent_id = cfg["agent_id"]
    if cfg["name"] != agent_id:
        raise ValueError("config.name must equal config.agent_id")

    runtime = cfg.get("runtime", {})
    kind = runtime.get("kind", "native")
    if kind == "bridge" and not runtime.get("upstream"):
        raise ValueError("bridge agents require runtime.upstream")

    conformance = runtime.get("conformance", "L1")
    if conformance not in ("L1", "L2", "L3"):
        raise ValueError(
            f"runtime.conformance must be L1, L2, or L3 (got {conformance!r})"
        )

    metadata = {
        "runtime.kind": kind,
        "runtime.roles": runtime.get("roles", ["worker"]),
        "runtime.heartbeat_interval_sec": runtime.get("heartbeat_interval_sec", 30),
        "runtime.conformance": conformance,
    }
    if runtime.get("tags"):
        metadata["runtime.tags"] = runtime["tags"]
    if runtime.get("deployment"):
        metadata["runtime.deployment"] = runtime["deployment"]
    if runtime.get("upstream"):
        metadata["runtime.upstream"] = runtime["upstream"]
    if node_id := os.environ.get("EDGECITADEL_NODE_ID"):
        metadata["edgecitadel.node_id"] = node_id
    if state_dir := os.environ.get("EDGECITADEL_STATE_DIR"):
        from edgecitadel_agentd.node_state import read_node

        if node := read_node(Path(state_dir)):
            metadata.update(topology_metadata(node))
    if plugin_id := os.environ.get("EDGECITADEL_PLUGIN_ID"):
        metadata["edgecitadel.plugin_id"] = plugin_id

    capabilities = cfg.get("capabilities", {}).copy()
    extensions = list(capabilities.get("extensions", []))
    if not any(e.get("uri") == NATS_EXT_URI for e in extensions):
        extensions.append(
            {
                "uri": NATS_EXT_URI,
                "description": "NATS JetStream transport binding for EdgeCitadel.",
                "required": False,
                "params": {"subject_prefix": f"agents.{agent_id}"},
            }
        )
    capabilities["extensions"] = extensions
    capabilities.setdefault("streaming", False)

    return {
        "name": agent_id,
        "description": cfg.get("description", ""),
        "version": cfg.get("version", "0.1.0"),
        "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
        "provider": {"organization": "EdgeCitadel", "url": "https://edgecitadel.local"},
        "capabilities": capabilities,
        "securitySchemes": cfg.get("securitySchemes", {}),
        "additionalInterfaces": cfg.get(
            "additionalInterfaces",
            [
                {
                    "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
                    "transport": "nats-jsonrpc",
                }
            ],
        ),
        "skills": cfg.get("skills", []),
        "defaultInputModes": cfg.get("defaultInputModes", ["text/plain"]),
        "defaultOutputModes": cfg.get("defaultOutputModes", ["text/plain"]),
        "metadata": metadata,
    }
