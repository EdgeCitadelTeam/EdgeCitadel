"""A2A v1.0 Agent Card factory from per-agent YAML config."""
from __future__ import annotations
from pathlib import Path
import yaml


NATS_EXT_URI = "https://edgecitadel.local/ext/nats-binding/v1"


def build_card(config_path: str | Path) -> dict:
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
        "runtime.heartbeat_interval_sec":
            runtime.get("heartbeat_interval_sec", 30),
        "runtime.conformance": conformance,
    }
    if runtime.get("tags"):
        metadata["runtime.tags"] = runtime["tags"]
    if runtime.get("deployment"):
        metadata["runtime.deployment"] = runtime["deployment"]
    if runtime.get("upstream"):
        metadata["runtime.upstream"] = runtime["upstream"]

    capabilities = cfg.get("capabilities", {}).copy()
    extensions = list(capabilities.get("extensions", []))
    if not any(e.get("uri") == NATS_EXT_URI for e in extensions):
        extensions.append({
            "uri": NATS_EXT_URI,
            "description": "NATS JetStream transport binding for EdgeCitadel.",
            "required": False,
            "params": {"subject_prefix": f"agents.{agent_id}"},
        })
    capabilities["extensions"] = extensions
    capabilities.setdefault("streaming", False)

    return {
        "name": agent_id,
        "description": cfg.get("description", ""),
        "version": cfg.get("version", "0.1.0"),
        "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
        "provider": {"organization": "EdgeCitadel",
                     "url": "https://edgecitadel.local"},
        "capabilities": capabilities,
        "securitySchemes": cfg.get("securitySchemes", {}),
        "additionalInterfaces": cfg.get("additionalInterfaces", [
            {"url": f"nats://edgecitadel/agents.{agent_id}.inbox",
             "transport": "nats-jsonrpc"}
        ]),
        "skills": cfg.get("skills", []),
        "defaultInputModes": cfg.get("defaultInputModes", ["text/plain"]),
        "defaultOutputModes": cfg.get("defaultOutputModes", ["text/plain"]),
        "metadata": metadata,
    }
