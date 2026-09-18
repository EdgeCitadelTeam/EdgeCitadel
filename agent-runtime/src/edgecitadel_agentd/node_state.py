"""Shared validation for local daemon NATS endpoint state."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit


def read_node(state_dir: Path) -> dict[str, Any] | None:
    path = state_dir / "node.json"
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or type(value.get("version")) is not int:
        return None
    role = value.get("mode")
    if role not in {"core", "edge"} or value["version"] not in (
        {1} if role == "core" else {1, 2}
    ):
        return None
    mode = value.get("messaging_mode", "single-client")
    if mode not in {"single-client", "nats_leaf"}:
        return None
    if mode == "nats_leaf" and (
        role != "edge"
        or not isinstance(value.get("jetstream_domain"), str)
        or not value["jetstream_domain"]
    ):
        return None
    # Only validated legacy Core records may supply the old endpoint pair.
    # Never fill half an explicit pair, or turn malformed Edge state valid.
    if (
        role == "core"
        and "plugin_nats_url" not in value
        and "plugin_nats_token" not in value
    ):
        value = {
            **value,
            "plugin_nats_url": value.get("nats_url"),
            "plugin_nats_token": value.get("nats_token"),
        }
    url, token = value.get("plugin_nats_url"), value.get("plugin_nats_token")
    if not isinstance(url, str) or not isinstance(token, str) or not token:
        return None
    try:
        endpoint = urlsplit(url)
        valid = (
            endpoint.scheme in {"nats", "tls"}
            and bool(endpoint.hostname)
            and endpoint.port != 0
            and endpoint.username is None
            and endpoint.password is None
            and endpoint.path in {"", "/"}
            and not endpoint.query
            and not endpoint.fragment
            and not any(character.isspace() or ord(character) < 32 for character in url)
        )
    except ValueError:
        return None
    return cast(dict[str, Any], value) if valid else None
