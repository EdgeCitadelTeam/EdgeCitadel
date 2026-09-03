"""Client for the private agentd Unix-socket protocol."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any, cast

from .service import MAX_REQUEST_BYTES, PROTOCOL_VERSION


class AgentdClientError(RuntimeError):
    """A local service connection or operation failure."""


class AgentdClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        connector_id: str | None = None,
        token: str | None = None,
        admin_token: str | None = None,
        timeout: float = 5,
    ) -> None:
        self.socket_path = socket_path
        self.connector_id = connector_id
        self.token = token
        self.admin_token = admin_token
        self.timeout = timeout

    def call(self, operation: str, **params: object) -> object:
        request: dict[str, object] = {
            "version": PROTOCOL_VERSION,
            "operation": operation,
            "params": params,
        }
        if self.connector_id is not None:
            request["connector_id"] = self.connector_id
        if self.token is not None:
            request["token"] = self.token
        if self.admin_token is not None:
            request["admin_token"] = self.admin_token
        encoded = (json.dumps(request, separators=(",", ":")) + "\n").encode()
        if len(encoded) > MAX_REQUEST_BYTES:
            raise AgentdClientError("request exceeds the 1 MiB limit")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(self.timeout)
                connection.connect(str(self.socket_path))
                connection.sendall(encoded)
                response = _read_line(connection)
        except OSError as error:
            raise AgentdClientError(f"agentd is unavailable: {error}") from error
        try:
            document = json.loads(response)
        except json.JSONDecodeError as error:
            raise AgentdClientError("agentd returned invalid JSON") from error
        if not isinstance(document, dict) or not document.get("ok"):
            error_payload = (
                document.get("error", {}) if isinstance(document, dict) else {}
            )
            message = error_payload.get("message", "agentd operation failed")
            raise AgentdClientError(str(message))
        return cast(dict[str, Any], document)["result"]


def _read_line(connection: socket.socket) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = connection.recv(min(65536, MAX_REQUEST_BYTES + 1 - size))
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > MAX_REQUEST_BYTES:
            raise AgentdClientError("agentd response exceeds the 1 MiB limit")
        if b"\n" in chunk:
            break
    response = b"".join(chunks)
    if not response.endswith(b"\n"):
        raise AgentdClientError("agentd closed the connection without a response")
    return response
