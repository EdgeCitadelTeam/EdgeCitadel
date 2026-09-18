"""Private Unix-socket JSON service for native connectors and the CLI."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import signal
import socketserver
import sqlite3
import subprocess
import sys
import tempfile
import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from .restore import RestorePendingError, require_startable
from .store import AgentdStore, StoreError
from .supervisor import ManagedAgentSupervisor
from .trace_contract import TraceContractError, validate_rpc_reply
from .trace_security import (
    flush_authentication_rejections,
    note_authentication_rejection,
)
from .trace_sync_service import TraceSyncService
from .transport import AgentdNatsTransport
from .writer_lock import WriterActiveError, exclusive_writer

MAX_REQUEST_BYTES = 1024 * 1024
PROTOCOL_VERSION = 1
MAX_UNIX_SOCKET_PATH_BYTES = 100
PROCESS_STATE_NAME = "process.json"
ADMIN_TOKEN_NAME = "admin.token"
ADMIN_OPERATIONS = frozenset(
    {
        "trace.import",
        "trace.import.configure",
        "trace.sync.control",
        "connector.register",
        "connector.configure",
        "connector.list",
        "connector.revoke",
        "managed.reconcile",
        "managed.list",
        "managed.connector.reissue",
    }
)


def socket_path_for(state_dir: Path) -> Path:
    """Return a deterministic private socket path within platform limits."""
    preferred = state_dir / "agentd.sock"
    if len(os.fsencode(preferred)) <= MAX_UNIX_SOCKET_PATH_BYTES:
        return preferred
    digest = hashlib.sha256(os.fsencode(state_dir.resolve())).hexdigest()[:16]
    runtime_dir = Path(tempfile.gettempdir()) / f"edgecitadel-agentd-{os.getuid()}"
    if runtime_dir.exists() and runtime_dir.is_symlink():
        raise StoreError("agentd runtime directory must not be a symbolic link")
    runtime_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    runtime_dir.chmod(0o700)
    return runtime_dir / f"{digest}.sock"


def _process_identity(pid: int) -> str:
    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.is_file():
        fields = proc_stat.read_text().rpartition(") ")[2].split()
        if len(fields) > 19:
            return hashlib.sha256(f"linux:{pid}:{fields[19]}".encode()).hexdigest()
    result = subprocess.run(
        ["ps", "-o", "lstart=", "-o", "command=", "-p", str(pid)],
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "LC_ALL": "C"},
    )
    description = result.stdout.strip()
    if not description:
        raise StoreError("agentd process identity is unavailable")
    return hashlib.sha256(f"ps:{pid}:{description}".encode()).hexdigest()


def _write_process_record(state_dir: Path, pid: int | None) -> None:
    record: dict[str, object] = {"version": 1, "pid": pid}
    if pid is not None:
        record["process_identity"] = _process_identity(pid)
    path = state_dir / PROCESS_STATE_NAME
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)
    path.chmod(0o600)


def _load_or_create_admin_token(state_dir: Path) -> str:
    path = state_dir / ADMIN_TOKEN_NAME
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise StoreError("agentd management credential must be a regular file")
        token = path.read_text().strip()
        if len(token) < 32 or len(token) > 1024:
            raise StoreError("agentd management credential is invalid")
        path.chmod(0o600)
        return token
    token = secrets.token_urlsafe(48)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, (token + "\n").encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return token


class AgentdServer(socketserver.ThreadingUnixStreamServer):
    """Unix server carrying one bounded JSON request per connection."""

    daemon_threads = True

    def __init__(
        self,
        socket_path: Path,
        store: AgentdStore,
        transport: AgentdNatsTransport,
        supervisor: ManagedAgentSupervisor,
        admin_token: str,
        telemetry: TraceSyncService | None = None,
    ):
        self.socket_path = socket_path
        self.store = store
        self.transport = transport
        self.supervisor = supervisor
        self.admin_token = admin_token
        self.telemetry = telemetry
        super().__init__(str(socket_path), AgentdRequestHandler)


class AgentdRequestHandler(socketserver.StreamRequestHandler):
    server: AgentdServer

    def handle(self) -> None:
        raw = self.rfile.readline(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES or not raw.endswith(b"\n"):
            self._write_error("request exceeds the 1 MiB limit", "invalid_request")
            return
        try:
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError
            response = dispatch(
                self.server.store,
                request,
                transport=self.server.transport,
                telemetry=self.server.telemetry,
                supervisor=self.server.supervisor,
                admin_token=self.server.admin_token,
            )
        except TraceContractError as error:
            self._write_error(error.code, "operation_failed")
            return
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            self._write_error("request is not a JSON object", "invalid_request")
            return
        except StoreError as error:
            self._write_error(str(error), "operation_failed")
            return
        except Exception as error:  # Keep implementation details off the local API.
            print(
                f"agentd request failed: {type(error).__name__}",
                file=sys.stderr,
            )
            self._write_error("agentd operation failed", "internal_error")
            return
        self.wfile.write((json.dumps({"ok": True, "result": response}) + "\n").encode())

    def _write_error(self, message: str, code: str) -> None:
        self.wfile.write(
            (
                json.dumps({"ok": False, "error": {"code": code, "message": message}})
                + "\n"
            ).encode()
        )


def _params(request: Mapping[str, object]) -> dict[str, Any]:
    value = request.get("params", {})
    if not isinstance(value, dict):
        raise StoreError("params must be an object")
    return cast(dict[str, Any], value)


def _auth(store: AgentdStore, request: Mapping[str, object]) -> sqlite3.Row:
    connector_id = request.get("connector_id")
    token = request.get("token")
    if (
        not isinstance(connector_id, str)
        or not isinstance(token, str)
        or not token.isascii()
    ):
        note_authentication_rejection(store)
        raise StoreError("connector authentication is required")
    try:
        return store.authenticate(connector_id, token)
    except StoreError:
        note_authentication_rejection(store)
        raise


def _authorize_connector_operation(connector: sqlite3.Row, operation: str) -> None:
    host_type = str(connector["host_type"])
    capabilities = set(json.loads(connector["capabilities_json"])["items"])
    common = {"connector.update", "session.open", "session.renew", "session.close"}
    managed = {
        "task.create",
        "task.get",
        "task.claim",
        "task.progress",
        "task.transition",
        "memory.get",
        "memory.put",
        "event.append",
        "span.record",
        "trace.bind",
        "trace.append",
        "trace.finish",
        "trace.dispatch",
        "trace.history",
        "trace.loss",
    }
    native_capabilities = {
        "agent.list": "edgecitadel_agents",
        "task.create": "edgecitadel_delegate",
        "task.list": "edgecitadel_inbox",
        "task.get": "edgecitadel_task_status",
        "task.transition": "edgecitadel_task_update",
        "trace.list": "edgecitadel_trace",
        "trace.get": "edgecitadel_trace",
        "trace.history": "edgecitadel_trace",
        "trace.loss": "edgecitadel_trace",
        "trace.purge": "edgecitadel_trace",
        "event.append": "edgecitadel_trace",
        "span.record": "edgecitadel_trace",
        "trace.bind": "edgecitadel_trace",
        "trace.append": "edgecitadel_trace",
        "trace.finish": "edgecitadel_trace",
        "trace.dispatch": "edgecitadel_trace",
    }
    if operation in common:
        return
    if host_type == "managed-agent" and operation in managed:
        return
    required = native_capabilities.get(operation)
    if (
        host_type != "managed-agent"
        and required is not None
        and required in capabilities
    ):
        return
    raise StoreError("connector is not authorized for this operation")


def _trace_operation(
    store: AgentdStore,
    connector_id: str,
    token: str,
    operation: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    from functools import partial

    from .trace_import import import_trace
    from .trace_loss import report_loss

    request_id = params.get(
        "request_id" if operation != "trace.append" else "observation_id"
    )
    try:
        identity = uuid.UUID(request_id)
        if identity.version != 4 or str(identity) != request_id:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as error:
        raise StoreError("invalid trace request identity") from error
    # node.json is daemon-owned enrollment state. Never accept a node claim in
    # connector params or copy its credentials into a diagnostic.
    try:
        with (store.path.parent.parent / "node.json").open("rb") as source:
            node = json.loads(source.read(65537))
        node_id = node["agent_id"]
        if not isinstance(node_id, str):
            raise ValueError
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise StoreError("trace node identity is unavailable") from error
    methods: dict[str, Callable[..., dict[str, Any]]] = {
        "trace.import": partial(import_trace, store, administrator_authenticated=True),
        "trace.bind": store.bind_trace,
        "trace.append": store.append_trace,
        "trace.finish": store.finish_trace,
        "trace.dispatch": store.dispatch_trace,
        "trace.loss": partial(report_loss, store),
    }
    method = methods[operation]
    code = "storage_unavailable"
    try:
        if operation == "trace.import":
            return method(node_id=node_id, params=params)
        return method(
            node_id=node_id, connector_id=connector_id, token=token, params=params
        )
    except TraceContractError as error:
        if error.code in {
            "idempotency_conflict",
            "binding_closed",
            "session_unavailable",
            "quota_exceeded",
            "storage_unavailable",
        }:
            code = error.code
        elif error.code in {
            "binding_not_owned",
            "execution_not_owned",
            "trace_not_authorized",
            "import_not_authorized",
        }:
            code = "not_authorized"
        elif error.code in {
            "binding_context_mismatch",
            "task_outcome_mismatch",
            "span_identity_mismatch",
            "span_parent_not_owned",
            "span_boundary_conflict",
        }:
            code = "identity_mismatch"
        elif error.code.startswith("unsupported_"):
            code = "unsupported_version"
        else:
            code = "invalid_metadata"
    except sqlite3.Error:
        pass
    reply = {
        "schema_version": 1,
        "operation": operation.removeprefix("trace."),
        "request_id": request_id,
        "status": "error",
        "code": code,
        "retryable": code == "storage_unavailable",
    }
    validate_rpc_reply(
        reply, operation=operation.removeprefix("trace."), request_id=request_id
    )
    return reply


def _admin_auth(
    store: AgentdStore, request: Mapping[str, object], expected_token: str | None
) -> None:
    token = request.get("admin_token")
    if (
        expected_token is None
        or not isinstance(token, str)
        or not token.isascii()
        or not hmac.compare_digest(token, expected_token)
    ):
        note_authentication_rejection(store)
        raise StoreError("agentd management authentication failed")


def dispatch(
    store: AgentdStore,
    request: Mapping[str, object],
    *,
    transport: AgentdNatsTransport | None = None,
    telemetry: TraceSyncService | None = None,
    supervisor: ManagedAgentSupervisor | None = None,
    admin_token: str | None = None,
) -> object:
    if request.get("version") != PROTOCOL_VERSION:
        raise StoreError("unsupported agentd protocol version")
    operation = request.get("operation")
    if not isinstance(operation, str):
        raise StoreError("operation is required")
    params = _params(request)

    if operation in ADMIN_OPERATIONS:
        _admin_auth(store, request, admin_token)

    if operation == "trace.sync.control":
        if telemetry is None:
            raise StoreError("telemetry lifecycle unavailable")
        return telemetry.control(store, params)

    if operation == "health":
        return {
            **store.health(),
            "transport": transport.status() if transport else {},
            "telemetry": telemetry.status()
            if telemetry
            else {
                "enabled": False,
                "state": "disabled",
                "connected": False,
                "active_scopes": 0,
                "fault": None,
            },
        }
    if operation == "connector.register":
        token = store.register_connector(
            connector_id=str(params.get("connector_id", "")),
            host_type=str(params.get("host_type", "")),
            agent_id=str(params.get("agent_id", "")),
            capabilities=list(params.get("capabilities", [])),
            card=cast(Mapping[str, object], params["card"])
            if isinstance(params.get("card"), Mapping)
            else None,
        )
        return {"token": token}
    if operation == "connector.configure":
        store.configure_connector(
            connector_id=str(params.get("connector_id", "")),
            host_type=str(params.get("host_type", "")),
            agent_id=str(params.get("agent_id", "")),
            capabilities=list(params.get("capabilities", [])),
        )
        return {"configured": True}
    if operation == "connector.list":
        return store.list_connectors()
    if operation == "connector.revoke":
        store.revoke_connector(str(params.get("connector_id", "")))
        return {"revoked": True}
    if operation == "managed.reconcile":
        records = params.get("records", [])
        if not isinstance(records, list) or not all(
            isinstance(record, Mapping) for record in records
        ):
            raise StoreError("Managed Agent records must be a list of objects")
        result = store.reconcile_managed_agents(
            cast(list[Mapping[str, object]], records)
        )
        if supervisor is not None:
            supervisor.wake()
        return result
    if operation == "managed.list":
        return (
            supervisor.status()
            if supervisor is not None
            else store.list_managed_agents()
        )
    if operation == "managed.connector.reissue":
        token = store.reissue_managed_connector(
            str(params.get("connector_id", "")), str(params.get("agent_id", ""))
        )
        return {"token": token}

    if operation == "trace.import.configure":
        from .trace_import import configure_import

        return configure_import(store, administrator_authenticated=True, params=params)
    if operation == "trace.import":
        return _trace_operation(store, "", "", operation, dict(params))

    connector = _auth(store, request)
    connector_id = str(connector["connector_id"])
    token = str(request["token"])
    _authorize_connector_operation(connector, operation)
    if operation in {
        "trace.bind",
        "trace.append",
        "trace.finish",
        "trace.dispatch",
        "trace.loss",
    }:
        return _trace_operation(store, connector_id, token, operation, dict(params))
    if operation == "connector.update":
        store.update_connector(
            connector_id=connector_id,
            token=token,
            host_type=str(params.get("host_type", "")),
            agent_id=str(params.get("agent_id", "")),
            capabilities=list(params.get("capabilities", [])),
            card=cast(Mapping[str, object], params["card"])
            if isinstance(params.get("card"), Mapping)
            else None,
        )
        return {"updated": True}
    if operation == "session.open":
        return store.open_session(
            connector_id=connector_id,
            token=token,
            lease_seconds=int(params.get("lease_seconds", 45)),
        )
    if operation == "session.renew":
        expires = store.renew_session(
            connector_id=connector_id,
            token=token,
            session_id=str(params.get("session_id", "")),
            lease_seconds=int(params.get("lease_seconds", 45)),
        )
        return {"lease_expires_at_ms": expires}
    if operation == "session.close":
        store.close_session(
            connector_id=connector_id,
            token=token,
            session_id=str(params.get("session_id", "")),
        )
        return {"closed": True}
    if operation == "agent.list":
        return store.list_agents()
    if operation == "task.create":
        if connector["host_type"] == "managed-agent":
            store.authorize_managed_recipient(
                connector_id,
                str(connector["agent_id"]),
                str(params.get("recipient_id", "")),
            )
        payload = params.get("payload", {})
        if not isinstance(payload, Mapping):
            raise StoreError("task payload must be an object")
        return store.create_task(
            sender_id=str(connector["agent_id"]),
            recipient_id=str(params.get("recipient_id", "")),
            skill_id=params.get("skill_id"),
            payload=cast(Mapping[str, object], payload),
            deadline_at_ms=params.get("deadline_at_ms"),
            task_id=params.get("task_id"),
            trace_id=params.get("trace_id"),
            context_id=params.get("context_id"),
        )
    if operation == "task.claim":
        return store.claim_next_task(
            connector_id=connector_id,
            token=token,
            session_id=str(params.get("session_id", "")),
        )
    if operation in {"memory.get", "memory.put"}:
        if transport is None:
            raise StoreError("NATS transport is unavailable")
        payload = params.get("payload")
        if not isinstance(payload, Mapping):
            raise StoreError("memory payload must be an object")
        if payload.get("agent_id") != connector["agent_id"]:
            raise StoreError("memory agent_id must match the connector identity")
        subject = (
            "memory.turns.get" if operation == "memory.get" else "memory.turns.put"
        )
        return transport.request(subject, cast(Mapping[str, object], payload))
    if operation == "task.progress":
        if transport is None:
            raise StoreError("NATS transport is unavailable")
        task_id = str(params.get("task_id", ""))
        task = store.get_task(task_id)
        if task["recipient_id"] != connector["agent_id"]:
            raise StoreError("only the task recipient may publish progress")
        payload = params.get("payload", {})
        if not isinstance(payload, Mapping):
            raise StoreError("progress payload must be an object")
        envelope = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": "task.progress",
            "sender_id": connector["agent_id"],
            "task_id": task_id,
            "task_state": "working",
            "timestamp": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "payload": dict(payload),
        }
        transport.publish(
            f"agents.{connector['agent_id']}.task_progress.{task_id}", envelope
        )
        return {"published": True}
    if operation == "task.list":
        return store.list_tasks(
            actor_id=str(connector["agent_id"]),
            recipient_id=params.get("recipient_id"),
            include_terminal=bool(params.get("include_terminal", True)),
        )
    if operation == "task.get":
        return store.get_task_for(
            str(params.get("task_id", "")), str(connector["agent_id"])
        )
    if operation == "task.transition":
        task_id = str(params.get("task_id", ""))
        requested_state = str(params.get("state", ""))
        current = store.get_task_for(task_id, str(connector["agent_id"]))
        if requested_state == "accepted" and current["state"] == "queued":
            store.transition_task(
                task_id=task_id,
                state="offered",
                actor_id="edgecitadel-system",
                evidence={"source": "native_session"},
                queue_transport=False,
            )
        return store.transition_task(
            task_id=task_id,
            state=requested_state,
            actor_id=str(connector["agent_id"]),
            reason=params.get("reason"),
            session_id=params.get("session_id"),
            evidence=cast(Mapping[str, object], params.get("evidence", {})),
            result=cast(Mapping[str, object], params["result"])
            if isinstance(params.get("result"), Mapping)
            else None,
        )
    if operation == "event.append":
        event_id = store.append_event(
            event_type=str(params.get("event_type", "")),
            agent_id=str(connector["agent_id"]),
            task_id=params.get("task_id"),
            trace_id=params.get("trace_id"),
            attributes=cast(Mapping[str, object], params.get("attributes", {})),
        )
        return {"event_id": event_id}
    if operation == "span.record":
        span_id = store.record_span(
            trace_id=str(params.get("trace_id", "")),
            operation=str(params.get("operation", "")),
            status=str(params.get("status", "unset")),
            agent_id=str(connector["agent_id"]),
            task_id=params.get("task_id"),
            parent_span_id=params.get("parent_span_id"),
            span_id=params.get("span_id"),
            started_at_ms=params.get("started_at_ms"),
            ended_at_ms=params.get("ended_at_ms"),
            attributes=cast(Mapping[str, object], params.get("attributes", {})),
        )
        return {"span_id": span_id}
    if operation == "trace.history":
        from .trace_history import read_history

        return read_history(
            store,
            connector_id=str(request["connector_id"]),
            token=str(request["token"]),
            params=dict(params),
        )
    if operation == "trace.list":
        return store.list_traces(
            limit=int(params.get("limit", 100)), agent_id=str(connector["agent_id"])
        )
    if operation == "trace.get":
        return store.get_trace(
            str(params.get("trace_id", "")), agent_id=str(connector["agent_id"])
        )
    if operation == "trace.purge":
        return store.purge_telemetry(
            before_ms=params.get("before_ms"), agent_id=str(connector["agent_id"])
        )
    raise StoreError("unsupported operation")


def serve(state_dir: Path, stop_event: threading.Event | None = None) -> None:
    with exclusive_writer(state_dir):
        require_startable(state_dir)
        _write_process_record(state_dir, os.getpid())
        try:
            _serve_locked(state_dir, stop_event)
        finally:
            _write_process_record(state_dir, None)


def _serve_locked(state_dir: Path, stop_event: threading.Event | None = None) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    state_dir.chmod(0o700)
    socket_path = socket_path_for(state_dir)
    if socket_path.exists():
        socket_path.unlink()
    store = AgentdStore(state_dir / "agentd.sqlite3")
    admin_token = _load_or_create_admin_token(state_dir)
    transport = AgentdNatsTransport(state_dir.parent, store)
    supervisor = ManagedAgentSupervisor(state_dir.parent, store)
    telemetry = TraceSyncService(
        state_dir.parent,
        store.path,
        enabled=os.environ.get("EDGECITADEL_TRACE_SYNC") == "1",
    )
    server = AgentdServer(
        socket_path, store, transport, supervisor, admin_token, telemetry
    )
    socket_path.chmod(0o600)
    owned_stop = stop_event or threading.Event()

    def reconcile() -> None:
        while not owned_stop.wait(1):
            try:
                store.reconcile()
                flush_authentication_rejections(store)
            except (sqlite3.Error, TraceContractError) as error:
                if isinstance(error, TraceContractError) and error.code not in {
                    "quota_exceeded",
                    "storage_unavailable",
                }:
                    raise
                if store.set_reconciliation_storage_unavailable(True):
                    print("agentd reconciliation storage unavailable", file=sys.stderr)
            else:
                store.set_reconciliation_storage_unavailable(False)

    reconciler = threading.Thread(target=reconcile, daemon=True)
    reconciler.start()
    transport.start()
    telemetry.start()
    supervisor.start()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    owned_stop.wait()
    supervisor.stop()
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
    telemetry.close()
    transport.stop()
    reconciler.join(timeout=5)
    store.close()
    socket_path.unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edgecitadel-agentd")
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    os.umask(0o077)
    state_dir = args.state_dir.expanduser()
    state_dir.mkdir(parents=True, exist_ok=True)
    state_dir.chmod(0o700)
    try:
        serve(state_dir, stop)
    except (WriterActiveError, RestorePendingError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
