"""NATS transport owned by agentd on behalf of local native connectors."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import uuid
from collections.abc import Mapping
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from edgecitadel_plugin_runtime.agent_card import topology_metadata
from edgecitadel_plugin_runtime.jetstream import ensure_consumer, ensure_stream
from edgecitadel_plugin_runtime.validator import ValidationError, default_validator

from .node_state import read_node
from .store import AgentdStore, StoreError
from .trace_contract import TraceContractError
from .trace_monitor import BrokerMonitor
from .trace_transport import TransportTrace, delivery_metadata, publication_metadata


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class AgentdNatsTransport:
    """Persistent NATS connection and durable inbox consumers for agentd."""

    def __init__(self, state_dir: Path, store: AgentdStore) -> None:
        self.state_dir = state_dir
        self.store = store
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._status: dict[str, object] = {
            "configured": False,
            "connected": False,
            "mode": "unconfigured",
            "detail": "node state is not configured",
        }
        self._thread: threading.Thread | None = None
        self._ready_agents: set[str] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._nc: NATS | None = None
        self._validator = default_validator()
        self._trace: TransportTrace | None = None
        self._broker_tracing = False

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise StoreError("NATS transport did not stop within 10 seconds")

    def status(self) -> dict[str, object]:
        with self._lock:
            return dict(self._status)

    def request(self, subject: str, payload: Mapping[str, object]) -> object:
        with self._lock:
            loop, nc = self._loop, self._nc
        if loop is None or nc is None or not nc.is_connected:
            raise StoreError("NATS transport is disconnected")
        future = asyncio.run_coroutine_threadsafe(
            nc.request(
                subject,
                json.dumps(dict(payload), separators=(",", ":")).encode(),
                timeout=2,
            ),
            loop,
        )
        try:
            message = future.result(timeout=3)
            value = json.loads(message.data)
        except (FutureTimeoutError, asyncio.TimeoutError) as error:
            future.cancel()
            raise StoreError("NATS service request timed out") from error
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StoreError("NATS service returned invalid JSON") from error
        except Exception as error:  # NATS exposes several transport-specific errors.
            raise StoreError(
                f"NATS service request failed: {type(error).__name__}"
            ) from error
        return value

    def publish(self, subject: str, envelope: Mapping[str, object]) -> None:
        try:
            self._validator.validate_envelope(dict(envelope))
        except ValidationError as error:
            raise StoreError("outbound transport envelope is invalid") from error
        with self._lock:
            loop, nc = self._loop, self._nc
        if loop is None or nc is None or not nc.is_connected:
            raise StoreError("NATS transport is disconnected")
        future = asyncio.run_coroutine_threadsafe(
            nc.publish(
                subject,
                json.dumps(dict(envelope), separators=(",", ":")).encode(),
            ),
            loop,
        )
        try:
            future.result(timeout=3)
        except FutureTimeoutError as error:
            future.cancel()
            raise StoreError("NATS publish timed out") from error
        except Exception as error:  # NATS exposes several transport-specific errors.
            raise StoreError(f"NATS publish failed: {type(error).__name__}") from error

    def _observe(self, phase: str, **values: Any) -> None:
        if self._trace is not None:
            self._trace.record(phase, **values)

    def _set_status(self, **values: object) -> None:
        with self._lock:
            self._status.update(values)

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception as error:  # noqa: BLE001
            self._set_status(
                connected=False, detail=f"transport failed: {type(error).__name__}"
            )

    def _node(self) -> dict[str, Any] | None:
        return read_node(self.state_dir)

    async def _run(self) -> None:
        with self._lock:
            self._loop = asyncio.get_running_loop()
        node: dict[str, Any] | None = None
        url: object = None
        token: object = None
        while not self._stop.is_set():
            node = self._node()
            if node is not None:
                url = node.get("plugin_nats_url")
                token = node.get("plugin_nats_token")
                if isinstance(url, str) and isinstance(token, str):
                    break
                self._set_status(detail="node state has no plugin NATS endpoint")
            await asyncio.sleep(1)
        if self._stop.is_set() or node is None:
            with self._lock:
                self._loop = None
            return
        assert isinstance(url, str)
        assert isinstance(token, str)
        self._trace = TransportTrace(self.store, str(node["agent_id"]))
        mode = str(node.get("messaging_mode", "single-client"))
        self._set_status(
            configured=True,
            mode=mode,
            endpoint="local" if mode == "nats_leaf" else "core",
        )

        async def disconnected() -> None:
            self._broker_tracing = False
            self._set_status(connected=False, detail="NATS disconnected")
            self._observe("disconnected", kind="infrastructure")

        async def reconnected() -> None:
            if self._nc is not None:
                await self._configure_broker_trace(
                    self._nc, str(node["agent_id"]), subscribe=False
                )
            self._set_status(connected=True, detail="NATS connected")
            self._observe("connected", kind="infrastructure")

        async def closed() -> None:
            self._set_status(connected=False, detail="NATS connection closed")
            self._observe("closed", kind="infrastructure")

        async def broker_error(error) -> None:
            name = type(error).__name__
            phase = (
                "permission_denied"
                if "Permission" in name or "permissions violation" in str(error).lower()
                else "authentication_rejected"
                if "Auth" in name
                else "advisory"
            )
            self._observe(
                phase,
                kind="broker",
                attributes={"provenance": "nats_client_error", "error_type": name},
            )

        while not self._stop.is_set():
            nc = NATS()
            try:
                await nc.connect(
                    servers=[url],
                    token=token,
                    connect_timeout=2,
                    reconnect_time_wait=1,
                    max_reconnect_attempts=-1,
                    disconnected_cb=disconnected,
                    reconnected_cb=reconnected,
                    closed_cb=closed,
                    error_cb=broker_error,
                )
                self._set_status(connected=True, detail="NATS connected")
                self._observe("connected", kind="infrastructure")
                with self._lock:
                    self._nc = nc
                domain = node.get("jetstream_domain")
                js = nc.jetstream(domain=domain if isinstance(domain, str) else None)
                await self._configure_broker_trace(nc, str(node["agent_id"]))
                await nc.subscribe("agents.*.register", cb=self._observe_presence)
                await nc.subscribe("agents.*.heartbeat", cb=self._observe_presence)
                await nc.subscribe("agents.*.status", cb=self._observe_presence)
                await nc.subscribe(
                    "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>",
                    cb=self._observe_advisory,
                )
                await self._connected_loop(nc, js)
            except Exception as error:  # noqa: BLE001
                self._set_status(
                    connected=False,
                    detail=f"NATS unavailable: {type(error).__name__}",
                )
            finally:
                with self._lock:
                    self._nc = None
                if not nc.is_closed:
                    await nc.close()
            await asyncio.sleep(1)
        with self._lock:
            self._loop = None

    async def _connected_loop(self, nc: NATS, js: JetStreamContext) -> None:
        consumers: dict[str, asyncio.Task[None]] = {}
        published_presence: set[str] = set()
        monitor = (
            asyncio.create_task(
                BrokerMonitor(
                    self._trace, os.environ.get("EDGECITADEL_NATS_MONITOR_URL")
                ).run()
            )
            if self._trace
            else None
        )
        try:
            while not self._stop.is_set():
                current_ids: set[str] = set()
                for connector in self.store.list_connectors():
                    if connector["revoked"]:
                        continue
                    agent_id = str(connector["agent_id"])
                    current_ids.add(agent_id)
                    if agent_id not in consumers:
                        consumers[agent_id] = asyncio.create_task(
                            self._consume_agent(js, agent_id)
                        )
                for agent_id, task in list(consumers.items()):
                    if agent_id not in current_ids or task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        consumers.pop(agent_id, None)
                        with self._lock:
                            self._ready_agents.discard(agent_id)
                            self._status["ready_inbox_count"] = len(self._ready_agents)

                active = {
                    str(connector["agent_id"]): connector
                    for connector in self.store.list_active_connectors()
                }
                for agent_id, connector in active.items():
                    if agent_id not in published_presence:
                        await self._publish_register(nc, connector)
                    await self._publish_heartbeat(nc, agent_id)
                for agent_id in published_presence - active.keys():
                    await self._publish_offline(nc, agent_id)
                published_presence = set(active)

                for pending in self.store.pending_transport():
                    envelope = json.dumps(
                        pending["envelope"], separators=(",", ":")
                    ).encode()
                    message_id = str(pending["message_id"])
                    observation = {
                        "envelope": pending["envelope"],
                        "attributes": {
                            "subject": str(pending["subject"]),
                            "publication_attempt_id": str(uuid.uuid4()),
                        },
                    }
                    self._observe(
                        "publish_started",
                        content={
                            "body": pending["envelope"]["payload"].get(
                                "body", pending["envelope"]["payload"]
                            )
                        },
                        **observation,
                    )
                    try:
                        ack = await js.publish(
                            str(pending["subject"]),
                            envelope,
                            headers={
                                "Nats-Msg-Id": message_id,
                                **(
                                    {
                                        "Nats-Trace-Dest": f"_EC.TRACE.{self._trace.node_id}.{message_id}.{observation['attributes']['publication_attempt_id']}",
                                        "Nats-Trace-Only": "false",
                                    }
                                    if self._trace and self._broker_tracing
                                    else {}
                                ),
                            },
                        )
                    except Exception as error:
                        self._observe(
                            "publish_failed",
                            envelope=pending["envelope"],
                            attributes={
                                **observation["attributes"],
                                "error_type": type(error).__name__,
                            },
                        )
                        raise
                    self._observe(
                        "publish_accepted",
                        envelope=pending["envelope"],
                        attributes={
                            **observation["attributes"],
                            **publication_metadata(ack),
                        },
                    )
                    await nc.publish(
                        f"agents.{pending['envelope']['sender_id']}.outbox",
                        envelope,
                    )
                    self.store.mark_transport_published(message_id)
                await asyncio.sleep(1)
        finally:
            if monitor is not None:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)
            for task in consumers.values():
                task.cancel()
            await asyncio.gather(*consumers.values(), return_exceptions=True)
            with self._lock:
                self._ready_agents.clear()
                self._status["ready_inbox_count"] = 0

    async def _consume_agent(self, js: JetStreamContext, agent_id: str) -> None:
        await ensure_stream(js, agent_id)
        await ensure_consumer(js, agent_id)
        subscription = await js.pull_subscribe(
            subject=f"agents.{agent_id}.inbox",
            durable=f"{agent_id}_inbox",
        )
        with self._lock:
            self._ready_agents.add(agent_id)
            self._status["ready_inbox_count"] = len(self._ready_agents)
        while not self._stop.is_set():
            try:
                messages = await subscription.fetch(batch=1, timeout=1)
            except asyncio.TimeoutError:
                continue
            for message in messages:
                await self._ingest_message(message, agent_id)

    async def _ingest_message(self, message: Msg, agent_id: str) -> None:
        try:
            envelope = json.loads(message.data)
            if not isinstance(envelope, Mapping):
                raise StoreError("transport envelope must be an object")
            if envelope.get("recipient_id") != agent_id:
                raise StoreError("transport recipient does not match consumer")
            metadata = delivery_metadata(message)
            self._observe(
                "received",
                envelope=envelope,
                attributes=metadata,
                content={
                    "body": envelope.get("payload", {}).get(
                        "body", envelope.get("payload", {})
                    )
                },
            )
            self.store.ingest_transport_envelope(envelope)
            self._observe(
                "caller_accepted"
                if envelope.get("type") == "result"
                else "durably_accepted",
                envelope=envelope,
                attributes=metadata,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, StoreError) as error:
            cause = error.__cause__
            if isinstance(cause, TraceContractError) and cause.code in {
                "quota_exceeded",
                "storage_unavailable",
            }:
                # A valid message was not persisted. Preserve broker redelivery
                # just as for SQLite failures, rather than terminating it as poison.
                raise
            await message.term()
            self._observe(
                "terminated",
                attributes={
                    **delivery_metadata(message),
                    "error_type": type(error).__name__,
                },
            )
            return
        try:
            await message.ack()
        except Exception as error:
            self._observe(
                "ack_failed",
                envelope=envelope,
                attributes={**metadata, "error_type": type(error).__name__},
            )
            raise
        self._observe("ack_sent", envelope=envelope, attributes=metadata)

    async def _configure_broker_trace(
        self, nc: NATS, node_id: str, *, subscribe: bool = True
    ) -> None:
        self._broker_tracing = False
        try:
            version = nc.connected_server_version
            if (version.major, version.minor) < (2, 11):
                self._observe(
                    "unavailable",
                    kind="infrastructure",
                    attributes={
                        "provenance": "nats_trace_probe",
                        "coverage_reason": "broker_tracing_unsupported",
                    },
                )
                return
            if subscribe:
                await nc.subscribe(
                    f"_EC.TRACE.{node_id}.>", cb=self._observe_broker_trace
                )
            # A real round trip checks both publish and subscribe permissions.
            # Never discover a forbidden trace destination using a task message.
            reply = await nc.request(f"_EC.TRACE.{node_id}.probe", b"", timeout=2)
            self._broker_tracing = reply.data == b"trace-ready"
        except Exception:  # noqa: BLE001 - telemetry permission is not task permission
            pass
        if not self._broker_tracing:
            self._observe(
                "unavailable",
                kind="infrastructure",
                attributes={
                    "provenance": "nats_trace_probe",
                    "coverage_reason": "trace_destination_unavailable",
                },
            )

    async def _observe_broker_trace(self, message: Msg) -> None:
        if self._trace and message.subject == f"_EC.TRACE.{self._trace.node_id}.probe":
            if message.reply:
                await message.respond(b"trace-ready")
            return
        # Destination identifies an actual durable publication, not a topology guess.
        try:
            parts = message.subject.split(".")
            if len(parts) != 5 or len(message.data) > 65536:
                return
            message_id, attempt = parts[-2:]
            uuid.UUID(message_id)
            uuid.UUID(attempt)
            with self.store._lock:
                row = self.store._connection.execute(
                    "SELECT envelope_json FROM transport_outbox WHERE message_id=?",
                    (message_id,),
                ).fetchone()
                envelope = self.store._decode_content(row[0]) if row else None
            if envelope is None:
                return
            document = json.loads(message.data)
            header = document.get("request", {}).get("header", {})
            if header.get("Nats-Msg-Id") != [message_id]:
                return
            server = document.get("server", {})
            self._observe(
                "observed",
                kind="broker",
                envelope=envelope,
                attributes={
                    "provenance": "nats_message_trace",
                    "publication_attempt_id": attempt,
                    **(
                        {"server_id": server["id"]}
                        if isinstance(server.get("id"), str)
                        else {}
                    ),
                    **(
                        {"server_name": server["name"]}
                        if isinstance(server.get("name"), str)
                        else {}
                    ),
                },
                content={"boundaries": document.get("events", [])},
            )
        except (ValueError, TypeError, KeyError):
            self._observe(
                "unavailable",
                kind="infrastructure",
                attributes={
                    "coverage_reason": "invalid_broker_trace",
                    "provenance": "nats_message_trace",
                },
            )

    async def _observe_presence(self, message: Msg) -> None:
        try:
            envelope = json.loads(message.data)
            if not isinstance(envelope, Mapping):
                return
            document = dict(envelope)
            if document.get("type") == "register":
                self._validator.validate_register(document)
            else:
                self._validator.validate_envelope(document)
            agent_id = envelope.get("sender_id")
            message_type = envelope.get("type")
            if not isinstance(agent_id, str):
                return
            subject_parts = message.subject.split(".")
            if len(subject_parts) != 3 or subject_parts[1] != agent_id:
                return
            state = "online"
            reason = str(message_type)
            if message_type == "status" and envelope.get("agent_state") in {
                "offline",
                "error",
            }:
                state = "unavailable"
            self.store.observe_presence(agent_id=agent_id, state=state, reason=reason)
        except (UnicodeDecodeError, json.JSONDecodeError, StoreError, ValidationError):
            return

    async def _observe_advisory(self, message: Msg) -> None:
        parts = message.subject.split(".")
        try:
            advisory = json.loads(message.data)
            if not isinstance(advisory, Mapping):
                return
            stream = advisory.get("stream")
            consumer = advisory.get("consumer")
            stream_seq = advisory.get("stream_seq")
            if (
                stream != "AGENT_INBOX"
                or len(parts) < 2
                or parts[-2] != stream
                or not isinstance(consumer, str)
                or parts[-1] != consumer
                or not consumer.endswith("_inbox")
                or type(stream_seq) is not int
                or stream_seq <= 0
            ):
                return
            observed_agent = consumer.removesuffix("_inbox")
            self.store.append_event(
                event_type="transport.max_deliveries",
                agent_id=observed_agent,
                task_id=None,
                trace_id=None,
                attributes={
                    "stream": stream,
                    "consumer": consumer,
                    "stream_seq": stream_seq,
                },
            )
        except (UnicodeDecodeError, json.JSONDecodeError, StoreError):
            return

    async def _publish_register(
        self, nc: NATS, connector: Mapping[str, object]
    ) -> None:
        agent_id = str(connector["agent_id"])
        capabilities = cast(list[object], connector["capabilities"])
        configured_card = connector.get("card")
        card: Mapping[str, object] = (
            cast(Mapping[str, object], configured_card)
            if isinstance(configured_card, Mapping)
            else {
                "name": agent_id,
                "description": f"Active {connector['host_type']} session connected by EdgeCitadel",
                "version": "0.1.0",
                "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
                "provider": {
                    "organization": "EdgeCitadel",
                    "url": "https://edgecitadel.local",
                },
                "capabilities": {
                    "streaming": False,
                    "extensions": [
                        {
                            "uri": "https://edgecitadel.local/ext/nats-binding/v1",
                            "description": "Agent messaging is carried by the host-local EdgeCitadel transport.",
                            "required": True,
                        }
                    ],
                },
                "securitySchemes": {},
                "skills": [
                    {"id": str(item), "name": str(item), "description": str(item)}
                    for item in capabilities
                ],
                "defaultInputModes": ["text/plain"],
                "defaultOutputModes": ["text/plain"],
                "metadata": {
                    "runtime.kind": "native",
                    "runtime.roles": ["worker"],
                    "runtime.heartbeat_interval_sec": 15,
                    "runtime.conformance": "L1",
                    "edgecitadel.connector_id": connector["connector_id"],
                },
            }
        )
        node = self._node()
        if node is not None:
            metadata = dict(cast(Mapping[str, object], card.get("metadata", {})))
            for name in (
                "edgecitadel.leaf_id",
                "edgecitadel.jetstream_domain",
                "edgecitadel.nats_address",
                "edgecitadel.core_address",
            ):
                metadata.pop(name, None)
            metadata.update(topology_metadata(node))
            card = {**card, "metadata": metadata}
        await self._publish_plain(
            nc, f"agents.{agent_id}.register", "register", agent_id, payload=card
        )

    async def _publish_heartbeat(self, nc: NATS, agent_id: str) -> None:
        await self._publish_plain(
            nc, f"agents.{agent_id}.heartbeat", "heartbeat", agent_id
        )

    async def _publish_offline(self, nc: NATS, agent_id: str) -> None:
        await self._publish_plain(
            nc,
            f"agents.{agent_id}.status",
            "status",
            agent_id,
            agent_state="offline",
            payload={"reason": "native_session_unavailable"},
        )

    async def _publish_plain(
        self,
        nc: NATS,
        subject: str,
        message_type: str,
        agent_id: str,
        *,
        payload: Mapping[str, object] | None = None,
        agent_state: str | None = None,
    ) -> None:
        envelope: dict[str, object] = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": message_type,
            "sender_id": agent_id,
            "timestamp": _timestamp(),
            "payload": dict(payload or {}),
        }
        if agent_state is not None:
            envelope["agent_state"] = agent_state
        if message_type == "register":
            self._validator.validate_register(envelope)
        else:
            self._validator.validate_envelope(envelope)
        await nc.publish(subject, json.dumps(envelope, separators=(",", ":")).encode())
