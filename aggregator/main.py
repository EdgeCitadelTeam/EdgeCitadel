from __future__ import annotations
import hashlib
import json
import os
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
from nats.errors import NoRespondersError, TimeoutError as NATSTimeoutError
from nats.js.errors import NoStreamResponseError, ServiceUnavailableError

from . import database as db
from .aggregator import AggregatorApp, now_iso
from .liveness import effective_agent_state, with_effective_agent_state
from .models import (
    CommandRequest,
    CommandResponse,
    EnrollmentInvitationRequest,
    EnrollmentRedeemRequest,
    EnrollmentRedeemResponse,
    RegistryEntry,
)
from .websocket_hub import WebSocketHub


_OPENCLAW_TOKENS: dict[str, str] = {}


async def _agent_inbox_consumer(js, agent_id: str):
    subject = f"agents.{agent_id}.inbox"
    consumers = await js.consumers_info("AGENT_INBOX")
    matches = [
        consumer
        for consumer in consumers
        if getattr(getattr(consumer, "config", None), "filter_subject", None) == subject
    ]
    if len(matches) != 1:
        raise LookupError("consumer not found")
    return matches[0]


def _build_direct_command_envelope(
    *,
    agent_id: str,
    sender_id: str,
    request: CommandRequest,
) -> dict:
    task_id = str(uuid.uuid4())
    context_id = request.context_id or task_id
    return {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "command",
        "sender_id": sender_id,
        "recipient_id": agent_id,
        "task_id": task_id,
        "context_id": context_id,
        "hop_count": 0,
        "timestamp": now_iso(),
        "payload": {
            "body": request.body,
            **({"args": request.args} if request.args else {}),
            **({"skill_id": request.skill_id} if request.skill_id else {}),
        },
    }


async def _publish_direct_command(router, envelope: dict) -> None:
    encoded = json.dumps(envelope).encode()
    await router.js.publish(
        f"agents.{envelope['recipient_id']}.inbox",
        encoded,
        headers={"Nats-Msg-Id": envelope["id"]},
    )
    await router.nc.publish(
        f"agents.{envelope['sender_id']}.outbox",
        encoded,
    )


def make_app(for_testing: bool = False) -> FastAPI:
    app = FastAPI(title="EdgeCitadel Aggregator", version="0.1.0")
    state: dict = {"app": None}

    db_path = os.environ.get("DB_PATH", "/data/openclaw.db")
    envelope_schema = Path(
        os.environ.get(
            "ENVELOPE_SCHEMA_PATH",
            str(Path(__file__).resolve().parents[1] / "schemas" / "envelope.v1.json"),
        )
    )
    card_schema = Path(
        os.environ.get(
            "CARD_SCHEMA_PATH",
            str(Path(__file__).resolve().parents[1] / "schemas" / "agent-card.v1.json"),
        )
    )

    db.init_db(db_path)
    lab_run_id = os.environ.get("LAB_RUN_ID")
    if lab_run_id:
        from .lab_inventory import build_lab_router
        from scripts.research.lab_config import LabConfigError, validate_run_id

        token_sha256 = os.environ.get("LAB_TOKEN_SHA256", "")
        inventory_value = os.environ.get("LAB_INVENTORY_PATH", "")
        try:
            validate_run_id(lab_run_id)
            if len(token_sha256) != 64 or any(
                char not in "0123456789abcdef" for char in token_sha256
            ):
                raise LabConfigError("lab token hash is invalid")
            inventory_path = Path(inventory_value)
            if not inventory_path.is_absolute():
                raise LabConfigError("lab inventory path must be absolute")
        except LabConfigError as error:
            raise RuntimeError("invalid lab runtime configuration") from error
        app.include_router(
            build_lab_router(
                run_id=lab_run_id,
                token_sha256=token_sha256,
                inventory_path=inventory_path,
            )
        )

    @app.on_event("startup")
    async def _startup():
        if for_testing:
            state["app"] = None
            state["hub"] = WebSocketHub()
            return
        nats_url = os.environ["NATS_URL"]
        nats_token = os.environ["NATS_TOKEN"]
        agg = AggregatorApp(
            nats_url=nats_url,
            nats_token=nats_token,
            db_path=db_path,
            envelope_schema=envelope_schema,
            card_schema=card_schema,
        )
        # Wire the WebSocket fan-out hub onto the router BEFORE start()
        # so any boot-time envelopes (self-register, request_register
        # broadcast) reach connected dashboards.
        hub = WebSocketHub()
        agg.router.hub = hub
        state["hub"] = hub
        await agg.start()
        from .memory import MemoryService

        mem_svc = MemoryService(nc=agg.router.nc)
        await mem_svc.start()
        state["memory"] = mem_svc
        state["app"] = agg

    @app.on_event("shutdown")
    async def _shutdown():
        if state.get("memory"):
            await state["memory"].stop()
        if state["app"] and state["app"].router.nc:
            await state["app"].router.nc.drain()

    @app.get("/api/system/status")
    async def system_status():
        agg = state["app"]
        nats_connected = bool(agg and agg.router.nc and agg.router.nc.is_connected)
        jetstream_ok = False
        if nats_connected:
            try:
                await agg.router.js.stream_info("AGENT_INBOX")
                jetstream_ok = True
            except Exception:
                jetstream_ok = False
        return {
            "nats_connected": nats_connected,
            "jetstream_stream_ok": jetstream_ok,
            "version": "0.1.0",
        }

    @app.post("/api/enrollment/invitations", status_code=201)
    async def create_enrollment_invitation(
        request: EnrollmentInvitationRequest,
        x_edgecitadel_admin_token: Annotated[str | None, Header()] = None,
    ):
        expected = os.environ.get("EDGECITADEL_ADMIN_TOKEN", "")
        if (
            not expected
            or not x_edgecitadel_admin_token
            or not secrets.compare_digest(expected, x_edgecitadel_admin_token)
        ):
            raise HTTPException(401, "invalid administrator credential")
        token = secrets.token_urlsafe(32)
        now = time.time()
        expires_at = now + request.expires_in_seconds
        db.create_enrollment_invitation(
            token_hash=hashlib.sha256(token.encode()).hexdigest(),
            agent_id=request.agent_id,
            created_at=now,
            expires_at=expires_at,
        )
        return {
            "token": token,
            "agent_id": request.agent_id,
            "expires_at": expires_at,
        }

    @app.post(
        "/api/enrollment/redeem",
        response_model=EnrollmentRedeemResponse,
        response_model_exclude_none=True,
    )
    async def redeem_enrollment_invitation(request: EnrollmentRedeemRequest):
        if request.messaging_mode == "single-client":
            nats_token = os.environ.get("NATS_TOKEN", "")
            if not nats_token:
                raise HTTPException(503, "broker enrollment is not configured")
            leaf_username = leaf_password = ""
        else:
            nats_token = ""
            leaf_username = os.environ.get("NATS_LEAF_USERNAME", "")
            leaf_password = os.environ.get("NATS_LEAF_PASSWORD", "")
            if not leaf_username or not leaf_password:
                raise HTTPException(503, "Leaf enrollment is not configured")
        agent_id = db.redeem_enrollment_invitation(
            token_hash=hashlib.sha256(request.token.encode()).hexdigest(),
            redeemed_at=time.time(),
        )
        if agent_id is None:
            raise HTTPException(400, "invitation is invalid, expired, or already used")
        if request.messaging_mode == "single-client":
            return {
                "agent_id": agent_id,
                "nats_token": nats_token,
            }
        return {
            "agent_id": agent_id,
            "leaf_username": leaf_username,
            "leaf_password": leaf_password,
        }

    @app.get("/api/agents")
    async def list_agents():
        agents = [with_effective_agent_state(a) for a in db.list_agents()]
        # exclude self-cached aggregator entry from peer list
        return [a for a in agents if a["agent_id"] != "aggregator"]

    @app.get("/api/agents/{agent_id}")
    async def get_agent(agent_id: str):
        a = db.get_agent(agent_id)
        if not a:
            raise HTTPException(404, "agent not found")
        return with_effective_agent_state(a)

    @app.get("/api/agents/{agent_id}/card")
    async def get_agent_card(agent_id: str):
        a = db.get_agent(agent_id)
        if not a:
            raise HTTPException(404, "agent not found")
        return a["card"]

    @app.delete("/api/agents/{agent_id}", status_code=204)
    async def delete_agent(agent_id: str):
        if agent_id == "aggregator":
            raise HTTPException(400, "cannot delete self")
        ok = db.delete_agent(agent_id)
        if not ok:
            raise HTTPException(404, "agent not found")
        hub: WebSocketHub | None = state.get("hub")
        if hub is not None:
            try:
                await hub.broadcast_event(
                    "agent_deleted", {"agent_id": agent_id}, agent_id=agent_id
                )
            except Exception:
                pass
        return PlainTextResponse(status_code=204)

    @app.get("/api/agents/{agent_id}/queue")
    async def get_queue(agent_id: str):
        agg = state["app"]
        if agg is None:
            raise HTTPException(503, "jetstream not initialized")
        try:
            ci = await _agent_inbox_consumer(agg.router.js, agent_id)
        except Exception as error:
            raise HTTPException(404, f"consumer not found: {error}")
        return {
            "pending": ci.num_pending,
            "ack_pending": ci.num_ack_pending,
            "num_waiting": getattr(ci, "num_waiting", 0),
        }

    @app.post(
        "/api/command/{agent_id}", status_code=202, response_model=CommandResponse
    )
    async def post_command(
        agent_id: str, req: CommandRequest, sender_id: str | None = None
    ):
        """Dispatch a command envelope to {agent_id} via JetStream.

        The optional `sender_id` query param overrides the default
        `aggregator` sender. When set to anything other than `aggregator`,
        the aggregator auto-registers a synthetic A2A card with
        `runtime.deployment: test` for that sender, so the resulting
        command + outbox + result envelopes all tag `deployment=test` via
        MessageRouter._deployment_for. Pattern intended for test runners
        (e.g. Playwright Phase 2 smoke); production callers omit it."""
        agg = state["app"]
        actual_sender = sender_id or "aggregator"
        recipient = db.get_agent(agent_id)
        if recipient is not None and effective_agent_state(recipient) == "offline":
            raise HTTPException(409, f"agent {agent_id} is offline")
        # Auto-register a synthetic test-deployment card for non-default
        # senders so deployment tagging propagates without requiring a real
        # NATS register envelope from the caller.
        if (
            agg is not None
            and actual_sender != "aggregator"
            and actual_sender not in agg.router.cache
        ):
            agg.router.cache[actual_sender] = {
                "name": actual_sender,
                "metadata": {
                    "runtime.kind": "native",
                    "runtime.roles": ["worker"],
                    "runtime.heartbeat_interval_sec": 30,
                    "runtime.deployment": "test",
                    "runtime.tags": ["synthetic", "auto-registered"],
                },
            }

        env = _build_direct_command_envelope(
            agent_id=agent_id,
            sender_id=actual_sender,
            request=req,
        )
        if agg is not None:
            try:
                await _publish_direct_command(agg.router, env)
            except (
                NoRespondersError,
                NATSTimeoutError,
                NoStreamResponseError,
                ServiceUnavailableError,
            ) as error:
                raise HTTPException(
                    503,
                    "destination durable inbox is unreachable; command was not accepted",
                ) from error
        return CommandResponse(
            task_id=env["task_id"],
            recipient_id=agent_id,
            accepted_at=env["timestamp"],
        )

    @app.get("/api/messages")
    async def query_messages(
        agent_id: str | None = None,
        task_id: str | None = None,
        context_id: str | None = None,
        type: str | None = None,
        since_ts: str | None = None,
        deployment: str | None = None,
        exclude_deployment: str | None = None,
        limit: int = 500,
    ):
        return db.query_messages(
            agent_id=agent_id,
            task_id=task_id,
            context_id=context_id,
            type=type,
            since_ts=since_ts,
            deployment=deployment,
            exclude_deployment=exclude_deployment,
            limit=limit,
        )

    @app.get("/api/poison")
    async def query_poison(agent_id: str | None = None, limit: int = 100):
        return db.recent_poison(agent_id=agent_id, limit=limit)

    @app.get(
        "/api/registry",
        response_model=list[RegistryEntry],
        summary="Fleet snapshot",
        description=(
            "Return one row per registered agent with card metadata, "
            "JetStream queue depth, and poison event count. Used by the "
            "dashboard's Registry tab. Frontend filters infrastructure "
            "system agents (aggregator) from the chat sidebar by "
            "inspecting card.metadata.runtime.roles."
        ),
    )
    async def get_registry(deployment: str | None = None):
        rows = db.list_agents()
        if deployment is not None:
            rows = [
                r
                for r in rows
                if (r.get("deployment") or "default") == (deployment or "default")
            ]
        poison_counts = db.count_poison_by_agent()

        out: list[dict] = []
        agg = state["app"]
        for r in rows:
            r = with_effective_agent_state(r)
            queue = {"pending": 0, "ack_pending": 0}
            if agg is not None:
                try:
                    ci = await agg.router.js.consumer_info(
                        "AGENT_INBOX", f"{r['agent_id']}_inbox"
                    )
                    queue = {
                        "pending": ci.num_pending,
                        "ack_pending": ci.num_ack_pending,
                    }
                except Exception:
                    # consumer missing → graceful zero
                    pass
            out.append(
                {
                    "agent_id": r["agent_id"],
                    "card": r["card"],
                    "agent_state": r["agent_state"],
                    "last_heartbeat": r.get("last_heartbeat"),
                    "last_register": r["last_register"],
                    "deployment": r.get("deployment"),
                    "heartbeat_interval_sec": r.get("heartbeat_interval_sec", 30),
                    "queue": queue,
                    "poison_count": poison_counts.get(r["agent_id"], 0),
                }
            )
        return out

    @app.get(
        "/api/conversations",
        summary="Conversation snapshot",
        description=(
            "Group conversation_turns by (agent_id, context_id) and return "
            "one row per conversation with turn count, total tokens, "
            "first/last seen timestamps, and skills used. Used by future "
            "dashboard 'active conversations' views."
        ),
    )
    async def get_conversations(agent_id: str | None = None):
        return db.list_conversations(agent_id=agent_id)

    @app.post("/api/openclaw/login")
    async def openclaw_login(body: dict):
        session_id = body.get("session_id", "")
        import re

        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", session_id):
            raise HTTPException(422, "invalid session_id")
        # v0.1: stub — real per-session NATS JWT issuance is v0.2.
        # We return a short-lived opaque token the aggregator recognizes on the
        # openclaw.* ingress path.
        import uuid as _u
        from datetime import timedelta

        token = _u.uuid4().hex
        exp = (
            (datetime.now(timezone.utc) + timedelta(hours=1))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        _OPENCLAW_TOKENS[token] = session_id  # in-memory, resets on restart
        return {"token": token, "expires_at": exp, "agent_id": f"openclaw-{session_id}"}

    # ---- WebSocket fan-out ----
    #
    # Phase 1 follow-up: real-time push to the dashboard, replacing the
    # 3-30s polling loops in ChatHistory / AgentDetail / TaskBoard.
    # Two surfaces:
    #   /ws/stream         — global firehose; every persisted envelope.
    #   /ws/agent/{id}     — only envelopes whose sender_id or recipient_id
    #                        matches {id}; keeps single-agent panels quiet.
    # Wire format mirrors what frontend/src/hooks/useWebSocket.js expects:
    # {event, data} JSON frames, plus client-side "ping" keepalives that
    # the server discards.
    async def _ws_loop(ws: WebSocket) -> None:
        """Consume client frames so the connection stays alive. The
        frontend sends 'ping' strings every 15s; we accept anything and
        do nothing with it. Returns on disconnect."""
        try:
            while True:
                # receive_text raises WebSocketDisconnect on close
                await ws.receive_text()
        except WebSocketDisconnect:
            return
        except Exception:
            return

    @app.websocket("/ws/stream")
    async def ws_stream(ws: WebSocket):
        hub: WebSocketHub | None = state.get("hub")
        if hub is None:
            await ws.close(code=1011, reason="hub not ready")
            return
        await ws.accept()
        await hub.add_global(ws)
        try:
            await _ws_loop(ws)
        finally:
            await hub.remove(ws)

    @app.websocket("/ws/agent/{agent_id}")
    async def ws_agent(ws: WebSocket, agent_id: str):
        hub: WebSocketHub | None = state.get("hub")
        if hub is None:
            await ws.close(code=1011, reason="hub not ready")
            return
        await ws.accept()
        await hub.add_agent(agent_id, ws)
        try:
            await _ws_loop(ws)
        finally:
            await hub.remove(ws)

    return app


app = make_app()
