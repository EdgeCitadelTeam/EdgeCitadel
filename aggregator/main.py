from __future__ import annotations
import json, os, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse

from . import database as db
from .aggregator import AggregatorApp, now_iso
from .models import CommandRequest, CommandResponse
from .websocket_hub import WebSocketHub


_OPENCLAW_TOKENS: dict[str, str] = {}


def make_app(for_testing: bool = False) -> FastAPI:
    app = FastAPI(title="EdgeCitadel Aggregator", version="0.1.0")
    state: dict = {"app": None}

    db_path = os.environ.get("DB_PATH", "/data/openclaw.db")
    envelope_schema = Path(os.environ.get(
        "ENVELOPE_SCHEMA_PATH",
        str(Path(__file__).resolve().parents[1] / "schemas" / "envelope.v1.json")))
    card_schema = Path(os.environ.get(
        "CARD_SCHEMA_PATH",
        str(Path(__file__).resolve().parents[1] / "schemas" / "agent-card.v1.json")))

    db.init_db(db_path)

    @app.on_event("startup")
    async def _startup():
        if for_testing:
            state["app"] = None
            state["hub"] = WebSocketHub()
            return
        nats_url = os.environ["NATS_URL"]
        nats_token = os.environ["NATS_TOKEN"]
        agg = AggregatorApp(nats_url=nats_url, nats_token=nats_token,
                            db_path=db_path,
                            envelope_schema=envelope_schema,
                            card_schema=card_schema)
        # Wire the WebSocket fan-out hub onto the router BEFORE start()
        # so any boot-time envelopes (self-register, request_register
        # broadcast) reach connected dashboards.
        hub = WebSocketHub()
        agg.router.hub = hub
        state["hub"] = hub
        await agg.start()
        state["app"] = agg

    @app.on_event("shutdown")
    async def _shutdown():
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
        return {"nats_connected": nats_connected,
                "jetstream_stream_ok": jetstream_ok,
                "version": "0.1.0"}

    @app.get("/api/agents")
    async def list_agents():
        agents = db.list_agents()
        # exclude self-cached aggregator entry from peer list
        return [a for a in agents if a["agent_id"] != "aggregator"]

    @app.get("/api/agents/{agent_id}")
    async def get_agent(agent_id: str):
        a = db.get_agent(agent_id)
        if not a: raise HTTPException(404, "agent not found")
        return a

    @app.get("/api/agents/{agent_id}/card")
    async def get_agent_card(agent_id: str):
        a = db.get_agent(agent_id)
        if not a: raise HTTPException(404, "agent not found")
        return a["card"]

    @app.delete("/api/agents/{agent_id}", status_code=204)
    async def delete_agent(agent_id: str):
        if agent_id == "aggregator":
            raise HTTPException(400, "cannot delete self")
        ok = db.delete_agent(agent_id)
        if not ok: raise HTTPException(404, "agent not found")
        return PlainTextResponse(status_code=204)

    @app.get("/api/agents/{agent_id}/queue")
    async def get_queue(agent_id: str):
        agg = state["app"]
        if agg is None:
            raise HTTPException(503, "jetstream not initialized")
        try:
            ci = await agg.router.js.consumer_info("AGENT_INBOX",
                                                   f"{agent_id}_inbox")
        except Exception as e:
            raise HTTPException(404, f"consumer not found: {e}")
        return {"pending": ci.num_pending,
                "ack_pending": ci.num_ack_pending,
                "num_waiting": getattr(ci, "num_waiting", 0)}

    @app.post("/api/command/{agent_id}", status_code=202,
              response_model=CommandResponse)
    async def post_command(agent_id: str, req: CommandRequest,
                           sender_id: str | None = None):
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
        # Auto-register a synthetic test-deployment card for non-default
        # senders so deployment tagging propagates without requiring a real
        # NATS register envelope from the caller.
        if (agg is not None and actual_sender != "aggregator"
                and actual_sender not in agg.router.cache):
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

        task_id = str(uuid.uuid4())
        env = {
            "v": 1, "id": str(uuid.uuid4()), "type": "command",
            "sender_id": actual_sender, "recipient_id": agent_id,
            "task_id": task_id, "timestamp": now_iso(),
            "payload": {"body": req.body, **({"args": req.args} if req.args else {})}
        }
        if agg is not None:
            # Publish JetStream with Nats-Msg-Id for idempotency
            await agg.router.js.publish(f"agents.{agent_id}.inbox",
                                        json.dumps(env).encode(),
                                        headers={"Nats-Msg-Id": env["id"]})
            # Mirror on the SENDER's outbox (was always aggregator before;
            # now matches the actual sender so test-runner traffic tags
            # consistently).
            await agg.router.nc.publish(f"agents.{actual_sender}.outbox",
                                        json.dumps(env).encode())
        return CommandResponse(task_id=task_id, recipient_id=agent_id,
                               accepted_at=env["timestamp"])

    @app.get("/api/messages")
    async def query_messages(agent_id: str | None = None,
                             task_id: str | None = None,
                             context_id: str | None = None,
                             type: str | None = None,
                             deployment: str | None = None,
                             exclude_deployment: str | None = None,
                             limit: int = 500):
        return db.query_messages(agent_id=agent_id, task_id=task_id,
                                 context_id=context_id, type=type,
                                 deployment=deployment,
                                 exclude_deployment=exclude_deployment,
                                 limit=limit)

    @app.get("/api/poison")
    async def query_poison(agent_id: str | None = None, limit: int = 100):
        return db.recent_poison(agent_id=agent_id, limit=limit)

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
        from datetime import datetime, timezone, timedelta
        token = _u.uuid4().hex
        exp = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(
            timespec="milliseconds").replace("+00:00", "Z")
        _OPENCLAW_TOKENS[token] = session_id  # in-memory, resets on restart
        return {"token": token, "expires_at": exp,
                "agent_id": f"openclaw-{session_id}"}

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
