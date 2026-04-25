from __future__ import annotations
import json, os, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, PlainTextResponse

from . import database as db
from .aggregator import AggregatorApp, now_iso
from .models import CommandRequest, CommandResponse


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
            return
        nats_url = os.environ["NATS_URL"]
        nats_token = os.environ["NATS_TOKEN"]
        agg = AggregatorApp(nats_url=nats_url, nats_token=nats_token,
                            db_path=db_path,
                            envelope_schema=envelope_schema,
                            card_schema=card_schema)
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
    async def post_command(agent_id: str, req: CommandRequest):
        agg = state["app"]
        task_id = str(uuid.uuid4())
        env = {
            "v": 1, "id": str(uuid.uuid4()), "type": "command",
            "sender_id": "aggregator", "recipient_id": agent_id,
            "task_id": task_id, "timestamp": now_iso(),
            "payload": {"body": req.body, **({"args": req.args} if req.args else {})}
        }
        if agg is not None:
            # Publish JetStream with Nats-Msg-Id for idempotency
            await agg.router.js.publish(f"agents.{agent_id}.inbox",
                                        json.dumps(env).encode(),
                                        headers={"Nats-Msg-Id": env["id"]})
            # also mirror on own outbox
            await agg.router.nc.publish("agents.aggregator.outbox",
                                        json.dumps(env).encode())
        return CommandResponse(task_id=task_id, recipient_id=agent_id,
                               accepted_at=env["timestamp"])

    @app.get("/api/messages")
    async def query_messages(agent_id: str | None = None,
                             task_id: str | None = None,
                             context_id: str | None = None,
                             type: str | None = None, limit: int = 500):
        return db.query_messages(agent_id=agent_id, task_id=task_id,
                                 context_id=context_id, type=type, limit=limit)

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

    return app


app = make_app()
