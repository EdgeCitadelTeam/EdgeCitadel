import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import database
from aggregator import OpenClawAggregator
from models import DeploymentConfig, PublishRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.environ.get("API_KEY", "change-me")

agg = OpenClawAggregator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db()

    # Connect to NATS with retry loop
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            await agg.connect()
            break
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Failed to connect to NATS after {max_retries} attempts: {e}")
                raise RuntimeError(f"Cannot start without NATS: {e}")
            logger.warning(f"NATS connection attempt {attempt}/{max_retries} failed: {e}, retrying in 2s...")
            await asyncio.sleep(2)

    # Start heartbeat monitor background task
    hb_interval = int(os.environ.get("HEARTBEAT_INTERVAL", "15"))
    hb_timeout = int(os.environ.get("HEARTBEAT_TIMEOUT", "120"))
    heartbeat_task = asyncio.create_task(agg.heartbeat_monitor(interval=hb_interval, timeout=hb_timeout))

    yield

    heartbeat_task.cancel()
    await agg.disconnect()


app = FastAPI(title="OpenClaw Edge Citadel", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
@app.get("/api/health")
def health_check():
    return {"status": "ok"}


# ── Deployment endpoints (kept for backward compat) ──

@app.post("/deployments/register")
def register_deployment(config: DeploymentConfig, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    database.upsert_deployment(
        config.name, config.host, config.port, config.description, config.network,
        config.mqtt_user, config.mqtt_pass
    )

    return {"ok": True, "message": f"{config.name} registered (MQTT handles connectivity)"}


@app.delete("/deployments/{name}")
def remove_deployment(name: str, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    database.deactivate_deployment(name)
    return {"ok": True, "message": f"{name} deregistered"}


@app.get("/deployments")
def list_deployments(api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    deployments = database.get_all_active_deployments()
    for dep in deployments:
        dep["connected"] = agg.nc is not None and agg.nc.is_connected
    return deployments


@app.get("/deployments/{name}/status")
def deployment_status(name: str, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    dep = database.get_deployment(name)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment not found")

    connected = agg.nc is not None and agg.nc.is_connected
    return {
        "name": dep["name"],
        "host": dep["host"],
        "port": dep["port"],
        "connected": connected,
        "status": "online" if connected else "offline",
    }


@app.post("/publish")
async def publish_message(req: PublishRequest, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        await agg.publish(req.topic, req.payload)
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Not connected to NATS")

    return {"ok": True}


@app.get("/history")
def get_history(
    api_key: str = Header(..., alias="api-key"),
    deployment: str | None = Query(None),
    limit: int = Query(100),
):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    return database.get_episodes(deployment=deployment, limit=limit)


# ── Frontend-compatible endpoints ──

@app.get("/agents")
def list_agents(exclude_test: bool = Query(False)):
    return database.get_all_agents(exclude_test=exclude_test)


@app.post("/agents", status_code=201)
def create_agent(data: dict):
    agent_id = data.get("id", "")
    if not agent_id:
        raise HTTPException(status_code=400, detail="Agent id required")
    existing = database.get_agent(agent_id)
    if existing:
        raise HTTPException(status_code=409, detail="Agent already exists")
    database.upsert_agent(agent_id, **{k: v for k, v in data.items() if k != "id"})
    return database.get_agent(agent_id)


@app.get("/agents/{agent_id}")
def get_agent(agent_id: str):
    agent = database.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@app.patch("/agents/{agent_id}")
def update_agent(agent_id: str, data: dict):
    agent = database.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    database.upsert_agent(agent_id, **data)
    return database.get_agent(agent_id)


@app.delete("/agents/{agent_id}", status_code=204)
def delete_agent(agent_id: str):
    database.delete_agent(agent_id)


@app.get("/agents/{agent_id}/stats")
def get_agent_stats(agent_id: str):
    return database.get_agent_stats(agent_id)


@app.get("/messages")
def list_messages(
    limit: int = Query(50),
    offset: int = Query(0),
    agent: str = Query(""),
    type: str = Query(""),
    search: str = Query(""),
    correlation_id: str = Query(""),
    exclude_test: bool = Query(False),
):
    items = database.get_messages(
        limit=limit, offset=offset, agent=agent, msg_type=type,
        search=search, correlation_id=correlation_id, exclude_test=exclude_test,
    )
    return {"items": items}


@app.get("/messages/conversations")
def list_conversations(
    limit: int = Query(50),
    agent: str = Query(""),
):
    return database.get_messages(limit=limit, agent=agent)


@app.get("/messages/flow")
def get_message_flow(hours: int = Query(24), exclude_test: bool = Query(False)):
    return database.get_message_flow(hours=hours, exclude_test=exclude_test)


@app.get("/tasks")
def list_tasks(
    limit: int = Query(200),
    agent: str = Query(""),
    status: str = Query(""),
    exclude_test: bool = Query(False),
):
    items = database.get_tasks(limit=limit, agent=agent, status=status, exclude_test=exclude_test)
    return {"items": items}


@app.post("/tasks", status_code=201)
async def create_task(data: dict):
    task_id = database.insert_task(
        title=data.get("title", ""),
        description=data.get("description", ""),
        assigned_agent=data.get("assigned_agent", ""),
        priority=data.get("priority", "normal"),
    )
    task = database.get_task(task_id)
    # Publish task assignment via NATS
    assigned = data.get("assigned_agent", "")
    if assigned:
        try:
            payload = json.dumps({
                "task_id": task_id,
                "title": data.get("title", ""),
                "description": data.get("description", ""),
                "priority": data.get("priority", "normal"),
                "assigned_agent": assigned,
            })
            await agg.publish(f"tasks.{task_id}.assign", payload)
        except Exception:
            pass
    return task


@app.patch("/tasks/{task_id}")
def update_task(task_id: str, data: dict):
    # Auto-set timestamps based on status transitions
    if data.get("status") == "running" and "started_at" not in data:
        data["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if data.get("status") in ("completed", "failed") and "completed_at" not in data:
        data["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    database.update_task(task_id, **data)
    return database.get_task(task_id)


@app.get("/tasks/{task_id}/trace")
def get_task_trace(task_id: str):
    return database.get_task_trace(task_id)


@app.get("/logs")
def list_logs(
    limit: int = Query(200),
    level: str = Query(""),
    agent: str = Query(""),
    agent_id: str = Query(""),
    source: str = Query(""),
    search: str = Query(""),
    exclude_test: bool = Query(False),
):
    agent_filter = agent or agent_id
    items = database.get_logs(
        limit=limit, level=level, agent=agent_filter, source=source, search=search,
        exclude_test=exclude_test,
    )
    return {"items": items}


@app.post("/command/{agent_name}")
async def send_command(agent_name: str, data: dict):
    """Send a command to an agent via NATS."""
    # Inject sender/receiver/correlation if not already present
    if "sender_id" not in data:
        data["sender_id"] = "dashboard"
    if "receiver_id" not in data:
        data["receiver_id"] = agent_name
    if "correlation_id" not in data:
        data["correlation_id"] = str(uuid.uuid4())

    correlation_id = data["correlation_id"]
    payload = json.dumps(data)

    try:
        await agg.publish(f"agents.{agent_name}.inbox", payload)
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Not connected to NATS")

    return {"ok": True, "correlation_id": correlation_id}


@app.post("/broadcast")
async def broadcast_message(data: dict):
    """Broadcast a message to all agents via NATS."""
    payload = json.dumps(data)
    try:
        await agg.publish("system.broadcast", payload)
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Not connected to NATS")

    return {"ok": True}


@app.get("/system/status")
def system_status(exclude_test: bool = Query(False)):
    status = database.get_system_status(exclude_test=exclude_test)
    status["nats_connected"] = agg.nc is not None and agg.nc.is_connected
    status["mqtt_connected"] = status["nats_connected"]
    return status


@app.get("/system/topology")
def system_topology(hours: int = Query(24), exclude_test: bool = Query(False)):
    return database.get_message_flow(hours=hours, exclude_test=exclude_test)


# ── WebSocket endpoints ──

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    agg.ws_connections.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            agg.ws_connections.remove(ws)
        except ValueError:
            pass


@app.websocket("/ws/stream")
async def websocket_stream(ws: WebSocket):
    """WebSocket endpoint for the original frontend UI."""
    await ws.accept()
    agg.stream_connections.append(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                continue
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            agg.stream_connections.remove(ws)
        except ValueError:
            pass


@app.websocket("/ws/agent/{agent_name}")
async def websocket_agent(ws: WebSocket, agent_name: str):
    """WebSocket endpoint for agent-specific messages."""
    await ws.accept()
    agg.stream_connections.append(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                continue
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            agg.stream_connections.remove(ws)
        except ValueError:
            pass
