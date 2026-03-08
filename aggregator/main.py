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
    agg.loop = asyncio.get_event_loop()

    deployments = database.get_all_active_deployments()
    for dep in deployments:
        try:
            agg.connect_deployment(dep["name"], dep["host"], dep["port"],
                                       dep.get("mqtt_user", ""), dep.get("mqtt_pass", ""))
        except Exception as e:
            logger.error(f"Failed to connect to {dep['name']}: {e}")

    # Start heartbeat monitor background task
    heartbeat_task = asyncio.create_task(agg.heartbeat_monitor(interval=15, timeout=120))

    yield

    heartbeat_task.cancel()
    agg.disconnect_all()


app = FastAPI(title="OpenClaw Edge Citadel", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Deployment endpoints (Edge Citadel specific) ──

@app.post("/deployments/register")
def register_deployment(config: DeploymentConfig, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    database.upsert_deployment(
        config.name, config.host, config.port, config.description, config.network,
        config.mqtt_user, config.mqtt_pass
    )

    try:
        agg.connect_deployment(config.name, config.host, config.port,
                               config.mqtt_user, config.mqtt_pass)
    except Exception as e:
        logger.error(f"Failed to connect to {config.name}: {e}")

    if agg.loop:
        asyncio.run_coroutine_threadsafe(
            agg._broadcast({"event": "deployment_added", "deployment": config.name}),
            agg.loop,
        )

    return {"ok": True, "message": f"{config.name} registered and connected"}


@app.delete("/deployments/{name}")
def remove_deployment(name: str, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    database.deactivate_deployment(name)

    try:
        agg.disconnect_deployment(name)
    except KeyError:
        pass

    if agg.loop:
        asyncio.run_coroutine_threadsafe(
            agg._broadcast({"event": "deployment_removed", "deployment": name}),
            agg.loop,
        )

    return {"ok": True, "message": f"{name} deregistered"}


@app.get("/deployments")
def list_deployments(api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    deployments = database.get_all_active_deployments()
    for dep in deployments:
        dep["connected"] = dep["name"] in agg.clients
    return deployments


@app.get("/deployments/{name}/status")
def deployment_status(name: str, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    dep = database.get_deployment(name)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment not found")

    connected = name in agg.clients
    return {
        "name": dep["name"],
        "host": dep["host"],
        "port": dep["port"],
        "connected": connected,
        "status": "online" if connected else "offline",
    }


@app.post("/publish")
def publish_message(req: PublishRequest, api_key: str = Header(..., alias="api-key")):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        agg.publish(req.deployment, req.topic, req.payload)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Deployment '{req.deployment}' not connected")

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
def list_agents():
    return database.get_all_agents()


@app.get("/agents/{agent_id}")
def get_agent(agent_id: str):
    agent = database.get_agent(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


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
):
    items = database.get_messages(
        limit=limit, offset=offset, agent=agent, msg_type=type,
        search=search, correlation_id=correlation_id,
    )
    return {"items": items}


@app.get("/messages/conversations")
def list_conversations(
    limit: int = Query(50),
    agent: str = Query(""),
):
    items = database.get_messages(limit=limit, agent=agent)
    return {"items": items}


@app.get("/messages/flow")
def get_message_flow(hours: int = Query(24)):
    return database.get_message_flow(hours=hours)


@app.get("/tasks")
def list_tasks(
    limit: int = Query(200),
    agent: str = Query(""),
):
    items = database.get_tasks(limit=limit, agent=agent)
    return {"items": items}


@app.post("/tasks")
def create_task(data: dict):
    task_id = database.insert_task(
        title=data.get("title", ""),
        description=data.get("description", ""),
        assigned_agent=data.get("assigned_agent", ""),
        priority=data.get("priority", "normal"),
    )
    return {"id": task_id, "ok": True}


@app.patch("/tasks/{task_id}")
def update_task(task_id: str, data: dict):
    database.update_task(task_id, **data)
    return {"ok": True}


@app.get("/tasks/{task_id}/trace")
def get_task_trace(task_id: str):
    return database.get_task_trace(task_id)


@app.get("/logs")
def list_logs(
    limit: int = Query(200),
    level: str = Query(""),
    agent: str = Query(""),
    source: str = Query(""),
    search: str = Query(""),
):
    items = database.get_logs(
        limit=limit, level=level, agent=agent, source=source, search=search,
    )
    return {"items": items}


@app.post("/command/{agent_name}")
def send_command(agent_name: str, data: dict):
    """Send a command to an agent via MQTT."""
    # Find which deployment this agent belongs to
    agent = database.get_agent(agent_name)
    deployment = agent["deployment"] if agent else ""

    # Verify the deployment is actually connected, otherwise fall back
    if not deployment or deployment not in agg.clients:
        deployment = ""
        for dep in database.get_all_active_deployments():
            if dep["name"] in agg.clients:
                deployment = dep["name"]
                break

    if not deployment:
        raise HTTPException(status_code=404, detail="No connected deployment found")

    # Inject sender/receiver/correlation if not already present
    if "sender_id" not in data:
        data["sender_id"] = "dashboard"
    if "receiver_id" not in data:
        data["receiver_id"] = agent_name
    if "correlation_id" not in data:
        data["correlation_id"] = str(uuid.uuid4())

    # Publish the command to MQTT on both topic conventions:
    # 1. openclaw/{deployment}/{agent}/cmd  — EdgeCitadel native format
    # 2. agents/inbox/{agent}               — OpenClaw listener format
    correlation_id = data["correlation_id"]
    payload_native = json.dumps(data)

    # OpenClaw listener format: {from, to, type, content, timestamp, correlationId}
    content = ""
    if isinstance(data.get("payload"), dict):
        content = data["payload"].get("message", "")
    elif isinstance(data.get("payload"), str):
        content = data["payload"]
    payload_listener = json.dumps({
        "from": data.get("sender_id", "dashboard"),
        "to": agent_name,
        "type": data.get("message_type", "command"),
        "content": content,
        "message": content,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "correlationId": correlation_id,
    })

    try:
        agg.publish(deployment, f"openclaw/{deployment}/{agent_name}/cmd", payload_native)
        agg.publish(deployment, f"agents/inbox/{agent_name}", payload_listener)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Deployment '{deployment}' not connected")

    return {"ok": True, "correlation_id": correlation_id}


@app.post("/broadcast")
def broadcast_message(data: dict):
    """Broadcast a message to all connected deployments."""
    payload = json.dumps(data)
    for dep_name, client in agg.clients.items():
        try:
            topic = f"openclaw/{dep_name}/broadcast"
            client.publish(topic, payload)
        except Exception as e:
            logger.error(f"Failed to broadcast to {dep_name}: {e}")

    return {"ok": True}


@app.get("/system/status")
def system_status():
    return database.get_system_status()


@app.get("/system/topology")
def system_topology(hours: int = Query(24)):
    return database.get_message_flow(hours=hours)


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
