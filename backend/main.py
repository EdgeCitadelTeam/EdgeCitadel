import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from database import init_db
from mqtt_client import MQTTService
from websocket_manager import WebSocketManager
from services.health_monitor import health_monitor_loop
from services.orchestrator_service import orchestrator_heartbeat_loop
from routes import agents, messages, tasks, logs, commands, system, websocket

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing database...")
    await init_db()

    ws_mgr = WebSocketManager()
    mqtt_svc = MQTTService(ws_mgr)

    # Wire references into route modules
    websocket.ws_manager = ws_mgr
    system.ws_manager = ws_mgr
    system.mqtt_service = mqtt_svc
    commands.mqtt_service = mqtt_svc
    tasks.mqtt_service = mqtt_svc

    # Start background tasks
    mqtt_task = asyncio.create_task(mqtt_svc.start())
    health_task = asyncio.create_task(health_monitor_loop(ws_mgr))
    orch_task = asyncio.create_task(orchestrator_heartbeat_loop(mqtt_svc))
    _background_tasks.extend([mqtt_task, health_task, orch_task])

    logger.info("OpenClaw backend started")

    yield

    # Shutdown
    logger.info("Shutting down background tasks...")
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    _background_tasks.clear()
    logger.info("OpenClaw backend stopped")


app = FastAPI(
    title="OpenClaw Swarm Control",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(agents.router)
app.include_router(messages.router)
app.include_router(tasks.router)
app.include_router(logs.router)
app.include_router(commands.router)
app.include_router(system.router)
app.include_router(websocket.router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}
