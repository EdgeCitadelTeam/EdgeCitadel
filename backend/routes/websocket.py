import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["websocket"])

logger = logging.getLogger(__name__)

# Reference set by main.py at startup
ws_manager = None


@router.websocket("/ws/stream")
async def ws_stream(ws: WebSocket):
    await ws.accept()
    ws_manager.connect_global(ws)
    try:
        while True:
            # Keep connection alive, handle client pings
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect_global(ws)


@router.websocket("/ws/agent/{agent_name}")
async def ws_agent(ws: WebSocket, agent_name: str):
    await ws.accept()
    ws_manager.connect_agent(agent_name, ws)
    ws_manager.connect_global(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect_agent(agent_name, ws)
        ws_manager.disconnect_global(ws)


@router.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    ws_manager.connect_logs(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        ws_manager.disconnect_logs(ws)
