import asyncio
import json
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(self):
        self._global_clients: set[WebSocket] = set()
        self._agent_clients: dict[str, set[WebSocket]] = {}
        self._log_clients: set[WebSocket] = set()

    def connect_global(self, ws: WebSocket):
        self._global_clients.add(ws)

    def disconnect_global(self, ws: WebSocket):
        self._global_clients.discard(ws)

    def connect_agent(self, agent_name: str, ws: WebSocket):
        if agent_name not in self._agent_clients:
            self._agent_clients[agent_name] = set()
        self._agent_clients[agent_name].add(ws)

    def disconnect_agent(self, agent_name: str, ws: WebSocket):
        if agent_name in self._agent_clients:
            self._agent_clients[agent_name].discard(ws)
            if not self._agent_clients[agent_name]:
                del self._agent_clients[agent_name]

    def connect_logs(self, ws: WebSocket):
        self._log_clients.add(ws)

    def disconnect_logs(self, ws: WebSocket):
        self._log_clients.discard(ws)

    async def broadcast(self, message: dict):
        data = json.dumps(message)
        dead: list[WebSocket] = []
        for ws in self._global_clients:
            await self._safe_send(ws, data, dead)
        for ws in dead:
            self._global_clients.discard(ws)

    async def send_to_agent_subscribers(self, agent_name: str, message: dict):
        clients = self._agent_clients.get(agent_name, set())
        if not clients:
            return
        data = json.dumps(message)
        dead: list[WebSocket] = []
        for ws in clients:
            await self._safe_send(ws, data, dead)
        for ws in dead:
            clients.discard(ws)

    async def broadcast_log(self, log_entry: dict):
        data = json.dumps(log_entry)
        dead: list[WebSocket] = []
        for ws in self._log_clients:
            await self._safe_send(ws, data, dead)
        for ws in dead:
            self._log_clients.discard(ws)

    async def _safe_send(self, ws: WebSocket, data: str, dead: list[WebSocket]):
        try:
            await asyncio.wait_for(ws.send_text(data), timeout=5.0)
        except Exception:
            dead.append(ws)

    @property
    def connection_count(self) -> dict:
        agent_count = sum(len(s) for s in self._agent_clients.values())
        return {
            "global": len(self._global_clients),
            "agent": agent_count,
            "logs": len(self._log_clients),
            "total": len(self._global_clients) + agent_count + len(self._log_clients),
        }
