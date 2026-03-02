# OpenClaw Swarm Control Dashboard

## Architecture Overview

Full-stack agent-centric swarm control dashboard for an OpenClaw edge-local LLM agent swarm deployed on IoT hardware. The dashboard runs on the orchestrator node, subscribing to all MQTT traffic on `agents/#` to provide real-time visibility into multi-agent communication, chat history, task execution, and system logs.

**Key constraint**: MQTT is the backbone. All inter-agent communication flows through MQTT pub/sub. The dashboard is the supervisor view.

## Tech Stack

- **Backend**: Python 3.12, FastAPI, aiomqtt, SQLAlchemy (async + aiosqlite), Pydantic
- **Frontend**: React 18, Vite 5, Tailwind CSS 3, Zustand, react-force-graph-2d, Recharts, Axios
- **Infrastructure**: Docker Compose, Eclipse Mosquitto 2, Nginx
- **Database**: SQLite (async via aiosqlite)

## Directory Structure

```
EdgeCitadel/
├── backend/
│   ├── main.py              # FastAPI app entry with lifespan
│   ├── config.py             # Pydantic settings from env vars
│   ├── database.py           # Async SQLAlchemy models & engine
│   ├── mqtt_client.py        # MQTT subscriber/publisher service
│   ├── websocket_manager.py  # WebSocket connection pools
│   ├── schemas.py            # Pydantic request/response models
│   ├── services/             # Business logic layer
│   │   ├── agent_service.py
│   │   ├── message_service.py
│   │   ├── task_service.py
│   │   ├── log_service.py
│   │   └── health_monitor.py
│   └── routes/               # FastAPI route modules
│       ├── agents.py
│       ├── messages.py
│       ├── tasks.py
│       ├── logs.py
│       ├── commands.py
│       ├── system.py
│       └── websocket.py
├── frontend/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── Layout.jsx
│   │   ├── components/       # UI components
│   │   ├── stores/           # Zustand state
│   │   ├── hooks/            # Custom React hooks
│   │   ├── api/              # API client
│   │   └── utils/            # Helpers
│   └── ...config files
├── mosquitto/config/         # Broker configuration
├── docker-compose.yml
└── .env
```

## Key Patterns

- **MQTT topic structure**: `agents/{action}` or `agents/{category}/{agent_name}/{action}`
- **Message envelope**: JSON with sender, receiver, type, correlation_id, payload, timestamp
- **WebSocket channels**: `/ws/stream` (all), `/ws/agent/{name}` (per-agent), `/ws/logs` (logs only)
- **Real-time flow**: MQTT → Backend → DB + WebSocket → Frontend
- **Agent discovery**: Dynamic via MQTT registration and heartbeat messages
- **Health monitoring**: Background loop checks heartbeat freshness every 15s, marks agents offline after 60s timeout

## Conventions

- Backend uses async/await throughout
- All API routes prefixed with `/api/`
- Frontend uses Zustand for state management (single store)
- Dark theme by default, Tailwind CSS for styling
- All timestamps stored as UTC, displayed as local time in frontend
- SQLite database stored at `./data/openclaw.db`
