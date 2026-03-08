# OpenClaw Edge Citadel

## Architecture Overview

MQTT-based agent communication platform. Runs a Mosquitto MQTT broker with agents (Rupert/Orchestrator, Jeeves/IoT, Percy/Mobile) publishing and subscribing to topics. The aggregator subscribes to all MQTT traffic, parses it into structured records (agents, messages, logs, tasks), stores in SQLite, and streams to a React dashboard via WebSocket.

## Tech Stack

- **MQTT Broker**: Eclipse Mosquitto 2
- **Aggregator**: Python 3.12, FastAPI, paho-mqtt 1.6.1, SQLite, Pydantic
- **Dashboard**: React 18, Vite 5, Tailwind CSS (build-time), Zustand, recharts, react-force-graph-2d, lucide-react
- **Infrastructure**: Docker Compose, Nginx (reverse proxy)
- **Database**: SQLite (sync, module-level connection)

## Directory Structure

```
EdgeCitadel/
├── aggregator/
│   ├── main.py           # FastAPI app, REST + WebSocket endpoints
│   ├── aggregator.py     # MQTT subscriber/publisher, message parser
│   ├── database.py       # SQLite DB (agents, messages, logs, tasks, episodes)
│   ├── models.py         # Pydantic request models
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── package.json
│   ├── vite.config.js
│   ├── tailwind.config.js
│   ├── index.html
│   ├── nginx.conf
│   ├── Dockerfile
│   └── src/
│       ├── main.jsx
│       ├── App.jsx
│       ├── Layout.jsx
│       ├── stores/appStore.js
│       ├── hooks/useWebSocket.js
│       ├── api/client.js
│       └── components/
│           ├── AgentSidebar.jsx
│           ├── AgentCard.jsx
│           ├── AgentDetail.jsx
│           ├── HeaderBar.jsx
│           ├── ChatHistory.jsx
│           ├── MessageBubble.jsx
│           ├── CommandInput.jsx
│           ├── ConversationThread.jsx
│           ├── CommFlow.jsx
│           ├── LogViewer.jsx
│           ├── TaskBoard.jsx
│           ├── TaskCard.jsx
│           ├── StatusBadge.jsx
│           └── Toast.jsx
├── mosquitto/
│   └── config/mosquitto.conf
├── nginx/
│   └── default.conf
├── openclaw-client/
│   ├── register.sh
│   └── openclaw.conf.example
├── data/
├── docker-compose.yml
└── .env.example
```

## Key Patterns

- **MQTT topic structure**: `openclaw/{deployment}/{agent}/...`, `agents/register/{agent}`, `agents/heartbeat/{agent}`
- **Real-time flow**: Mosquitto -> paho-mqtt threads -> parse + DB + WebSocket broadcast -> React frontend
- **WebSocket**: `/ws` (raw events), `/ws/stream` (structured events for frontend), `/ws/agent/{name}`
- **API auth**: `api-key` header checked against `API_KEY` env var (deployment endpoints only)
- **Frontend API**: `/api/agents`, `/api/messages`, `/api/logs`, `/api/tasks`, `/api/system/status`, `/api/command/{agent}` (no auth)
- **Agent auto-discovery**: Agents are created/updated from MQTT topic parsing and payload fields
- **paho-mqtt 1.6.1**: Use `mqtt.Client(client_id=...)` constructor, NOT 2.x API

## Conventions

- Aggregator uses sync SQLite with module-level connection
- paho-mqtt `loop_start()` runs in background threads; use `asyncio.run_coroutine_threadsafe()` to bridge to async
- All API routes served behind nginx at `/api/` prefix (strips prefix via proxy_pass)
- Dashboard uses Tailwind CSS with build-time PostCSS (not CDN)
- Zustand for state management, no API key needed for frontend endpoints
- Dark theme, Tailwind utility classes, custom color palette in tailwind.config.js
- SQLite database at `/data/openclaw.db` (inside container)
