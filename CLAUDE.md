# OpenClaw Edge Citadel

## Architecture Overview

NATS-based agent communication platform. Runs a NATS server with JetStream for persistence. Agents (Rupert/Orchestrator, Jeeves/IoT, Percy/Mobile) publish and subscribe to NATS subjects. The aggregator subscribes to all agent/task/system subjects, parses messages into structured records (agents, messages, logs, tasks), stores in SQLite, and streams to a React dashboard via WebSocket.

## Tech Stack

- **Messaging**: NATS 2.10+ with JetStream (pub/sub, request-reply, persistent streams, K/V store)
- **Aggregator**: Python 3.12, FastAPI, nats-py, SQLite, Pydantic
- **Dashboard**: React 18, Vite 5, Tailwind CSS (build-time), Zustand, recharts, react-force-graph-2d, lucide-react
- **Infrastructure**: Docker Compose, Nginx (reverse proxy)
- **Database**: SQLite (sync, module-level connection)

## Directory Structure

```
EdgeCitadel/
├── aggregator/
│   ├── main.py           # FastAPI app, REST + WebSocket endpoints
│   ├── aggregator.py     # NATS subscriber/publisher, message parser
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
├── nats/
│   └── data/             # JetStream storage
├── nginx/
│   └── default.conf
├── openclaw-client/
│   ├── nats-listener.js  # Agent listener (NATS)
│   ├── register.sh
│   └── openclaw.conf.example
├── data/
├── docker-compose.yml
└── .env.example
```

## Key Patterns

- **NATS subject structure**: `agents.{name}.{action}` (heartbeat, register, inbox, outbox, status, log), `tasks.{id}.{action}` (assign, stream, complete, failed), `system.broadcast`
- **Real-time flow**: NATS server -> async nats-py subscriptions -> parse + DB + WebSocket broadcast -> React frontend
- **WebSocket**: `/ws` (raw events), `/ws/stream` (structured events for frontend), `/ws/agent/{name}`
- **API auth**: `api-key` header checked against `API_KEY` env var (deployment endpoints only)
- **Frontend API**: `/api/agents`, `/api/messages`, `/api/logs`, `/api/tasks`, `/api/system/status`, `/api/command/{agent}` (no auth)
- **Agent auto-discovery**: Agents are created/updated from NATS subject parsing and payload fields
- **JetStream**: `CONVERSATIONS` stream for persistent message history, `AGENT_STATE` K/V bucket for live state

## Conventions

- Aggregator uses sync SQLite with module-level connection
- nats-py async subscriptions run natively in the FastAPI event loop (no thread bridging needed)
- All API routes served behind nginx at `/api/` prefix (strips prefix via proxy_pass)
- Dashboard uses Tailwind CSS with build-time PostCSS (not CDN)
- Zustand for state management, no API key needed for frontend endpoints
- Dark theme, Tailwind utility classes, custom color palette in tailwind.config.js
- SQLite database at `/data/openclaw.db` (inside container)
- NATS server at port 4222 (client), 8222 (monitoring HTTP)
