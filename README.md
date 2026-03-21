# EdgeCitadel

Hybrid NATS + MQTT agent communication platform. Connects edge devices (Raspberry Pis, Mac Minis, cloud VMs) through a single NATS server with built-in MQTT adapter, JetStream persistence, and a real-time dashboard.

## Demo

![EdgeCitadel Dashboard Demo](docs/demo.gif)

## Quick Start

```bash
git clone https://github.com/zhonghaozhan/EdgeCitadel.git && cd EdgeCitadel
cp .env.example .env              # set NATS_TOKEN and OPENCLAW_API_KEY
mkdir -p data nats/data
docker compose up --build -d
```

Verify: `curl http://localhost:8222/healthz` (NATS) and open `http://localhost` (dashboard).

## Add an Agent

**On the server:**

```bash
./add-agent.sh my-agent
```

**On the agent's machine:**

```bash
git clone https://github.com/zhonghaozhan/EdgeCitadel.git && cd EdgeCitadel
./join.sh <server-ip> <nats-token> my-agent
```

The agent auto-detects its hostname, device type, and local OpenClaw installation. It registers over MQTT and appears on the dashboard within seconds.

## Managing Agents

```bash
journalctl --user -u edgecitadel-my-agent -f        # logs
systemctl --user restart edgecitadel-my-agent        # restart
systemctl --user stop edgecitadel-my-agent           # stop
```

---

## Architecture

```mermaid
graph TB
    subgraph Server["EdgeCitadel Server"]
        NATS["NATS 2.10<br/>JetStream + MQTT Adapter"]
        AGG["Aggregator<br/>(FastAPI + nats-py)"]
        DB[(SQLite)]
        NG["Nginx"]
        UI["React Dashboard"]

        NATS -->|native NATS :4222| AGG
        AGG --> DB
        AGG <-->|WebSocket| NG
        UI --- NG
    end

    subgraph Edge["Edge Agents"]
        A1["Mac Mini"]
        A2["Raspberry Pi"]
        A3["EC2 Instance"]
    end

    A1 <-->|"MQTT :1883"| NATS
    A2 <-->|"MQTT :1883"| NATS
    A3 <-->|"MQTT :1883"| NATS

    Browser["Browser"] <-->|":80"| NG
```

There is **no separate MQTT broker**. NATS 2.10 has a built-in MQTT adapter that translates MQTT topics (`agents/name/action`) to NATS subjects (`agents.name.action`) automatically.

- **IoT agents** connect via MQTT on port 1883 using `NATS_TOKEN` as password
- **Aggregator** connects via native NATS on port 4222
- **Dashboard** receives updates via WebSocket through Nginx

## Communication Flow

```mermaid
sequenceDiagram
    participant Agent as Edge Agent (MQTT)
    participant NATS as NATS Server
    participant Agg as Aggregator
    participant UI as Dashboard

    Agent->>NATS: PUB agents/{id}/register (retained)
    NATS-->>Agg: agents.{id}.register
    Agg->>Agg: Store in SQLite
    Agg-->>UI: WebSocket push

    loop Every 30s
        Agent->>NATS: PUB agents/{id}/heartbeat
        NATS-->>Agg: agents.{id}.heartbeat
    end

    UI->>Agg: POST /api/agents/{id}/command
    Agg->>NATS: PUB agents.{id}.inbox
    NATS-->>Agent: agents/{id}/inbox
    Agent->>Agent: Execute (openclaw CLI)
    Agent->>NATS: PUB agents/{id}/outbox
    NATS-->>Agg: agents.{id}.outbox
    Agg-->>UI: WebSocket push
```

Agents can also delegate tasks peer-to-peer by publishing to another agent's inbox topic, up to 3 levels deep with loop detection.

## Subjects & Topics

| MQTT Topic (agent-side) | NATS Subject (server-side) | Purpose |
|---|---|---|
| `agents/{id}/register` | `agents.{id}.register` | Agent registration |
| `agents/{id}/heartbeat` | `agents.{id}.heartbeat` | Health metrics |
| `agents/{id}/inbox` | `agents.{id}.inbox` | Commands to agent |
| `agents/{id}/outbox` | `agents.{id}.outbox` | Responses from agent |
| `agents/{id}/status` | `agents.{id}.status` | Online/offline |
| `agents/{id}/log` | `agents.{id}.log` | Log entries |
| `tasks/{id}/assign` | `tasks.{id}.assign` | Task assignment |
| `tasks/{id}/complete` | `tasks.{id}.complete` | Task completion |
| `system/broadcast` | `system.broadcast` | Broadcast to all |

## Services & Ports

| Port | Protocol | Service |
|------|----------|---------|
| 80 | HTTP | Nginx (dashboard + API + WebSocket) |
| 1883 | MQTT | NATS MQTT adapter (agent connections) |
| 4222 | NATS | Native NATS (aggregator) |
| 8222 | HTTP | NATS monitoring (`/healthz`, `/varz`, `/jsz`) |

---

## Coding Agent Compatibility

This repo is structured to work with both Codex and Claude Code.

- Shared coding-agent instructions live in `AGENTS.md`.
- `CLAUDE.md` is a thin Claude compatibility wrapper that imports the shared instructions.
- Subproject-specific guidance lives in nested `AGENTS.md` files under active paths such as `aggregator/`, `frontend/`, `openclaw-client/`, and `e2e/`.
- Shared Claude project settings live in `.claude/settings.json`; local-only Claude overrides belong in `.claude/settings.local.json`.

For setup and verification details, see `docs/agent-setup.md`.

### Verification Expectations

- UI, browser-flow, and operator-workflow changes must include actual Playwright verification from `e2e/`; curl-only smoke checks are not sufficient.
- Shared workflow, repo-structure, Docker, or agent-config changes should restart the stack and then run smoke checks plus the narrowest relevant Playwright coverage.

---

## Documentation

| Topic | Link |
|-------|------|
| Architecture | [docs/01-architecture.md](docs/01-architecture.md) |
| Server Setup | [docs/02-server-setup.md](docs/02-server-setup.md) |
| Agent Registration | [docs/03-agent-registration.md](docs/03-agent-registration.md) |
| Messaging Protocol | [docs/05-messaging.md](docs/05-messaging.md) |
| P2P Delegation | [docs/06-p2p-delegation.md](docs/06-p2p-delegation.md) |
| Task Management | [docs/07-task-management.md](docs/07-task-management.md) |
| API Reference | [docs/08-api-reference.md](docs/08-api-reference.md) |
| Monitoring | [docs/09-monitoring.md](docs/09-monitoring.md) |

## Development

```bash
# Aggregator
cd aggregator && ruff check --fix && ruff format && mypy --strict && pytest tests/ -x

# Frontend
cd frontend && npm run lint && npm run build

# E2E
cd e2e && npx playwright test
```

## License

MIT
