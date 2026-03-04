# EdgeCitadel

Real-time swarm control dashboard for [OpenClaw](https://github.com/openclaw) edge-local LLM agent networks. Runs on the orchestrator node, subscribing to all MQTT traffic on `agents/#` to provide live visibility into multi-agent communication, task execution, and system health.

## Features

- **Agent Registry** — Auto-discovers agents via MQTT registration and heartbeat messages. Shows online/offline status, CPU/memory metrics, device type, and capabilities.
- **Chat History** — Full message timeline across all agents with sender/receiver tracking, filterable by agent or message type.
- **Communication Flow** — Interactive force-directed graph visualizing agent-to-agent message patterns.
- **Task Board** — Track task lifecycle (pending, assigned, running, completed, failed) across the swarm.
- **System Logs** — Aggregated log viewer with level filtering (INFO, WARN, ERROR).
- **Live Updates** — WebSocket streaming pushes all MQTT events to the browser in real time.
- **Command Dispatch** — Send commands to individual agents or broadcast to the entire swarm from the dashboard.
- **Health Monitoring** — Background loop checks heartbeat freshness every 15s, marks agents offline after 60s timeout.

## Architecture

```
  Agents ──MQTT──> Mosquitto Broker
                        │
                   EdgeCitadel Backend
                   ├── subscribes to agents/#
                   ├── persists to SQLite
                   └── pushes via WebSocket
                        │
                   Nginx (frontend)
                        │
                   React Dashboard
```

All inter-agent communication flows through MQTT pub/sub. The backend subscribes to `agents/#`, stores everything in SQLite, and forwards events over WebSocket to the React frontend.

## Tech Stack

| Layer        | Technology                                      |
| ------------ | ----------------------------------------------- |
| Backend      | Python 3.12, FastAPI, aiomqtt, SQLAlchemy async |
| Frontend     | React 18, Vite 5, Tailwind CSS, Zustand        |
| Broker       | Eclipse Mosquitto 2                             |
| Database     | SQLite (via aiosqlite)                          |
| Proxy        | Nginx                                           |
| Orchestration| Docker Compose                                  |

## Installation

### Prerequisites

- Docker and Docker Compose
- Git

### Setup

```bash
git clone <repo-url> EdgeCitadel
cd EdgeCitadel
```

Create a `.env` file (or use the provided defaults):

```bash
MQTT_HOST=mqtt
MQTT_PORT=1883
MQTT_USER=iot_agent
MQTT_PASS=openclaw_secret
DATABASE_URL=sqlite+aiosqlite:///./data/openclaw.db
LOG_LEVEL=INFO
CORS_ORIGINS=http://localhost:3000
```

Start the stack:

```bash
docker compose up -d --build
```

This starts three containers:

| Service    | Port | Description           |
| ---------- | ---- | --------------------- |
| `mqtt`     | 1883 | Mosquitto MQTT broker |
| `backend`  | 8000 | FastAPI backend       |
| `frontend` | 3000 | Nginx + React app     |

Open `http://localhost:3000` in your browser.

### Adding MQTT users

The broker requires authentication. Add credentials with:

```bash
docker exec edgecitadel-mqtt-1 mosquitto_passwd -b /mosquitto/config/passwd <username> <password>
docker compose restart mqtt
```

The default backend user is `iot_agent` / `openclaw_secret`.

## Connecting an Agent

Agents connect to the MQTT broker and follow a simple topic convention to appear in the dashboard automatically.

### 1. MQTT credentials

First, create a broker user for your agent (see "Adding MQTT users" above).

### 2. Register on connect

When your agent connects, publish a registration message:

**Topic:** `agents/register/<agent_id>`

```json
{
  "agent_id": "my-agent",
  "display_name": "My Agent",
  "role": "assistant",
  "device_type": "raspberry_pi",
  "capabilities": ["chat", "vision", "sensor_reading"],
  "ip_address": "192.168.0.50",
  "status": "online"
}
```

The dashboard will create the agent entry and show it in the sidebar.

### 3. Send heartbeats

Publish a heartbeat every 30 seconds to stay online (agents are marked offline after 60s of silence):

**Topic:** `agents/heartbeat/<agent_id>`

```json
{
  "agent_id": "my-agent",
  "status": "online",
  "cpu_percent": 42.5,
  "memory_percent": 67.1,
  "ip_address": "192.168.0.50"
}
```

### 4. Topic reference

| Topic Pattern                          | Purpose                  | Direction       |
| -------------------------------------- | ------------------------ | --------------- |
| `agents/register/<agent_id>`           | Agent registration       | Agent -> Broker |
| `agents/heartbeat/<agent_id>`          | Heartbeat with metrics   | Agent -> Broker |
| `agents/status/<agent_id>`             | Status changes           | Agent -> Broker |
| `agents/inbox/<agent_id>`              | Direct messages to agent | Broker -> Agent |
| `agents/broadcast`                     | Broadcast to all agents  | Broker -> Agent |
| `agents/task/assign`                   | Task assignment          | Broker -> Agent |
| `agents/task/progress`                 | Task progress update     | Agent -> Broker |
| `agents/task/complete`                 | Task completion          | Agent -> Broker |
| `agents/task/failed`                   | Task failure             | Agent -> Broker |
| `agents/logs/<agent_id>` or `agents/log/<agent_id>` | Log entries | Agent -> Broker |

### 5. Message envelope

All payloads are JSON. The standard envelope fields are:

```json
{
  "sender": "my-agent",
  "receiver": "other-agent",
  "type": "chat",
  "correlation_id": "uuid-for-request-tracking",
  "payload": { ... },
  "timestamp": "2026-03-04T12:00:00Z"
}
```

Fields are optional — the backend infers `sender`, `receiver`, and `type` from the topic when not present in the payload.

### 6. Minimal client example (Python)

```python
import json, time
import paho.mqtt.client as mqtt

AGENT_ID = "my-agent"
BROKER = "192.168.0.102"   # EdgeCitadel host
PORT = 1883
USERNAME = "my-agent"
PASSWORD = "my-secret"

client = mqtt.Client(client_id=AGENT_ID)
client.username_pw_set(USERNAME, PASSWORD)

def on_connect(client, userdata, flags, rc, properties=None):
    # Subscribe to direct messages and broadcasts
    client.subscribe(f"agents/inbox/{AGENT_ID}", qos=1)
    client.subscribe("agents/broadcast", qos=1)

    # Register with the dashboard
    client.publish(f"agents/register/{AGENT_ID}", json.dumps({
        "agent_id": AGENT_ID,
        "display_name": "My Agent",
        "role": "worker",
        "device_type": "raspberry_pi",
        "capabilities": ["sensor_reading"],
        "status": "online",
    }))

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    print(f"[{msg.topic}] {data}")

client.on_connect = on_connect
client.on_message = on_message
client.will_set(f"agents/status/{AGENT_ID}",
                json.dumps({"status": "offline"}), qos=1, retain=True)
client.connect(BROKER, PORT)
client.loop_start()

# Heartbeat loop
while True:
    client.publish(f"agents/heartbeat/{AGENT_ID}", json.dumps({
        "agent_id": AGENT_ID,
        "status": "online",
        "cpu_percent": 25.0,
        "memory_percent": 60.0,
    }))
    time.sleep(30)
```

### 7. Minimal client example (Node.js)

```javascript
const mqtt = require("mqtt");

const AGENT_ID = "my-agent";
const client = mqtt.connect("mqtt://192.168.0.102:1883", {
  username: "my-agent",
  password: "my-secret",
  clientId: AGENT_ID,
  will: {
    topic: `agents/status/${AGENT_ID}`,
    payload: JSON.stringify({ status: "offline" }),
    qos: 1, retain: true,
  },
});

client.on("connect", () => {
  client.subscribe(`agents/inbox/${AGENT_ID}`);
  client.subscribe("agents/broadcast");

  client.publish(`agents/register/${AGENT_ID}`, JSON.stringify({
    agent_id: AGENT_ID,
    display_name: "My Agent",
    role: "worker",
    device_type: "raspberry_pi",
    capabilities: ["sensor_reading"],
    status: "online",
  }));

  // Heartbeat every 30s
  setInterval(() => {
    client.publish(`agents/heartbeat/${AGENT_ID}`, JSON.stringify({
      agent_id: AGENT_ID,
      status: "online",
      cpu_percent: 25.0,
      memory_percent: 60.0,
    }));
  }, 30000);
});

client.on("message", (topic, message) => {
  console.log(`[${topic}]`, JSON.parse(message.toString()));
});
```

## REST API

The backend exposes a REST API at `/api/`. Key endpoints:

| Method | Endpoint                    | Description              |
| ------ | --------------------------- | ------------------------ |
| GET    | `/api/health`               | Health check             |
| GET    | `/api/agents`               | List all agents          |
| GET    | `/api/agents/:id`           | Get agent details        |
| GET    | `/api/messages`             | List messages (paginated)|
| GET    | `/api/messages/flow`        | Message flow graph data  |
| GET    | `/api/tasks`                | List tasks (paginated)   |
| POST   | `/api/tasks`                | Create a task            |
| GET    | `/api/logs`                 | List logs (paginated)    |
| POST   | `/api/command/:agent`       | Send command to agent    |
| POST   | `/api/broadcast`            | Broadcast to all agents  |
| GET    | `/api/system/status`        | System overview metrics  |
| GET    | `/api/system/topology`      | Network topology data    |

## WebSocket Channels

Connect to these endpoints for real-time streaming:

| Path                  | Description                     |
| --------------------- | ------------------------------- |
| `/ws/stream`          | All events (messages, status changes, logs) |
| `/ws/agent/<agent_id>`| Events for a specific agent     |
| `/ws/logs`            | Log entries only                |

Events are JSON with an `event` field (`message`, `agent_status_change`, `agent_registered`, `log`).

## License

MIT
