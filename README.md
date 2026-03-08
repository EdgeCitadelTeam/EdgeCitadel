# Edge Citadel

Central aggregation hub for multiple remote [OpenClaw](https://github.com/openclaw) deployments. Subscribes to MQTT brokers across all your OpenClaw instances simultaneously and presents a single real-time dashboard. New deployments register themselves with a single command.

## Demo

![EdgeCitadel Dashboard Demo](docs/demo.gif)

> Multi-deployment agent monitoring with real-time MQTT aggregation. ([Full video](docs/demo.mp4))

## Features

- **Multi-Deployment Aggregation** -- Connects to MQTT brokers on multiple remote OpenClaw instances simultaneously
- **Real-Time Dashboard** -- WebSocket-driven feed of all MQTT messages across every deployment
- **Agent Graph** -- SVG visualization of deployments and their agents, with flash-on-message animation
- **Message Composer** -- Publish messages to any connected deployment directly from the dashboard
- **Deployment Registry** -- REST API for registering/deregistering OpenClaw deployments
- **Message Persistence** -- All MQTT episodes stored in SQLite for history queries
- **Client Script** -- One-command registration from any OpenClaw box via `register.sh`

## Architecture

```
                        ┌─────────────────────────┐
                        │  Mosquitto MQTT Broker   │
                        │      (port 1883)         │
                        └────────┬────────────────┘
                                 │
                     ┌───────────┼───────────┐
                     │           │           │
                  Rupert      Jeeves      Percy
               (Orchestrator) (IoT)     (Mobile)
                     │           │           │
                     └───────────┼───────────┘
                                 │
                          paho-mqtt subscribe
                                 │
                        ┌────────┴────────────┐
                        │     Aggregator      │
                        │  (FastAPI + SQLite)  │
                        └────────┬────────────┘
                                 │
                            WebSocket
                                 │
                        ┌────────┴────────────┐
                        │   Nginx (reverse    │
                        │      proxy)         │
                        └────────┬────────────┘
                                 │
                        ┌────────┴────────────┐
                        │   React Dashboard   │
                        └─────────────────────┘
```

Edge Citadel runs a Mosquitto MQTT broker. Agents (Rupert/Orchestrator, Jeeves/IoT, Percy/Mobile) connect to the broker and communicate via MQTT topics. The aggregator subscribes to all traffic, stores it in SQLite, and streams it to the React dashboard via WebSocket.

## Tech Stack

| Layer         | Technology                                        |
| ------------- | ------------------------------------------------- |
| MQTT Broker   | Eclipse Mosquitto 2                               |
| Aggregator    | Python 3.12, FastAPI, paho-mqtt 1.6.1              |
| Dashboard     | React 18, Vite 5, Tailwind CSS, Zustand, recharts |
| Database      | SQLite                                             |
| Proxy         | Nginx                                              |
| Orchestration | Docker Compose                                     |

## Installation

### Prerequisites

- Docker and Docker Compose
- Git

### 1. Clone and configure

```bash
git clone <repo-url> EdgeCitadel
cd EdgeCitadel
cp .env.example .env
```

Edit `.env` and set a strong API key:

```
OPENCLAW_API_KEY=your-secret-key-here
```

### 2. Start the stack

```bash
mkdir -p data
docker compose up --build -d
```

This starts four services:

| Service      | Port | Description                          |
| ------------ | ---- | ------------------------------------ |
| `mqtt`       | 1883 | Mosquitto MQTT broker                |
| `aggregator` | 8000 | FastAPI aggregator (internal)        |
| `dashboard`  | 80   | React Swarm Control UI (internal)    |
| `nginx`      | 80   | Reverse proxy (public)               |

### 3. Verify

```bash
# Check all services are running
docker compose ps

# Check aggregator logs
docker compose logs -f aggregator

# Test the API
curl http://localhost/api/deployments -H "api-key: your-secret-key-here"
```

Open `http://localhost` in your browser. Click "Set API Key" in the header and enter your key.

## Registering an OpenClaw Deployment

There are two ways to register a deployment: the client script (recommended) or a direct API call.

### Option A: Client script (recommended)

Copy the `openclaw-client/` folder to your OpenClaw machine:

```bash
scp -r openclaw-client/ user@openclaw-host:~/
```

On the OpenClaw machine:

```bash
cd ~/openclaw-client
cp openclaw.conf.example openclaw.conf
```

Edit `openclaw.conf`:

```bash
DEPLOYMENT_NAME="home"                       # unique name for this deployment
DEPLOYMENT_HOST="192.168.1.42"               # address Edge Citadel can reach this MQTT broker
DEPLOYMENT_PORT=1883
DEPLOYMENT_DESCRIPTION="Home lab rack"

AGGREGATORS=(
    "edge-citadel=http://your-citadel-ip"    # Edge Citadel address
)

API_KEY_EDGE_CITADEL="your-secret-key-here"  # must match OPENCLAW_API_KEY
```

Register:

```bash
chmod +x register.sh
./register.sh
```

Other commands:

```bash
./register.sh --status       # check registration status
./register.sh --list         # show configured aggregators
./register.sh --deregister   # remove from all aggregators
./register.sh --target edge-citadel  # register with one specific aggregator
```

### Option B: Direct API call

```bash
curl -X POST http://your-citadel-ip/api/deployments/register \
  -H "Content-Type: application/json" \
  -H "api-key: your-secret-key-here" \
  -d '{
    "name": "home",
    "host": "192.168.1.42",
    "port": 1883,
    "description": "Home lab rack",
    "network": "lan",
    "mqtt_user": "iot_agent",
    "mqtt_pass": "broker-password"
  }'
```

The `mqtt_user` and `mqtt_pass` fields are optional -- only needed if the MQTT broker requires authentication.

### Network requirements

| Path                          | Port | Purpose                                 |
| ----------------------------- | ---- | --------------------------------------- |
| Browser -> Edge Citadel       | 80   | Dashboard + API + WebSocket             |
| Agents -> Mosquitto           | 1883 | MQTT publish/subscribe                  |

Agents connect to the Mosquitto broker on port 1883. The aggregator subscribes to all MQTT traffic on the same broker.

## REST API

All endpoints require an `api-key` header matching `OPENCLAW_API_KEY`.

| Method | Endpoint                        | Description                        |
| ------ | ------------------------------- | ---------------------------------- |
| POST   | `/api/deployments/register`     | Register a new deployment          |
| DELETE  | `/api/deployments/{name}`      | Deregister a deployment            |
| GET    | `/api/deployments`              | List all active deployments        |
| GET    | `/api/deployments/{name}/status`| Get deployment connection status   |
| POST   | `/api/publish`                  | Publish a message to a deployment  |
| GET    | `/api/history`                  | Query stored episodes              |

### WebSocket

Connect to `/ws` for real-time streaming. No auth required (nginx restricts to same origin). Events are JSON with `deployment`, `topic`, `payload`, and `ts` fields.

## Dashboard Usage

1. **Set API Key** -- Click the key icon in the header bar and enter your `OPENCLAW_API_KEY`
2. **Deployments** -- Left panel shows registered deployments with connection status (green = connected), network badge, and message count
3. **Agent Graph** -- Right panel SVG showing deployment nodes and auto-discovered agent nodes. Nodes flash yellow on new messages
4. **Message Feed** -- Filterable real-time stream. Filter by deployment, agent, or free text. Click a row to expand the full JSON payload
5. **Publish Message** -- Select a deployment, enter topic and payload, and send directly to that broker

## Project Structure

```
EdgeCitadel/
├── aggregator/              # Python FastAPI + paho-mqtt aggregator
│   ├── main.py              # FastAPI app, REST + WebSocket endpoints
│   ├── aggregator.py        # MQTT subscriber/publisher, message parser
│   ├── database.py          # SQLite persistence (agents, messages, logs, tasks)
│   ├── models.py            # Pydantic request models
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/                # React Swarm Control dashboard
│   ├── src/
│   │   ├── App.jsx
│   │   ├── Layout.jsx
│   │   ├── stores/appStore.js
│   │   ├── hooks/useWebSocket.js
│   │   ├── api/client.js
│   │   └── components/
│   │       ├── AgentSidebar.jsx
│   │       ├── ChatHistory.jsx
│   │       ├── CommFlow.jsx
│   │       ├── LogViewer.jsx
│   │       ├── TaskBoard.jsx
│   │       ├── CommandInput.jsx
│   │       └── HeaderBar.jsx
│   ├── Dockerfile
│   └── nginx.conf
├── mosquitto/
│   └── config/mosquitto.conf
├── nginx/
│   └── default.conf         # Reverse proxy config
├── openclaw-client/
│   ├── register.sh
│   └── openclaw.conf.example
├── data/                    # SQLite database (gitignored)
├── docker-compose.yml
└── .env.example
```

## License

MIT
