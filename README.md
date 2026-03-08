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

## Quick Start

### 1. Start the server

```bash
git clone https://github.com/zhonghaozhan/EdgeCitadel.git
cd EdgeCitadel
cp .env.example .env          # edit to set OPENCLAW_API_KEY
mkdir -p data
docker compose up --build -d
```

Open `http://localhost` -- the dashboard is live.

### 2. Add an agent (on the server)

```bash
./add-agent.sh my-agent-name
```

This creates MQTT credentials and prints a join command.

### 3. Join from the agent's machine

```bash
git clone https://github.com/zhonghaozhan/EdgeCitadel.git
cd EdgeCitadel
./join.sh <server-ip> <mqtt-password>
```

That's it. The agent auto-detects its hostname, device type, and local OpenClaw gateway. It appears on the dashboard within seconds.

## What happens

- `add-agent.sh` creates an MQTT user on the Mosquitto broker
- `join.sh` installs a persistent MQTT listener as a systemd service that:
  - Publishes heartbeats (CPU, memory, status) every 30s
  - Subscribes to `agents/inbox/{agent-id}` for incoming commands
  - Wakes the local OpenClaw gateway when a command arrives
  - Sends the gateway's response back via MQTT
  - Auto-restarts on crash or reboot

## Services

| Service      | Port | Description                       |
| ------------ | ---- | --------------------------------- |
| `mqtt`       | 1883 | Mosquitto MQTT broker             |
| `aggregator` | 8000 | FastAPI aggregator (internal)     |
| `dashboard`  | 80   | React Swarm Control UI (internal) |
| `nginx`      | 80   | Reverse proxy (public)            |

## Network requirements

| Path                    | Port | Purpose                     |
| ----------------------- | ---- | --------------------------- |
| Browser -> EdgeCitadel  | 80   | Dashboard + API + WebSocket |
| Agents -> Mosquitto     | 1883 | MQTT publish/subscribe      |

## Managing agents

```bash
# On the agent machine:
journalctl --user -u edgecitadel-my-agent.service -f   # logs
systemctl --user restart edgecitadel-my-agent.service   # restart
systemctl --user stop edgecitadel-my-agent.service      # stop
```

## Project Structure

```
EdgeCitadel/
├── add-agent.sh             # Server: create MQTT user for a new agent
├── join.sh                  # Client: auto-setup and join EdgeCitadel
├── aggregator/              # Python FastAPI + paho-mqtt aggregator
├── frontend/                # React Swarm Control dashboard
├── mosquitto/config/        # Mosquitto broker config + passwd
├── nginx/                   # Reverse proxy config
├── openclaw-client/         # Generated listener + config (gitignored)
├── e2e/                     # Playwright E2E tests + demo recorder
├── docker-compose.yml
└── .env.example
```

## License

MIT
