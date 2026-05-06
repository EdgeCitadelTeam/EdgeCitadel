# Agent Registration

## Overview

Agents join the EdgeCitadel network by connecting to the NATS server's MQTT port (1883) and publishing a registration message. The `join.sh` script automates this process.

## Quick Start

On the agent's machine:

```bash
# Clone the repo (or copy the openclaw-client/ directory)
git clone <repo> EdgeCitadel && cd EdgeCitadel

# Join the network
./join.sh <server-host> <nats-token> [agent-id]
```

Example:
```bash
./join.sh 100.97.29.74 edgecitadel-nats-secret-2026 us-openclaw
```

## What join.sh Does

1. **Auto-detects** agent identity:
   - Agent ID: from argument, or hostname (lowercased, sanitized)
   - Display name: title-cased agent ID (e.g., `us-openclaw` → `Us Openclaw`)
   - Device type: raspberry_pi, laptop, smartphone, cloud_vm, or server
   - Role: from `~/.openclaw/config.json` or default "Agent"

2. **Checks prerequisites**:
   - Node.js >= 16
   - `openclaw` CLI installed with model auth configured
   - `mqtt` npm package

3. **Tests MQTT connection** to `<server-host>:1883` with the provided token

4. **Writes config** to `openclaw-client/agent.env`:
   ```
   AGENT_ID=us-openclaw
   AGENT_DISPLAY=Us Openclaw
   AGENT_ROLE=Agent
   AGENT_DEVICE_TYPE=server
   CITADEL_HOST=100.97.29.74
   CITADEL_PORT=1883
   NATS_TOKEN=edgecitadel-nats-secret-2026
   OPENCLAW_BIN=/usr/bin/openclaw
   AGENT_TIMEOUT=600
   HEARTBEAT_SEC=30
   ```

5. **Installs systemd service** at `~/.config/systemd/user/edgecitadel-{agent-id}.service`

6. **Starts the service** — agent appears on the dashboard within seconds

## Server-Side Helper

On the server, `add-agent.sh` prints the join command for a new agent:

```bash
./add-agent.sh <agent-id>
# Output: ./join.sh <detected-server-ip> <nats-token-from-.env> <agent-id>
```

## What the Agent Does Once Registered

The `mqtt-listener.js` process:

1. **Connects** to MQTT port 1883 with NATS_TOKEN as password
2. **Publishes registration** to `agents/{id}/register` (retained message)
3. **Sends heartbeats** every 30 seconds with CPU%, memory%, IP
4. **Subscribes** to `agents/{id}/inbox` for commands and `system/broadcast`
5. **Processes commands** by calling the `openclaw agent` CLI (LLM)
6. **Publishes replies** to `agents/{id}/outbox` and sender's inbox
7. **Supports P2P delegation** to other agents (see [P2P Delegation](06-p2p-delegation.md))

## Registration Message Format

```json
{
  "agent_id": "jeeves",
  "sender_id": "jeeves",
  "display_name": "Jeeves",
  "role": "IoT Controller",
  "device_type": "raspberry_pi",
  "capabilities": ["sensors", "actuators", "messaging", "home_automation"],
  "ip_address": "192.168.1.20",
  "status": "online",
  "timestamp": "2026-03-12T10:00:00.000Z"
}
```

## Heartbeat Message Format

```json
{
  "agent_id": "jeeves",
  "sender_id": "jeeves",
  "display_name": "Jeeves",
  "role": "IoT Controller",
  "device_type": "raspberry_pi",
  "status": "online",
  "cpu_percent": 12.1,
  "memory_percent": 28.7,
  "ip_address": "192.168.1.20",
  "capabilities": ["chat", "task_execution", "mqtt_listener", "delegation"],
  "timestamp": "2026-03-12T10:00:30.000Z"
}
```

## Managing the Agent Service

```bash
# View logs
journalctl --user -u edgecitadel-us-openclaw -f

# Restart
systemctl --user restart edgecitadel-us-openclaw

# Stop
systemctl --user stop edgecitadel-us-openclaw

# Re-register (after code update)
git pull && ./join.sh <server-host> <nats-token> <agent-id>
```

## Agent Configuration

All agent settings are in `openclaw-client/agent.env`:

| Variable | Description | Default |
|---|---|---|
| `AGENT_ID` | Unique agent identifier | hostname |
| `AGENT_DISPLAY` | Display name on dashboard | title-cased ID |
| `AGENT_ROLE` | Agent role description | `Agent` |
| `AGENT_DEVICE_TYPE` | Device type | auto-detected |
| `CITADEL_HOST` | Server IP/hostname | — |
| `CITADEL_PORT` | MQTT port | `1883` |
| `NATS_TOKEN` | Auth token | — |
| `OPENCLAW_BIN` | Path to openclaw CLI | `openclaw` |
| `AGENT_TIMEOUT` | LLM call timeout (seconds) | `600` |
| `HEARTBEAT_SEC` | Heartbeat interval | `30` |
| `MAX_DELEGATION_DEPTH` | Max P2P delegation rounds | `3` |
| `DELEGATION_TIMEOUT` | Delegation reply timeout (seconds) | `90` |
| `ROSTER_REFRESH_SEC` | Agent roster refresh interval | `60` |

## Local adapter onboarding

For Python adapters running directly on a host (Gemma, Watchdog, Hermes, AG2 orchestrator), the onboarding pattern is three steps:

### Step 1 — Install the upstream (if any)

For native adapters (own model invocation): install Ollama, pull the target model, etc.

For bridge adapters (front a third-party agent product): install the upstream, configure it, start it on its expected port. Examples:
- Hermes: `hermes serve --port 8642`
- Future Claude Code bridge: install Claude Code, configure agent mode, expose its API endpoint.

### Step 2 — Get NATS broker host + token

On the aggregator host (or any machine with the repo and a populated `.env`):

```bash
./add-agent.sh <agent-id>
```

The script prints the broker IP, the `NATS_TOKEN`, and a `./join.sh ...` line. Browser-style agents (openclaw-client) use the `join.sh` line; Python adapter onboarders use only the broker + token from the printed banner.

### Step 3 — Configure and start the adapter

On the host:

```bash
cd /path/to/edge-research
cp adapters/<name>/agent.env.example adapters/<name>/agent.env
# Edit adapters/<name>/agent.env to fill in NATS_URL, NATS_TOKEN, plus any
# adapter-specific values (HERMES_TOKEN for hermes, etc.)
pip install -r adapters/<name>/requirements.txt
python -m adapters.<name>.adapter
```

For production (auto-start, restart on crash), use a launchd plist on Mac (see `scripts/launchd/`) or a systemd unit on Linux.

### Per-adapter quickstart docs

Each adapter ships a `README.md` with adapter-specific flags, environment, and verification steps:
- Gemma: `adapters/gemma/README.md`
- Hermes: `adapters/hermes/README.md`
- Watchdog: `adapters/watchdog/README.md`
