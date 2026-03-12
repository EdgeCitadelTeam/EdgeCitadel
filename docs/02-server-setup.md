# Server Setup & Deployment

## Prerequisites

- Docker and Docker Compose
- Git

## Quick Start

```bash
git clone <repo> EdgeCitadel && cd EdgeCitadel

# Configure secrets
cp .env.example .env
# Edit .env — set OPENCLAW_API_KEY and NATS_TOKEN

# Start the stack
docker compose up -d --build

# Verify
curl http://localhost/api/health          # {"status":"ok"}
curl http://localhost:8222/healthz        # NATS health
```

The dashboard is available at `http://localhost`.

## Environment Variables

### Server (.env)

| Variable | Description | Default |
|---|---|---|
| `OPENCLAW_API_KEY` | API key for deployment endpoints | `change-me` |
| `NATS_TOKEN` | Auth token for NATS/MQTT connections | `changeme` |

### Aggregator (set in docker-compose.yml)

| Variable | Description | Default |
|---|---|---|
| `NATS_URL` | NATS server URL | `nats://nats:4222` |
| `NATS_TOKEN` | NATS auth token | from .env |
| `DB_PATH` | SQLite database path | `/data/openclaw.db` |
| `API_KEY` | API key for protected endpoints | from .env |
| `HEARTBEAT_INTERVAL` | Seconds between offline checks | `15` |
| `HEARTBEAT_TIMEOUT` | Seconds before marking agent offline | `120` |

## Ports

| Port | Service | Protocol |
|---|---|---|
| 80 | Nginx (dashboard + API) | HTTP |
| 4222 | NATS client | NATS |
| 1883 | NATS MQTT adapter | MQTT |
| 8222 | NATS monitoring | HTTP |

## NATS Configuration

The NATS server config is at `nats/nats.conf`:

```
server_name: "edgecitadel"
listen: 0.0.0.0:4222
http_port: 8222

jetstream {
    store_dir: "/data/jetstream"
    max_mem: 256MB
    max_file: 1GB
}

mqtt {
    port: 1883
    ack_wait: "30s"
    max_ack_pending: 1024
}

authorization {
    token: $NATS_TOKEN
}
```

## Persistent Data

| Path | Contents |
|---|---|
| `./data/openclaw.db` | SQLite database (agents, messages, logs, tasks) |
| `./nats/data/jetstream/` | JetStream stream and K/V storage |

## Rebuilding

```bash
# Rebuild a single service
docker compose up -d --build aggregator

# Rebuild everything
docker compose up -d --build

# Nginx may need a restart after aggregator rebuild
docker restart edgecitadel-nginx-1
```

## Monitoring

```bash
# NATS server health
curl http://localhost:8222/healthz

# NATS server info
curl http://localhost:8222/varz

# System status
curl http://localhost/api/system/status

# Container logs
docker compose logs -f aggregator
docker compose logs -f nats
```

## Remote Access via Tailscale

If using Tailscale for agent connectivity:

```bash
# Get Tailscale IP
tailscale ip -4

# Agents connect to this IP on port 1883 (MQTT)
# Dashboard accessible at http://<tailscale-ip>
```

Port 1883 must be reachable from agent machines. Tailscale handles this automatically within the tailnet.
