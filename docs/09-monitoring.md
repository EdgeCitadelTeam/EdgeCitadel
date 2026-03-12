# Monitoring & Observability

## System Status

The header bar shows real-time system metrics:

- **Agents online / total**: e.g., `3/5 online`
- **Total messages**: cumulative message count
- **Active tasks**: pending + assigned + running tasks
- **Errors today**: ERROR-level log count for today
- **Connection badge**: WebSocket connection status

Query programmatically:

```bash
curl http://localhost/api/system/status
```

```json
{
  "agents_online": 3,
  "agents_total": 5,
  "total_messages": 142,
  "active_tasks": 2,
  "errors_today": 0,
  "nats_connected": true,
  "mqtt_connected": true
}
```

## Agent Health Monitoring

### Heartbeats

Agents send heartbeats every 30 seconds (configurable) containing:
- CPU percentage
- Memory percentage
- IP address
- Status
- Capabilities

### Offline Detection

A background task in the aggregator runs every `HEARTBEAT_INTERVAL` seconds (default: 15) and checks:

- If `last_heartbeat` is older than `HEARTBEAT_TIMEOUT` seconds (default: 120), the agent is marked **offline**
- An `agent_status_change` event is broadcast via WebSocket
- The dashboard sidebar updates the agent's status badge

### Agent Status Flow

```
Register/Heartbeat → online (green badge)
                        │
    120s no heartbeat → offline (gray badge)
                        │
    Next heartbeat   → online (green badge)
```

## NATS Server Monitoring

NATS exposes HTTP monitoring on port 8222:

```bash
# Health check
curl http://localhost:8222/healthz

# Server info (connections, memory, etc.)
curl http://localhost:8222/varz

# Active connections
curl http://localhost:8222/connz

# JetStream info
curl http://localhost:8222/jsz

# Subscription info
curl http://localhost:8222/subsz
```

## Log Viewer

The dashboard Logs tab (keyboard shortcut `3`) shows all log entries with filters:

- **Level**: INFO, WARN, ERROR, DEBUG, NATS
- **Source**: filter by log source
- **Search**: full-text search
- **Agent**: filter by agent (via sidebar selection)

ERROR-level logs trigger a red toast notification on the dashboard.

Query logs via API:

```bash
# All logs
curl http://localhost/api/logs?limit=100

# Filter by level
curl http://localhost/api/logs?level=ERROR

# Filter by agent
curl http://localhost/api/logs?agent=jeeves

# Search
curl http://localhost/api/logs?search=sensor
```

## Container Logs

```bash
# Aggregator logs
docker compose logs -f aggregator

# NATS server logs
docker compose logs -f nats

# All services
docker compose logs -f
```

## Agent Service Logs

On the agent's machine:

```bash
# Follow logs
journalctl --user -u edgecitadel-{agent-id} -f

# Last 50 lines
journalctl --user -u edgecitadel-{agent-id} -n 50

# Since last hour
journalctl --user -u edgecitadel-{agent-id} --since "1 hour ago"
```

## Communication Flow Graph

The Flow tab (keyboard shortcut `2`) visualizes agent communication topology:

- Nodes represent agents
- Edges represent message flows (direction + volume)
- Central NATS hub shows the broker
- Time range: 1h, 6h, 24h, 7d

Query the topology data:

```bash
curl http://localhost/api/system/topology?hours=24
```
