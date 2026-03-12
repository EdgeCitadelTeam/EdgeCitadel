# API Reference

Base URL: `http://<host>/api`

## Public Endpoints (No Auth)

### Agents

| Method | Path | Description |
|---|---|---|
| `GET` | `/agents` | List all agents |
| `GET` | `/agents/{id}` | Get agent by ID |
| `GET` | `/agents/{id}/stats` | Agent message & task counts |
| `POST` | `/agents` | Create agent |
| `PATCH` | `/agents/{id}` | Update agent fields |
| `DELETE` | `/agents/{id}` | Delete agent |

**GET /agents** query params:
- `exclude_test` (bool): filter out test agents

### Messages

| Method | Path | Description |
|---|---|---|
| `GET` | `/messages` | List messages |
| `GET` | `/messages/conversations` | Grouped conversation threads |
| `GET` | `/messages/flow` | Communication topology graph |

**GET /messages** query params:
- `limit` (int, default 50)
- `offset` (int, default 0)
- `agent` (string): filter by sender or receiver
- `type` (string): filter by message_type
- `search` (string): full-text search
- `correlation_id` (string): get conversation thread
- `exclude_test` (bool): filter out test data

**GET /messages/flow** query params:
- `hours` (int, default 24)
- `exclude_test` (bool)

### Tasks

| Method | Path | Description |
|---|---|---|
| `GET` | `/tasks` | List tasks |
| `POST` | `/tasks` | Create task |
| `PATCH` | `/tasks/{id}` | Update task |
| `GET` | `/tasks/{id}/trace` | Messages linked to task |

**GET /tasks** query params:
- `limit` (int, default 200)
- `agent` (string): filter by assigned agent
- `status` (string): filter by status
- `exclude_test` (bool)

**POST /tasks** body:
```json
{
  "title": "Check sensors",
  "description": "Read all temperature sensors",
  "assigned_agent": "jeeves",
  "priority": "normal"
}
```

### Logs

| Method | Path | Description |
|---|---|---|
| `GET` | `/logs` | List log entries |

**GET /logs** query params:
- `limit` (int, default 200)
- `level` (string): INFO, WARN, ERROR, DEBUG, NATS
- `agent` or `agent_id` (string): filter by agent
- `source` (string): filter by log source
- `search` (string): full-text search
- `exclude_test` (bool)

### Commands

| Method | Path | Description |
|---|---|---|
| `POST` | `/command/{agent_name}` | Send command to agent |
| `POST` | `/broadcast` | Broadcast to all agents |

**POST /command/{agent_name}** body:
```json
{
  "message_type": "command",
  "payload": { "message": "What is the system status?" }
}
```

Auto-injects `sender_id: "dashboard"`, `receiver_id: agent_name`, and `correlation_id` if not provided. Returns `{ "ok": true, "correlation_id": "..." }`.

### System

| Method | Path | Description |
|---|---|---|
| `GET` | `/system/status` | System-wide metrics |
| `GET` | `/system/topology` | Same as /messages/flow |
| `GET` | `/health` | Health check |

**GET /system/status** response:
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

## Protected Endpoints (api-key header required)

| Method | Path | Description |
|---|---|---|
| `POST` | `/publish` | Publish raw NATS message |
| `GET` | `/history` | Raw episode history |
| `POST` | `/deployments/register` | Register deployment |
| `DELETE` | `/deployments/{name}` | Remove deployment |
| `GET` | `/deployments` | List deployments |
| `GET` | `/deployments/{name}/status` | Deployment status |

Auth header: `api-key: <OPENCLAW_API_KEY>`

**POST /publish** body:
```json
{
  "topic": "agents/jeeves/inbox",
  "payload": "{\"message\": \"hello\"}"
}
```

## WebSocket Endpoints

| Path | Description |
|---|---|
| `/ws` | Raw NATS events (topic + payload) |
| `/ws/stream` | Structured events for the frontend |
| `/ws/agent/{name}` | Agent-specific event stream |

### /ws/stream Events

```json
{"event": "message", "data": {"sender_id": "rupert", "receiver_id": "jeeves", ...}}
{"event": "agent_registered", "agent_id": "jeeves", "status": "online"}
{"event": "agent_status_change", "agent_id": "jeeves", "status": "offline"}
{"event": "task_update", "data": {"task_id": "...", "action": "complete", ...}}
{"event": "token_stream", "data": {"task_id": "...", "action": "stream", ...}}
```

Send `"ping"` as text to keep the connection alive. The frontend pings every 15 seconds.
