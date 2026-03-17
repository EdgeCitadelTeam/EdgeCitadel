---
paths:
  - "nats/**"
  - "aggregator/aggregator.py"
  - "openclaw-client/**"
---

# NATS & MQTT Messaging Rules

## Subject Naming
- NATS subjects use dots: `agents.{name}.{action}`
- MQTT topics use slashes: `agents/{name}/{action}` (auto-translated)
- Agent actions: `register`, `heartbeat`, `inbox`, `outbox`, `status`, `log`
- Task actions: `assign`, `progress`, `stream`, `complete`, `failed`
- System: `system.broadcast`

## Adding New Subjects
1. Define the subject pattern in aggregator subscription (`aggregator.py`)
2. Add corresponding handler in aggregator message parser
3. Update `docs/05-messaging.md` with subject, payload schema, and purpose
4. If agents publish to it: update `openclaw-client/mqtt-listener.js`
5. If frontend consumes it: update WebSocket broadcast in `main.py`

## JetStream
- Streams must have explicit `max_msgs`, `max_age`, and `storage` settings
- Use `FileStorage` for persistence, `MemoryStorage` only for ephemeral data
- Consumer names should be descriptive: `aggregator-messages`, not `consumer-1`

## Message Format
- All payloads are JSON-encoded UTF-8
- Include `timestamp` (ISO 8601) in every message
- Include `agent_id` or `agent_name` for attribution
- Use `correlation_id` (UUID4) for request-reply patterns
- Payload size limit: 1MB (NATS default)

## Auth
- Single `NATS_TOKEN` for both protocols
- NATS native: pass as `token` in connection options
- MQTT adapter: pass as `password` (username is ignored)
- Token loaded from environment variable, never hardcoded
