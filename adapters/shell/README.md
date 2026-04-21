# Shell Adapter

L1 reference implementation of the [Agent Contract](../../docs/agent-contract.md).

This is the minimum viable conformant agent. Its job is to prove L1 is actually trivial — if writing this adapter exposed gaps in the contract, the contract is wrong, not the adapter.

## What it does

1. Connects to the NATS server's MQTT port.
2. Publishes an Agent Card on `agents/{id}/register` (retained).
3. Sends heartbeats every `HEARTBEAT_SEC` seconds.
4. Subscribes to `agents/{id}/inbox` and `system/broadcast`.
5. For each inbound `command` or `delegation`, runs `$SHELL_COMMAND` with the message body on stdin and replies with stdout as the result body. If `SHELL_COMMAND` is unset, echoes the body back.
6. Validates every outbound envelope and Agent Card against the JSON Schemas in [`schemas/`](../../schemas). Fails loud on the producer.
7. On SIGINT/SIGTERM, publishes `{status: offline}` and exits cleanly. On ungraceful drop, the MQTT Last Will does it for us.

## Run it

```bash
cd adapters/shell
pip install -r requirements.txt

AGENT_ID=echo-1 \
CITADEL_HOST=127.0.0.1 \
NATS_TOKEN=edgecitadel-nats-secret-2026 \
python shell_adapter.py
```

With a real command:

```bash
AGENT_ID=sensor-reader \
AGENT_TAGS=indoor,low-latency \
SHELL_COMMAND='jq -r ".temperature // \"unknown\""' \
CITADEL_HOST=100.97.29.74 \
NATS_TOKEN=... \
python shell_adapter.py
```

## Config

| Env var | Default | Purpose |
|---|---|---|
| `AGENT_ID` | *(required)* | Lowercase, matches `^[a-z0-9][a-z0-9_-]{0,63}$`. |
| `AGENT_DISPLAY` | title-cased ID | Shown in the dashboard. |
| `AGENT_ROLE` | `Shell Worker` | Free-form string. |
| `AGENT_DEVICE_TYPE` | `server` | One of the enum values in the Card schema. |
| `AGENT_TAGS` | *empty* | Comma-separated tags for planner selection. |
| `CITADEL_HOST` | `127.0.0.1` | NATS server host. |
| `CITADEL_PORT` | `1883` | MQTT adapter port. |
| `NATS_TOKEN` | *(required)* | Shared token. Used as MQTT password. |
| `HEARTBEAT_SEC` | `30` | Heartbeat interval. Declared in the Card. |
| `SHELL_COMMAND` | *empty* | Command to run on each inbound command. Empty → echo mode. |
| `SHELL_TIMEOUT_SEC` | `60` | Kill the shell command after this long. |
| `SCHEMA_DIR` | `../../schemas` | Where to find `envelope.v1.json` and `agent-card.v1.json`. |

## What it does NOT do (by design)

- **P2P delegation.** That's L2 — use the AG2 adapter or extend this one.
- **MCP tool serving.** That's L3.
- **Typed payload validation.** Only the envelope and Card are schema-validated in v0.1. Per-type payload schemas come in v0.2.
- **Retained Card verification of inbound `sender_id`.** §1.5 of the contract requires this; this adapter currently logs but doesn't enforce. The aggregator is the intended central enforcement point.

## Why it's a test of the contract

If this adapter grew past ~300 lines, or if any of its code had to invent behavior the contract doesn't specify, the contract is under-specified. Treat any growth as a spec bug first.
