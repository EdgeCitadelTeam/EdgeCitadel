# openclaw-client v0.1

Browser-side process that connects to the EdgeCitadel NATS plane on behalf of an operator session. Publishes `register` / `heartbeat` / `status` on plain NATS using an account-scoped `OPENCLAW_TOKEN`, dispatches commands through the aggregator HTTP API, and observes task results on the per-session subject prefix `openclaw.{session_id}.results.*`.

This client is intentionally not an Agent Plugin. It represents an untrusted,
short-lived browser/operator session with a scoped token; installing it through
the host Plugin Supervisor would collapse the trust boundary described below.

This is the v0.1 clean rebuild on `@nats-io/nats-core` + `@nats-io/transport-node`. The legacy MQTT listener (`mqtt-listener.js`, paho-mqtt) is removed.

## Why a scoped token

The browser is not a trusted runtime. The fleet `NATS_TOKEN` would, if leaked, let an attacker impersonate any agent and drain queues. Instead, the aggregator issues a per-session `OPENCLAW_TOKEN` whose broker permissions are restricted to:

- publish: `openclaw.{session_id}.>` only (no `agents.*` publish).
- subscribe: `openclaw.{session_id}.results.>` only.

The aggregator subscribes `openclaw.*.>` and translates browser-published envelopes into canonical `agents.{recipient_id}.inbox` JetStream publishes, setting `sender_id` server-side. See [ADR-0005](../docs/adr/0005-browser-scoped-token.md) for the full rationale.

## Environment variables

| Variable                  | Required | Default                          | Notes                                                                |
| ------------------------- | -------- | -------------------------------- | -------------------------------------------------------------------- |
| `NATS_URL`                | no       | `nats://localhost:4222`          | NATS server URL.                                                     |
| `OPENCLAW_TOKEN`          | yes      | —                                | Per-session scoped token. **Not** the fleet `NATS_TOKEN`.            |
| `OPENCLAW_SESSION_ID`     | no       | `sess-{uuid8}` (auto)            | Session prefix; must match the prefix the broker permission grants.  |
| `OPENCLAW_AGENT_ID`       | no       | `openclaw-{OPENCLAW_SESSION_ID}` | Canonical agent id used in `sender_id`.                              |
| `HEARTBEAT_INTERVAL_SEC`  | no       | `30`                             | Plain-NATS heartbeat cadence.                                        |

`dotenv` is loaded automatically; place values in `.env` next to `index.js` if convenient. Never commit `OPENCLAW_TOKEN`.

## Token acquisition

The aggregator exposes (Task 14):

```
POST /api/openclaw/login
Content-Type: application/json

{ "session_id": "sess-1234" }
```

Returns `{ "token": "...", "expires_at": "...", "agent_id": "openclaw-sess-1234" }`. Tokens expire in roughly one hour; the operator UI re-logs in automatically before expiry.

## Run

```bash
cd openclaw-client
npm install
OPENCLAW_TOKEN=<scoped-token> npm start
```

The process publishes `agents.{agent_id}.register`, then heartbeats every `HEARTBEAT_INTERVAL_SEC` seconds, and subscribes `openclaw.{session_id}.results.*`. On `SIGINT` / `SIGTERM` it publishes a final `agents.{agent_id}.status` with `agent_state: "offline"` before draining.

## Test

```bash
cd openclaw-client
npm test
```

Tests are stand-alone (no NATS broker required). They exercise the envelope builders and the Ajv validator compiled against `schemas/envelope.v1.json`. End-to-end coverage for the full register → heartbeat → command path lives in the e2e smoke spec (Task 17).

## Layout

- `index.js` — top-level runner; opens the NATS connection, publishes register/heartbeats, subscribes results, handles shutdown.
- `src/nats-session.js` — envelope builders (`buildRegisterEnvelope`, `buildHeartbeatEnvelope`, `buildStatusEnvelope`, `buildCommandEnvelope`) and the strict envelope validator. Same canonical shape as Python Plugin runtimes.
- `tests/nats-session.test.js` — node:test cases covering register/heartbeat shape, legacy-field rejection, and command envelope acceptance.
