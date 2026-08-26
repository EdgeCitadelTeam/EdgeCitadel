# ADR-0005: Browser-Scoped OPENCLAW_TOKEN for openclaw-client

## Status

Accepted

## Date

2026-04-24

## Context and Problem Statement

The browser-side `openclaw-client` runs in an untrusted JavaScript runtime: a browser tab, a user-controlled Electron shell, or a kiosk that an operator can poke at. Whatever ships into that runtime is, in practice, recoverable by anyone with devtools access.

Pre-v0.1, openclaw connected to NATS using the same fleet-wide `NATS_TOKEN` that every aggregator subscriber and every backend adapter holds. That token grants publish on `agents.>` and full subscribe — i.e., the keys to the entire messaging plane. Shipping it to a browser session is unacceptable: an attacker who exfiltrates it can:

- Impersonate any agent on the cluster (`PUB agents.shell-1.outbox` with arbitrary content).
- Register fake agents that pollute the registry and consume queue capacity.
- Drain or replay `tasks.work` JetStream messages addressed to other agents.
- Subscribe to `agents.*.inbox` and observe every command across the fleet.

We need a token that is **safe to ship to a browser session** AND that **bounds what the holder can do** even when leaked.

## Decision Drivers

- The browser is not a trusted runtime; assume token exfiltration is eventual, not hypothetical.
- v0.1 is dashboard-only operator UX — the aggregator is reachable on every relevant deployment, so aggregator-mediated dispatch is acceptable.
- Per-agent JWTs with NATS account isolation are the right long-term answer but materially expand the v0.1 scope (account provisioning, JWT signing, rotation tooling). They are out of scope for v0.1.
- Direct JetStream publish from the browser is a non-starter — the publisher would need credentials capable of publishing to canonical agent subjects, which is exactly the capability we cannot ship to a browser.

## Considered Options

1. **Reuse the fleet `NATS_TOKEN` in the browser.** Operationally simplest. Catastrophic on leak.
2. **Per-session, scope-limited NATS user.** Aggregator issues a short-lived `OPENCLAW_TOKEN` bound to broker permissions that allow publish/subscribe only under a per-session subject prefix; aggregator translates those into canonical subjects. (Chosen.)
3. **Per-agent JWT with NATS account isolation.** Each agent gets its own NKey-signed JWT with permissions matching its agent identity. Right answer eventually; out of scope for v0.1.
4. **WebSocket relay (no NATS in browser at all).** Browser speaks only HTTP/WS to the aggregator; aggregator owns the NATS connection. Simpler permission model but adds a new transport surface and a new server-side session lifetime to manage. Defer until openclaw is multi-tenant.

## Decision Outcome

Chosen option: **Option 2 — per-session, scope-limited `OPENCLAW_TOKEN`.**

Concretely:

- **Token issuance.** The aggregator exposes `POST /api/openclaw/login` (Task 14) which accepts a session identifier and returns `{token, expires_at, agent_id}`. TTL is approximately one hour; refresh is a re-login via the same endpoint.
- **Broker permissions.** `OPENCLAW_TOKEN` is configured at the NATS broker as a user-scoped credential restricted to:
  - publish: `openclaw.{session_id}.>` only.
  - subscribe: `openclaw.{session_id}.results.>` only.
  - **No** publish to `agents.*.>`, `tasks.*.>`, or `system.*.>` directly.
- **Subject translation.** The aggregator subscribes `openclaw.*.>` and translates each browser-published envelope into a canonical `agents.{recipient_id}.inbox` JetStream publish. The aggregator sets `sender_id = openclaw-{session_id}` server-side; the browser cannot spoof a `sender_id` outside its session prefix because it cannot publish on `agents.*` subjects at all.
- **Results path.** Aggregator mirrors task results back onto `openclaw.{session_id}.results.{task_id}` plain NATS. The browser subscribes that prefix to display results live.

## Consequences

### Positive

- **Bounded blast radius on token leak.** A stolen `OPENCLAW_TOKEN` lets the attacker impersonate one session ID for at most ~1 hour, restricted to one subject prefix. They cannot register fake agents, cannot drain the work queue, cannot subscribe to other operators' inboxes.
- **Single point of validation.** The aggregator validates `recipient_id`, applies rate limits, and rejects malformed envelopes before any canonical subject sees the publish. Defense-in-depth on top of the broker permission grant.
- **No fleet credentials in the browser.** The fleet `NATS_TOKEN` never leaves the server side; rotating it does not require redeploying the browser bundle.

### Negative

- **Aggregator becomes a hard dependency for browser-originated dispatch.** If the aggregator is down, the browser cannot send commands. v0.1 acceptable: openclaw is dashboard-only operator UX and the aggregator is on every relevant path anyway.
- **Results path is plain NATS, no persistence.** `openclaw.{session_id}.results.*` is not a JetStream stream. If the browser disconnects between publish and result, the result is lost. Acceptable for live operator UX in v0.1; tasks themselves are durable on `tasks.work` and reachable via the aggregator HTTP API.
- **Per-session lifecycle is now aggregator state.** The aggregator must track active sessions, issue and expire tokens, and clean up when sessions disconnect. Bounded surface area but it is new state.

### Neutral

- The decision binds openclaw to aggregator-mediated dispatch for v0.1. When per-agent JWTs land in v0.2, openclaw can switch to direct JetStream publish under its own scoped credential without changing the envelope contract.

## Pros and Cons of the Options

### 1. Reuse fleet `NATS_TOKEN`

- Good, because no new endpoint or token-issuance code.
- Bad, because token leak compromises the entire messaging plane. Non-starter.

### 2. Per-session scope-limited `OPENCLAW_TOKEN` (chosen)

- Good, because blast radius is bounded by broker permission grant, then again by aggregator validation.
- Good, because it does not require account-isolation tooling or per-agent JWT signing today.
- Bad, because aggregator-mediated dispatch adds one hop and makes the aggregator a hard dependency for browser-originated commands.

### 3. Per-agent JWT with NATS account isolation

- Good, because every agent's identity is cryptographically distinct and broker-enforced.
- Bad, because v0.2-scope: requires account provisioning, JWT signing keys, rotation tooling, and a new operator workflow. Premature for v0.1.

### 4. WebSocket relay (no NATS in browser)

- Good, because the browser never holds a NATS credential at all.
- Bad, because it duplicates aggregator messaging behind a new HTTP/WS API surface; the per-session subject prefix in option 2 already gives us most of the isolation benefit at lower cost.

## Links

- [docs/agent-contract.md](../agent-contract.md) — envelope and subject contract that openclaw publishes against.
- [openclaw-client/index.js](../../openclaw-client/index.js) — runtime wiring for the scoped token.
- [openclaw-client/src/nats-session.js](../../openclaw-client/src/nats-session.js) — envelope builders and validator.
- [ADR-0001: NATS over MQTT broker](0001-nats-over-mqtt-broker.md)
- [ADR-0003: A2A v1.0 vocabulary adoption](0003-a2a-v1-vocabulary-adoption.md)
