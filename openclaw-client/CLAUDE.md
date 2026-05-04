# OpenClaw Client Guide

## Scope

- This directory contains the browser-side NATS client used by the operator dashboard.
- The runtime entry point is `index.js`; envelope builders and the validator live in `src/nats-session.js`.
- The legacy `mqtt-listener.js` (paho-mqtt) was removed in the v0.1 rebuild.

## Local Rules

- Keep the listener lightweight and operationally simple.
- Preserve environment-variable-based configuration unless the task explicitly changes the runtime contract.
- Avoid backend or frontend changes from this directory unless the task requires coordinated updates.
- Never connect with the fleet `NATS_TOKEN`. The browser must use the per-session `OPENCLAW_TOKEN`; see ADR-0005.
- If message subjects, payloads, or registration behavior change, update `docs/05-messaging.md` and the envelope/Card schemas in lockstep.

## Commands

- Install deps: `npm install`
- Run listener: `npm start`
- Run tests: `npm test`

## Validation

- For envelope builder or validator edits: `npm test` is sufficient (no broker required).
- For runtime wiring edits in `index.js`: prefer a broker-backed smoke check when the environment is available (full stack via `docker compose up --build -d` plus a session-token from the aggregator).
- If runtime validation is skipped because the NATS environment is unavailable, say so explicitly.
