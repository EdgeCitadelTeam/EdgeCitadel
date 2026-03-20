# OpenClaw Client Guide

## Scope

- This directory contains the MQTT-based agent listener and registration helpers.
- The main runtime file is `mqtt-listener.js`.

## Local Rules

- Keep the listener lightweight and operationally simple.
- Preserve environment-variable-based configuration unless the task explicitly changes the runtime contract.
- Avoid backend or frontend changes from this directory unless the task requires coordinated updates.
- If message subjects, payloads, or registration behavior change, verify compatibility expectations against the backend docs and code.

## Commands

- Install deps: `npm install`
- Run listener: `npm start`

## Validation

- For JavaScript edits here, run the relevant script if the required broker environment is available.
- For protocol or registration changes, prefer a broker-backed smoke check when the environment is available.
- If runtime validation is skipped because the MQTT/NATS environment is unavailable, say so.
