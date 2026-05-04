---
name: verify-infra
description: Use when verifying shared infrastructure or workflow changes (Docker compose, nginx, agent setup, root-level config) — full stack restart plus Playwright.
---

# Verify a shared/infra change

Use this when the change affects multiple subsystems, Docker wiring, nginx config, repo-shared config, or anything that touches the operator/agent workflow.

## Steps

1. Restart the full stack:
   ```bash
   docker compose down && docker compose up --build -d
   ```
   All services must come up healthy (`docker compose ps` shows running, no restart loops).

2. Smoke check the core endpoints:
   ```bash
   curl http://localhost:8222/healthz
   curl http://localhost/api/system/status
   ```
   Both must return 2xx.

3. Run at least one Playwright spec that exercises the affected workflow:
   ```bash
   cd e2e && npm test -- <relevant spec>
   ```
   For broad changes, run the full suite: `cd e2e && npm test`.

## Rules

- Curl alone is NOT sufficient for shared workflow or UI delivery changes. Playwright is the gate.
- If the docker environment is unavailable, say so explicitly — do not claim infra correctness without runtime evidence.
- If verification cannot run end-to-end, state which steps were skipped and why.
