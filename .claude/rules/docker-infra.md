---
paths:
  - "docker-compose.yml"
  - "docker-compose.*.yml"
  - "**/Dockerfile"
  - "nginx/**"
  - "nats/*.conf*"
  - "scripts/render-nats-conf.sh"
---

# Docker & Infrastructure Rules (v0.1+)

> Authoritative: `docker-compose.yml`, ADR-0004 (MQTT opt-in),
> `aggregator/Dockerfile`, `nginx/default.conf`,
> `scripts/render-nats-conf.sh`, `nats/nats.conf.tpl`.

## Docker Compose

- Default services: `nats`, `aggregator`, `dashboard`, `nginx`.
- Optional `nats-mqtt` service (`profiles: [mqtt-ingress]`) — only
  activated by `docker compose --profile mqtt-ingress up`. Default
  brings up the four core services with MQTT off and port 1883 not
  exposed (per ADR-0004).
- Use named volumes / bind mounts for persistent data:
  `./data:/data` (SQLite), `./nats/data:/data` (JetStream).
- Health checks on every service.
- Environment variables via `.env` (never hardcode secrets).
- `depends_on` with `condition: service_healthy` where supported.
- `aggregator` has `stop_grace_period: 40s` so JetStream consumers
  drain cleanly on shutdown.

## Aggregator Dockerfile

Build context is the repo root (NOT `./aggregator`). The image copies
both `aggregator/` and `schemas/` so:

```
FROM python:3.12-slim
WORKDIR /app
COPY aggregator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY aggregator/ ./aggregator/
COPY schemas/    ./schemas/
CMD ["uvicorn", "aggregator.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`aggregator/__init__.py` MUST exist so `aggregator.main` resolves as a
package and relative imports (`from . import database`) work.

## Dockerfiles in general

- Pin base image versions (e.g. `python:3.12-slim`, never `python:latest`).
- `.dockerignore` must exclude `.git`, `node_modules`, `__pycache__`, `.env`,
  `data/`, `nats/data/`.
- Production images: non-root user (deferred; current images run as root —
  acceptable for local dev, revisit for Mac Mini deploy in Phase 5).
- Copy `requirements.txt` / `package.json` first for layer caching.

## Nginx

- `proxy_pass http://aggregator:8000;` (NO trailing slash) — preserves the
  `/api/` prefix so FastAPI routes registered as `/api/...` resolve.
- `/api/*` proxied to aggregator.
- `/ws`, `/ws/stream`, `/ws/agent/*` reserved for future WebSocket bridge
  (Phase 1 follow-up — endpoints not yet shipped on the aggregator).
  Upgrade headers and 86400s read timeout already configured in
  `nginx/default.conf`.
- All other routes serve the React SPA via the dashboard service.

## NATS configuration

- `nats/nats.conf.tpl` is the source template; `scripts/render-nats-conf.sh`
  outputs `nats/nats.conf` (the file the container actually mounts).
  Re-render after toggling `EC_ENABLE_MQTT`.
- JetStream enabled with file storage at `/data/jetstream`,
  `max_mem: 256MB`, `max_file: 1GB`.
- Authorization is **token-only** in v0.1 (`token: $NATS_TOKEN`).
  Multi-user / per-session JWTs are deferred to v0.2 (per ADR-0005).
  Do NOT add `users: [...]` to `authorization {}` while keeping `token:`
  — NATS rejects the combination ("Can not have a token and a users
  array").
- Monitoring HTTP on port 8222 (exposed for healthchecks; safe to keep
  internal only on production deploys).

## Ports

| Port | Service | Profile |
|---|---|---|
| 80 | nginx (dashboard + /api/* + /ws/*) | default |
| 4222 | NATS clients | default |
| 8222 | NATS HTTP monitoring | default |
| 1883 | NATS MQTT ingress | `mqtt-ingress` only |

Internal-only (not exposed to host):
- aggregator HTTP 8000
- dashboard nginx 80
