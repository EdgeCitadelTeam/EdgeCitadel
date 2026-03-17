---
paths:
  - "docker-compose.yml"
  - "docker-compose.*.yml"
  - "**/Dockerfile"
  - "nginx/**"
  - "nats/*.conf"
---

# Docker & Infrastructure Rules

## Docker Compose
- Services: `nats`, `aggregator`, `dashboard`, `nginx`
- Use named volumes for persistent data (JetStream, SQLite)
- Health checks on all services
- Environment variables via `.env` file (never hardcoded)
- Use `depends_on` with `condition: service_healthy` where possible

## Dockerfiles
- Multi-stage builds for production images
- Pin base image versions (e.g., `python:3.12-slim`, not `python:latest`)
- `.dockerignore` must exclude: `.git`, `node_modules`, `__pycache__`, `.env`
- Non-root user in production containers
- COPY requirements/package.json first for layer caching

## Nginx
- `/api/*` proxied to aggregator (strips `/api/` prefix)
- `/ws`, `/ws/stream`, `/ws/agent/*` proxied with WebSocket upgrade headers
- All other routes serve React SPA (try_files $uri /index.html)
- WebSocket timeout: 86400s (24 hours)

## NATS Configuration
- JetStream enabled with file storage
- MQTT adapter on port 1883 with `ack_wait: 30s`
- Token auth via `NATS_TOKEN` environment variable
- Monitoring HTTP on port 8222 (internal only, not exposed to public)
