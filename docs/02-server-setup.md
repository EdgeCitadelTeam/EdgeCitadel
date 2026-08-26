# Server Setup & Deployment

EdgeCitadel runs in two distinct configurations on the same code:

- **Dev stack** — Docker Compose, all services in containers. Quick start in `01-architecture.md` § Dev Stack. For development, smoke testing, and CI.

## Choose your platform for production deploy

| Platform | Guide | Status |
|---|---|---|
| Linux (Ubuntu/Debian) | [02-server-setup-linux.md](02-server-setup-linux.md) | Validated |
| macOS (Mac Mini) | [02-server-setup-macos.md](02-server-setup-macos.md) | Forward-looking — not yet executed end-to-end |

The two guides have identical structure (11 sections in the same order). Pick by platform, follow §2's five commands, then walk down the rest.

## Cross-platform invariants

- Single source of truth for dependencies: `deploy/manifest.toml`. Edit there; do not duplicate dependency lists into prose.
- Single source of truth for secrets: `/etc/edgecitadel/env` (mode `0640`).
- Backups: local-only nightly to `/var/lib/edgecitadel/backups/`. Mirror to another host or off-host storage is a Phase 5.x follow-up; restore procedure in `deploy/backup/README.md`.
- See ADR-0012 for the dedicated-user / systemd-hardening rationale.
- See ADR-0004 for the MQTT-opt-in design intent and ADR-0012 § Amendment for why Phase 5 deploy supersedes that design (single `nats` service binds 1883 unconditionally; MQTT listener inside the broker still gated via `EC_ENABLE_MQTT` in `nats.conf.tpl`).

## Operating the dev stack

```bash
cd /path/to/EdgeCitadel
docker compose up --build -d
# Dashboard: http://localhost
# Stop: docker compose down
```

The dev stack is independent of the production deploy. You can run the dev stack on a different machine for local development while production runs on the deploy target.
