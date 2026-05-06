# ADR-0009: Host Deploy Architecture (Phase 5)

## Status

Accepted

## Date

2026-05-04

## Context and Problem Statement

Phase 5 introduces production-shaped deployment to the central EdgeCitadel host. Three architectural choices have lasting consequences and warrant ADR-level documentation because they're hard to reverse without a re-deploy:

1. Hybrid (docker for broker + aggregator + dashboard, systemd for adapters) vs all-systemd vs all-docker.
2. Dedicated `edgecitadel` system user vs running adapters as the operator user.
3. Drop the `edgecitadel-stack.service` wrapper unit — let `docker.service` + per-container `restart=unless-stopped` handle reboot survival.

## Decision Drivers

- The docker compose stack is already production-shaped via existing restart policies; rebuilding it as systemd units would re-derive working infrastructure for no behavior change.
- Adapter services need direct host access: Ollama for CPU/GPU and on-disk model store; gemma adapter for low-latency loopback HTTP to Ollama.
- Production attack surface should be minimized — adapters running as the operator's user mean any RCE in an adapter compromises the operator's full session (ssh keys, sudo, credentials).
- Linux's `docker` group membership is root-equivalent; granting it to the adapter user would cancel the security benefit of a dedicated identity.
- The macOS variant should mirror the Linux structure so two guides aren't actually two designs.

## Considered Options

### A. Hybrid — docker for broker layer + systemd for adapters (chosen)

Docker compose continues to run NATS + aggregator + nginx + dashboard. Three new systemd units own Ollama, gemma adapter, watchdog adapter. Reboot survival: docker.service handles container restart; systemd handles unit restart. No additional wrapper unit.

### B. All-systemd

Tear out docker entirely. NATS via nats-server binary as a systemd unit, aggregator via uvicorn unit, nginx as host nginx, dashboard built statically and served by host nginx.

### C. All-docker

Containerize Ollama too (`ollama/ollama` image with model volume on host). Single systemd unit for `docker compose` itself.

## Decision Outcome

**Chosen: A. Hybrid.**

Concretely:

1. **Hybrid layout.** Docker stack untouched. Three systemd units (ollama, gemma, watchdog) added. Optional fourth unit (shell) installed but disabled by default.
2. **Dedicated `edgecitadel` system user**, no shell, no sudo, NOT in `docker` group. All adapter services run as this user with full systemd hardening (`NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `MemoryDenyWriteExecute`, `RestrictAddressFamilies`, `SystemCallFilter`).
3. **No `edgecitadel-stack.service` wrapper.** Docker handles its own services' reboot survival via `docker.service` + per-container `restart=unless-stopped`. The dedicated `edgecitadel` user stays out of `docker` group, preserving the privilege isolation.

## Consequences

### Positive

- Adapter compromise stays contained — no docker-group escalation path, no operator-session credentials accessible.
- Docker stack reboot story uses standard mechanisms; one less unit to maintain.
- macOS variant has the same structure (LaunchDaemons + dedicated `_edgecitadel` user); guides stay parallel.
- `systemd-analyze security` scores < 2.0 for adapter units (vs ~9 default = "UNSAFE").

### Negative

- Operator must remember the docker stack and adapter units have separate lifecycles: `docker compose down` vs `systemctl stop edgecitadel-*`. Documented in setup guides.
- Future "deploy script restarts the stack" UX uses `docker compose restart` directly, not a unit — slightly less uniform than "everything is a systemctl command."
- The dedicated user adds a one-time setup step (`useradd`); deploy-host.sh handles it.

### Neutral

- The architecture is reversible per-decision: collapsing back to all-docker (option C) is a future refactor that doesn't require a redesign of the broker or adapter wire contracts.

## Pros and Cons of the Options

### A. Hybrid (chosen)

- Good, because matches what already works (docker stack) and adds only what's needed (adapter units).
- Good, because adapter security isolation is full and verifiable.
- Bad, because two restart mechanisms to remember.

### B. All-systemd

- Good, because uniform `systemctl` UX.
- Bad, because re-derives working docker infrastructure.
- Bad, because aggregator's package import semantics need re-derivation for a bare-metal venv.

### C. All-docker

- Good, because tightest blast radius (everything containerized).
- Bad, because Ollama in a container loses simple host CPU/GPU passthrough on Linux (CDI works but is more setup).
- Bad, because diverges the macOS variant (where host-Ollama is the well-trodden path).

## Amendment to ADR-0004 (MQTT opt-in)

ADR-0004 stipulated that the MQTT listener on `:1883` is off by default and only enabled via `docker compose --profile mqtt-ingress up`. Phase 5 implementation (commit `4b4031c`) discovered that the original profile-based pattern was structurally broken — both the default `nats` service and the optional `nats-mqtt` service declared `4222:4222`, so `--profile mqtt-ingress` activated both and the second container failed with "port already allocated."

Phase 5 consolidates them into a single `nats` service that always exposes `1883:1883` on the host. The MQTT *listener inside the broker* is still controlled by `nats.conf` (the `mqtt {}` block, which `scripts/render-nats-conf.sh` toggles via `EC_ENABLE_MQTT`). When MQTT is disabled at the broker level, the host port is bound by docker-proxy but accepts no MQTT traffic.

This is a deliberate amendment to ADR-0004:

- **Production posture**: MQTT is required for IoT-shaped clients (e.g., the operator's standalone OpenClaw tool publishing as `us-openclaw`). Production deploy needs MQTT on; the always-bind matches that intent.
- **Test/dev posture unchanged**: developers running `docker compose up` get the same default-listening behavior; the `EC_ENABLE_MQTT=0` toggle still suppresses the in-broker listener so traffic is rejected at the application layer.
- **Trade-off acknowledged**: a tailnet scanner sees `:1883` as a TCP-open port even when no MQTT is being served. Documented in `docs/02-server-setup-linux.md` § Known trade-offs. Operators wanting to suppress the bind can revert to a profile-based or two-compose-file pattern; the current implementation prioritizes reachability simplicity.

## Links

- Phase 5 spec: `docs/superpowers/specs/2026-05-04-host-deploy-design.md`
- ADR-0004 (MQTT opt-in — amended above; deploy now consolidates `nats`/`nats-mqtt` into one always-listening service to fix a port-collision bug)
- ADR-0005 (browser-scoped token — informs why we don't add an "openclaw browser launcher")
- systemd hardening reference: `man systemd.exec`
- Tailscale ACL pattern: `https://tailscale.com/kb/1068/acl-tags`
