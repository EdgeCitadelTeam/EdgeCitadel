# ADR-0004: MQTT Ingress is Deploy-Time Opt-In

## Status

Accepted

## Date

2026-04-24

## Context and Problem Statement

Pre-v0.1, the EdgeCitadel dev fleet used MQTT 3.1.1 as the primary transport for the openclaw browser client. This worked for paho-mqtt-style clients but coupled the deploy to:

1. **NATS's MQTT adapter**, which is MQTT 3.1.1-only, with known correctness issues (notably [nats-server #5282](https://github.com/nats-io/nats-server/issues/5282)) and a stalled MQTT 5.0 upstream effort.
2. **A separate auth surface** — every MQTT-speaking client needed credentials shaped for the adapter, distinct from the NATS-native token used by the rest of the fleet.

Tasks 1-14 of the v0.1 messaging rebuild moved every internal client (aggregator, adapters, openclaw browser) onto a NATS-native transport. The remaining theoretical use case for MQTT is constrained IoT sensors that physically cannot speak NATS — none are deployed in v0.1, but the option to onboard them later should not be foreclosed.

The question for v0.1 is whether the MQTT adapter remains exposed in the default deploy.

## Decision Drivers

- **Smaller default attack surface.** Each exposed port is a separate listener with its own auth path. Default-on MQTT means every operator pays for an option most won't use.
- **Cleaner internal fleet.** With every internal client on native NATS, MQTT becomes load-bearing for nothing inside the fleet. Keeping it default-on encourages new code to assume both transports work.
- **Migration story.** Pre-v0.1 deployments using MQTT need a deliberate signal that they are now using a non-default deploy mode, rather than silently rolling forward and getting the same shape as before.
- **Future MQTT 5.0.** When MQTT 5.0 sensor onboarding becomes a real ask, the right answer is likely an EMQX sidecar with its NATS Gateway, not waiting on NATS's stale MQTT 5.0 work. The toggle leaves room for either path.

## Considered Options

1. **Keep MQTT default-on.** No template, no toggle, no profile. Port 1883 is exposed by every `docker compose up`.
2. **Drop MQTT entirely.** Remove the `mqtt {}` block from `nats.conf`, remove port 1883 from `docker-compose.yml`. No path to onboard sensors short of re-introducing the block.
3. **Run a parallel MQTT broker.** Add a separate MQTT broker (Mosquitto or EMQX) alongside NATS, with its own auth and bridging. Always available, distinct ops surface.
4. **Template + deploy-time toggle (chosen).** `nats/nats.conf` is rendered from `nats/nats.conf.tpl` by `scripts/render-nats-conf.sh`. The MQTT block is commented in the template and uncommented only when `EC_ENABLE_MQTT=1`. A second `nats-mqtt` service in `docker-compose.yml`, gated by `profiles: ["mqtt-ingress"]`, exposes port 1883 only when explicitly requested.

## Decision Outcome

Chosen option: **Option 4 — template + deploy-time toggle.**

Concretely:

- **Template.** `nats/nats.conf.tpl` is the source of truth. The MQTT block sits between `# MQTT_BEGIN` / `# MQTT_END` markers, fully commented in the template.
- **Renderer.** `scripts/render-nats-conf.sh` rewrites `nats/nats.conf`. Default behavior is `cp tpl conf` (MQTT remains commented). When `EC_ENABLE_MQTT=1` the script uncomments the block before writing.
- **Default deploy.** `docker compose up -d` runs the `nats` service with ports `4222` (NATS client) and `8222` (NATS HTTP monitoring) exposed. **Port 1883 is never exposed by the default profile.**
- **Opt-in deploy.** Operators who need MQTT ingress run:
  ```sh
  EC_ENABLE_MQTT=1 scripts/render-nats-conf.sh
  docker compose --profile mqtt-ingress up -d
  ```
  This activates the `nats-mqtt` service (which exposes port 1883) instead of the default `nats` service. The rendered config has the MQTT block live; the profile flag selects the variant of the broker container that publishes the port.
- **Auth.** The template adds an `openclaw` user account permitted to publish `openclaw.*.>` and subscribe `openclaw.*.results.>`. v0.1 relies on the aggregator translating `openclaw.{session}.command.{target}` → canonical JetStream subjects; per-session isolation is a v0.2 hardening (per-session JWTs).

### Consequences

#### Positive

- **One fewer exposed port by default.** The default deploy advertises only NATS-native traffic.
- **Internal fleet is MQTT-free.** No code paths in adapters, aggregator, or browser client depend on the adapter being present.
- **Explicit operator intent.** Anyone enabling MQTT ingress signs a paper trail: setting an env var, running a script, choosing a compose profile.
- **Future MQTT 5.0 path is unblocked.** If sensor onboarding ever needs MQTT 5.0, an EMQX sidecar with its NATS Gateway can be added without disturbing the default deploy.

#### Negative

- **Pre-v0.1 deployments using MQTT must opt-in explicitly when migrating.** They will not silently keep working with the same `docker compose up` invocation.
- **Two deploy paths to test.** The default deploy and the `mqtt-ingress` profile diverge enough that integration tests should cover both when MQTT is in scope.

#### Neutral

- The `openclaw` user permissions in the template do not by themselves enforce per-session isolation; the aggregator's subject translator is the v0.1 isolation point. v0.2 will tighten this with per-session JWTs.

## Pros and Cons of the Options

### 1. Keep MQTT default-on

- Good, because pre-v0.1 deployments roll forward with no operator action.
- Bad, because it permanently expands the default attack surface and keeps the adapter as load-bearing in the default config when nothing internal needs it.

### 2. Drop MQTT entirely

- Good, because it is the smallest possible default surface.
- Bad, because IoT-sensor onboarding loses its escape hatch — re-introducing MQTT later is a config + ADR change, not a flag flip.

### 3. Parallel MQTT broker (Mosquitto / EMQX)

- Good, because MQTT becomes a first-class subsystem with its own ops story.
- Bad, because it doubles the broker count, adds a bridging surface, and is overkill for the (currently zero) v0.1 sensor population.

### 4. Template + deploy-time toggle (chosen)

- Good, because the default deploy is minimal AND the MQTT path remains a flag flip away when needed.
- Bad, because operators who genuinely want MQTT ingress now run two extra commands instead of one.

## Links

- [scripts/render-nats-conf.sh](../../scripts/render-nats-conf.sh) — renderer that drives the toggle.
- [nats/nats.conf.tpl](../../nats/nats.conf.tpl) — source template, MQTT block commented between markers.
- [docker-compose.yml](../../docker-compose.yml) — `nats` service (default) vs `nats-mqtt` service behind `profiles: ["mqtt-ingress"]`.
- [ADR-0001: NATS over MQTT broker](0001-nats-over-mqtt-broker.md)
- [ADR-0005: Browser scoped token](0005-browser-scoped-token.md)
