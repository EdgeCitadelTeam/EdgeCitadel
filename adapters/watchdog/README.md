# Watchdog Adapter

Detects offline agents in the EdgeCitadel fleet and synthesises
`recipient_offline` failures so callers don't hang.

## Identity

- `agent_id: watchdog-1`
- `runtime.kind: native`, `runtime.roles: [watchdog]`
- Single instance per fleet (durable inbox consumer enforces).
- Card source: `adapters/watchdog/config.yaml`.

## Trigger model

Three reinforcing paths share one dedup key (`Nats-Msg-Id: watchdog-syn-{task_id}`):

1. **Heartbeat-staleness fast path** — when `now - last_seen[X] > max(2 × declared_interval, 20s) + 5s`, fan out failures for every observed pending task targeted at X. ~30–65 s detection for 30 s interval agents.
2. **Sticky-offline immediate path** — once X is flagged offline, new commands to X synthesise immediately (~ms).
3. **MAX_DELIVERIES advisory backstop** — for cold-start gaps and tasks not observed via outbox, JetStream's advisory eventually fires and the watchdog synthesises.

See `docs/adr/0007-watchdog-trigger-model.md` for the full rationale.

## Running (dev)

```bash
# Stack up
docker compose up --build -d

# Watchdog (host process, like gemma)
NATS_URL=nats://localhost:4222 NATS_TOKEN=$NATS_TOKEN \
  python -m adapters.watchdog.adapter
```

Verify it registered:
```bash
curl -s http://localhost/api/agents/watchdog-1 | jq '.card.metadata."runtime.roles"'
# → ["watchdog"]
```

## Interpreting WARN log envelopes

When the watchdog synthesises a failure it publishes a WARN-level `log`
envelope on `agents.watchdog-1.log`. The dashboard's Logs tab surfaces
these. Format:

```
synthesised recipient_offline (trigger=<heartbeat_staleness|sticky_offline|max_deliveries_advisory>, offline_agent=<id>, task_id=<uuid>)
```

`trigger` distinguishes which path produced the synthesis — useful when
diagnosing why a sender saw `recipient_offline` for a particular task.

## Two-instance chaos test (manual)

The watchdog uses a durable JetStream consumer for its inbox, so two
instances can't both process unknown-command rejections. The plain-NATS
subscriptions (outbox / heartbeat / advisory) accept multiple subscribers,
which means a second instance also publishes synthesised failures — but
the `Nats-Msg-Id: watchdog-syn-{task_id}` header collapses duplicates via
JetStream's 5-min `duplicate_window`.

To verify:

```bash
# Terminal A
python -m adapters.watchdog.adapter

# Terminal B (same NATS broker)
python -m adapters.watchdog.adapter
```

Trigger a heartbeat-staleness event for a peer agent. Inspect the original
sender's inbox stream — exactly one synthesised `result` per `task_id`
should appear (the second is JetStream-deduped). This is documented but
not automated; multi-instance HA is v0.2+ work.

## v0.2 ideas (not implemented)

- Persistent state across restarts (today rebuilds from live traffic).
- `runtime.synthesise_failures: false` per-agent opt-out.
- Admin commands (`list_offline`, `dump_pending_tasks`).
- `offline_since` timestamps for the dashboard's "offline N days" view.

See `docs/superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md`
§"Non-goals (Phase 3)" and `docs/roadmap.md`.
