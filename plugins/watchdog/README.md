# Watchdog Plugin

Watchdog is an infrastructure Plugin that observes Agent registration,
heartbeats, outboxes, and JetStream delivery advisories. It synthesizes a
bounded `recipient_offline` result for tracked work when a recipient is known
to be unavailable.

```bash
edgecitadel plugin install watchdog
```

It exposes no operator-invokable skill. The Supervisor owns its process and
NATS connection; threshold environment variables are intended only for
controlled testing. Runtime and isolated real-NATS tests live in
`plugin-toolkit/tests/watchdog_runtime/`.
