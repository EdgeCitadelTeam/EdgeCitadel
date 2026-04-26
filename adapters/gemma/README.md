# Gemma adapter

Single-shot Ollama-backed reasoner agent (`agent_id: gemma-1`) for the
EdgeCitadel v0.2 fleet. Wraps `POST http://OLLAMA_HOST:OLLAMA_PORT/api/generate`
and returns the response in a single `result` envelope.

`runtime.kind: native` (Ollama is stateless inference, not an upstream agent).
Single skill: `reasoning.chat`. No streaming, no conversation memory — those
are deferred to Phase 2.5; see `docs/roadmap.md`.

Spec: `docs/superpowers/specs/2026-04-24-gemma-adapter-design.md`.

## Subjects

- `agents.gemma-1.register` — A2A Agent Card on startup.
- `agents.gemma-1.heartbeat` — every 30s.
- `agents.gemma-1.status` — offline on shutdown.
- `agents.gemma-1.inbox` — JetStream WorkQueue, durable consumer
  `gemma-1_inbox` (`max_ack_pending=1`, `ack_wait=300s`, `max_deliver=3`).
- `agents.gemma-1.outbox` — plain-NATS audit mirror (per ADR-0006).

## Environment

| Var | Default | Purpose |
|---|---|---|
| `NATS_URL` | (required) | Fleet broker, e.g. `nats://localhost:4222` |
| `NATS_TOKEN` | (required) | Fleet token |
| `OLLAMA_HOST` | `localhost` | Ollama HTTP host |
| `OLLAMA_PORT` | `11434` | Ollama HTTP port |
| `OLLAMA_MODEL` | `gemma3:4b` | Default model name |
| `OLLAMA_TIMEOUT_SEC` | `120` | HTTP timeout for `/api/generate` |
| `ACK_WAIT_SEC` | `300` | JetStream `ack_wait` (PullConsumer kwarg) |

## One-time setup

```bash
brew install ollama          # or your platform's installer
ollama serve &               # foreground or via the GUI app
ollama pull gemma3:4b        # ~3 GB
ollama list                  # confirm model is present
```

The adapter fails fast on startup if Ollama is unreachable or the
configured model is not pulled. There is no auto-pull (intentional —
see spec §"Why fail-fast, not auto-pull?").

## Run

```bash
cd <repo>
PYTHONPATH=. python3 -m adapters.gemma.adapter
```

Connects to NATS, registers the Agent Card, starts a 30s heartbeat,
and runs the JetStream pull consumer against `agents.gemma-1.inbox`.

## Test

```bash
# Unit tests (no live Ollama required)
python3 -m pytest adapters/gemma/tests/test_gemma.py -v

# Integration test (gated)
OLLAMA_URL_TEST=http://localhost:11434 OLLAMA_MODEL_TEST=gemma3:4b \
  python3 -m pytest adapters/gemma/tests/test_gemma_integration.py -v

# E2E smoke (requires running stack + Ollama + adapter)
cd e2e && npm test -- phase2-gemma-smoke.spec.js
```

## Failure modes

The adapter returns one of seven typed error codes in `payload.error`:

| Code | task_state | Cause |
|---|---|---|
| `unsupported_type` | rejected | Inbound is not a `command` envelope |
| `empty_prompt` | rejected | `payload.body` is missing or whitespace |
| `ollama_unreachable` | failed | TCP connect failed |
| `ollama_timeout` | failed | HTTP read/write timeout |
| `model_not_loaded` | failed | Ollama 404 |
| `ollama_inference_error` | failed | Ollama 5xx |
| `ollama_bad_response` | failed | non-JSON or non-200 non-5xx |

Other exceptions bubble up to `pull_consumer`'s nak path, which
redelivers up to 3× before the JetStream MAX_DELIVERIES advisory
fires (captured by the aggregator's `on_advisory` handler).

## Cancel

SIGTERM or SIGINT publishes `agents.gemma-1.status` with
`agent_state: offline`, stops the consumer, drains NATS. The
aggregator marks the agent offline on receipt; the watchdog (Phase 3)
will detect missing heartbeats independently.
