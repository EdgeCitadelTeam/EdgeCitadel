# Gemma Adapter Design — EdgeCitadel v0.2 / Phase 2

Status: design-complete, pending user review
Date: 2026-04-24
Branch (target): TBD; will branch from the Phase 1 implementation branch
Author: collaborative brainstorm
Builds on: `docs/superpowers/specs/2026-04-23-agent-messaging-design.md` (rev 7)
Predecessor plan: `docs/superpowers/plans/2026-04-23-agent-messaging-v0.1-phase-1.md`

## Summary

Phase 2 ships the **Gemma adapter** — a single-shot Ollama-backed reasoner agent that wraps the v0.1 messaging contract end-to-end. It is the first non-trivial workload on the `adapters/_common/` skeleton (Phase 1 Tasks 9–11) and the first agent in the fleet that exposes a `reasoner` role.

Scope is deliberately tight: one skill (`reasoning.chat`), `runtime.kind: native`, no streaming, no conversation memory, no auto-pull of models. The adapter wraps Ollama `POST /api/generate` and returns the response in a single `result` envelope. Conversational memory, multi-skill dispatch, token streaming, and a WebSocket bridge for live UI updates are all explicitly deferred to Phase 2.5+ and tracked in `docs/roadmap.md`.

## Problem

Phase 1 shipped the messaging foundation (strict envelope schema, JetStream WorkQueue, validator, HTTP API, adapter common, shell adapter, openclaw browser client). The shell adapter is sufficient to validate the wire contract, but its workload (subprocess exec) is too short and stateless to exercise some of the contract's harder corners:

- Long-running tasks (LLM inference can take 10–60s on consumer hardware), where `ack_wait` extension via `in_progress()` matters.
- Variable-latency tasks where keepalive cadence is non-trivial.
- Tasks where the upstream is over the network (Ollama HTTP) rather than a subprocess — different failure modes.
- Tasks that produce structured-but-large payloads (LLM responses can be kilobytes), exercising the 1 MB `max_msg_size` headroom.

Without a reasoner agent on the bus, the dashboard can demonstrate "send a shell command, see the result" but not "ask a question, get an answer" — the latter is the actual end-user product. Phase 2 unblocks the demo path.

## Approach

A single Python process (`adapters/gemma/adapter.py`) running on the host (NOT in a Docker container — see [Deployment](#deployment)). It loads its A2A Agent Card from `adapters/gemma/config.yaml` via `adapters/_common/agent_card.build_card()`, registers + heartbeats over plain NATS, and runs `adapters/_common/pull_consumer.PullConsumer` against `agents.gemma-1.inbox`.

For each `command` envelope, the adapter:
1. Validates type and prompt presence.
2. POSTs to Ollama `/api/generate` with the prompt and optional args (model override, temperature, max_tokens, timeout).
3. Returns the response text (or a typed error code) in the result payload, with `task_state: completed | failed | rejected`.
4. The PullConsumer publishes the `result` envelope to the sender's inbox via JetStream (with `Nats-Msg-Id` dedup) and mirrors to `agents.gemma-1.outbox` per ADR-0006.

`runtime.kind: native`. `runtime.roles: [reasoner]`. The bridge pattern is **not** used — Ollama is a stateless inference backend, not an upstream agent with its own lifecycle. (See [Why native, not bridge?](#why-native-not-bridge) below.)

## Out of scope

The following are deliberately deferred to keep Phase 2 a 1-session smoke. Each appears in `docs/roadmap.md`:

- **Multi-skill dispatch** (`text.summarize`, `text.classify`, `code.explain`, ...) — Phase 2.5.
- **Conversational memory** keyed by `context_id` — Phase 2.5; will swap `/api/generate` for `/api/chat`.
- **Token streaming** via `task.progress` envelopes — Phase 2.5; needs WebSocket bridge to be user-visible.
- **WebSocket bridge** for live UI updates — separate phase; `useWebSocket.js` exists in the frontend but its server endpoint doesn't.
- **Non-Ollama LLM backends** (vLLM, llama.cpp, OpenAI-compatible, Anthropic API) — out of scope for v0.2; would change failure modes and auth assumptions.
- **Auto-pull on startup** — adapter fails fast on missing model; operator runs `ollama pull` once during setup.
- **Container packaging** of the Gemma adapter — host-process for now.
- **Phase 1 follow-ups** (stale `.claude/rules/*` files, `httpx` pin, `OPENCLAW_API_KEY` retirement, JetStream test fast-skip) — tracked in `docs/roadmap.md`.

Forward-compat hooks the spec preserves so deferred items land cleanly:
- `context_id` is propagated from inbound to outbound result envelopes (memory drop-in).
- `skills` array in the card is open-ended (multi-skill drop-in).
- `capabilities.streaming` is `false` today; flipping to `true` is schema-clean (streaming drop-in).

## Architecture

### Process model

Single Python process running on the host. Same shape as the shell adapter — invoked via `python3 -m adapters.gemma.adapter` from the repo root. SIGTERM/SIGINT triggers `template.main()`'s graceful shutdown (publish offline status → stop consumer → drain).

The adapter MUST run on the same host as Ollama (or a network-reachable host with `OLLAMA_HOST` env override). Mac Mini deployment in Phase 5 will run both side-by-side as systemd-style services.

### Subjects

Subscribed:
- `agents.gemma-1.inbox` (JetStream WorkQueue, durable consumer `gemma-1_inbox`).

Published:
- `agents.gemma-1.register` — A2A Agent Card on startup.
- `agents.gemma-1.heartbeat` — every `runtime.heartbeat_interval_sec` (30s default).
- `agents.gemma-1.status` — offline on shutdown.
- `agents.{sender_id}.inbox` — JetStream result envelope.
- `agents.gemma-1.outbox` — plain-NATS audit mirror (per ADR-0006).

No new subject inventory entries — uses existing v0.1 contract.

### Agent Card (config.yaml → registered card)

```yaml
# adapters/gemma/config.yaml
agent_id: gemma-1
name: gemma-1
description: Single-shot Ollama-backed reasoner. Wraps /api/generate.
version: 0.1.0
runtime:
  kind: native
  roles: [reasoner]
  tags: [ollama, llm]
  heartbeat_interval_sec: 30
skills:
  - id: reasoning.chat
    name: chat
    description: Send a free-text prompt; receive the model's full response.
    tags: [llm, generate]
capabilities:
  streaming: false
```

`build_card()` produces the A2A v1.0 shape with the NATS extension URI auto-attached. `runtime.roles: [reasoner]` distinguishes the agent from `worker` (shell). The role is enum-validated by `schemas/agent-card.v1.json` (Phase 1 Task 2).

### Why native, not bridge?

The agent-card schema's `runtime.kind: native | bridge` discriminates on **whether the upstream has its own agent identity / lifecycle / state**, not on whether the adapter calls out to another process. By that test:

| Adapter | Upstream | Has own agent state? | runtime.kind |
|---|---|---|---|
| shell (Phase 1) | subprocess | No (stateless exec) | native |
| **gemma (Phase 2)** | **Ollama HTTP** | **No (stateless inference)** | **native** |
| Future Hermes wrapper | Nous Research Hermes | Yes (sessions, conversation, personalities) | bridge |
| Future AG2 wrapper (Phase 4) | AG2 framework | Yes (agent runtime, group chat) | bridge |
| Future OpenAI Assistants wrapper | OpenAI Threads API | Yes (server-side threads) | bridge |

Ollama is a pure inference backend: no sessions, no resume tokens, no per-tenant state. Even when Phase 2.5 adds conversation memory, the memory will live in OUR adapter — Ollama stays stateless. Therefore the adapter is the agent; Ollama is library-style infrastructure. `runtime.kind: native`.

## Envelope flow

### Inbound (`command`)

```json
{
  "v": 1,
  "id": "<uuid4>",
  "type": "command",
  "sender_id": "aggregator | openclaw-<session>",
  "recipient_id": "gemma-1",
  "task_id": "<uuid4>",
  "context_id": "<uuid4>",         // optional; preserved on result
  "timestamp": "2026-04-24T...Z",
  "payload": {
    "body": "<prompt text>",
    "args": {                       // all optional
      "model": "gemma3:4b",
      "temperature": 0.7,
      "timeout_sec": 60,
      "max_tokens": 1024
    }
  }
}
```

`payload.body` is the prompt. `payload.args` is a flat dict of optional overrides; the adapter passes through what Ollama accepts and ignores unknown keys.

### Adapter `handle()` decision flow

```
1. if env.type != "command":
       return ({"error": "unsupported_type"}, "rejected")

2. body = env.payload.get("body", "").strip()
   if not body:
       return ({"error": "empty_prompt"}, "rejected")

3. args = env.payload.get("args") or {}
   model = args.get("model") or OLLAMA_MODEL
   timeout_sec = args.get("timeout_sec") or OLLAMA_TIMEOUT_SEC

4. POST http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate
       body = {model, prompt: body, stream: false,
               options: {temperature, num_predict: max_tokens}}
       timeout = timeout_sec

5. on HTTP 200:
       return ({"body": resp.response, "model": model,
                "duration_ms": <int>}, "completed")

6. on connection refused:
       return ({"error": "ollama_unreachable"}, "failed")

7. on read timeout:
       return ({"error": "ollama_timeout"}, "failed")

8. on HTTP 404 with "model not found":
       return ({"error": "model_not_loaded"}, "failed")

9. on HTTP 5xx:
       return ({"error": "ollama_inference_error"}, "failed")

10. on JSON decode error:
        return ({"error": "ollama_bad_response"}, "failed")

11. any other exception → propagated to PullConsumer's nak path
        (re-deliver up to max_deliver=3, then MAX_DELIVERIES advisory)
```

### Outbound (`result`)

The adapter returns `(payload, state)`. `pull_consumer._publish_result` (Phase 1 Task 10) constructs and publishes:

```json
{
  "v": 1,
  "id": "<new uuid4>",
  "type": "result",
  "sender_id": "gemma-1",
  "recipient_id": "<inbound.sender_id>",
  "task_id": "<inbound.task_id>",
  "context_id": "<inbound.context_id>",   // preserved if present
  "task_state": "completed | failed | rejected",
  "timestamp": "...",
  "payload": {
    "body": "<model response text>",       // on completed
    "model": "gemma3:4b",                   // on completed
    "duration_ms": 1234,                    // on completed
    "error": "<code>"                       // on failed | rejected
  }
}
```

Routes:
- JetStream publish to `agents.{inbound.sender_id}.inbox` with `Nats-Msg-Id: <result.id>` for 5-min dedup.
- Plain-NATS mirror to `agents.gemma-1.outbox` per ADR-0006 (best-effort, not retried).

## Failure modes (full table)

| Condition | Detection | task_state | payload.error | Notes |
|---|---|---|---|---|
| Non-command envelope type | `env["type"] != "command"` | rejected | `unsupported_type` | Pre-handler check |
| Empty/whitespace prompt | `body.strip() == ""` | rejected | `empty_prompt` | Pre-handler check |
| Ollama connection refused | `httpx.ConnectError` | failed | `ollama_unreachable` | Backend down |
| Ollama read timeout | `httpx.ReadTimeout` | failed | `ollama_timeout` | Inference exceeded `timeout_sec` |
| Ollama 404 ("model not found") | HTTP 404 + body match | failed | `model_not_loaded` | Operator forgot `ollama pull` |
| Ollama 5xx | HTTP 500/502/503 | failed | `ollama_inference_error` | OOM, internal error |
| Malformed Ollama response | `json.JSONDecodeError` | failed | `ollama_bad_response` | Defensive |
| Any other exception | Bubbles to `pull_consumer._handle_msg` | failed | `<exception class name>` | Existing nak path; redelivers |

The PullConsumer's `nak`-on-exception path means JetStream redelivers persistent failures up to `max_deliver=3`. After three failed attempts, the JetStream MAX_DELIVERIES advisory fires and the aggregator's `on_advisory` handler (Phase 1 Task 5) records a poison event. The dashboard's poison panel (Phase 1 Task 16) surfaces these.

The seven adapter-level error codes above are typed strings, NOT exception class names. This stable vocabulary lets the dashboard color-code or aggregate failures without scraping exception messages.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `NATS_URL` | (required) | Fleet broker (`nats://...`) |
| `NATS_TOKEN` | (required) | Fleet token; same as shell adapter |
| `OLLAMA_HOST` | `localhost` | Ollama HTTP host |
| `OLLAMA_PORT` | `11434` | Ollama HTTP port |
| `OLLAMA_MODEL` | `gemma3:4b` | Default model name |
| `OLLAMA_TIMEOUT_SEC` | `120` | HTTP timeout for `/api/generate` |
| `ACK_WAIT_SEC` | `300` | JetStream ack_wait (PullConsumer kwarg) |

`config.yaml` declares the static identity (agent_id, skills, runtime.kind). Env vars cover deployment-specific values. `.env.example` will document all six new vars.

`OLLAMA_TIMEOUT_SEC=120` is generous enough for `gemma3:4b` (~10–20s) and `gemma3:12b` (~30–60s) on M-series Macs but too short for, say, a 70b model on the same hardware. Operators picking heavy models override per-deployment.

## Preflight + lifecycle

### Startup preflight (in `main()`, before consumer starts)

1. Build card from `config.yaml` via `agent_card.build_card()`.
2. HTTP `GET http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/tags`.
   - Connection refused → log error, exit 1. (Don't register; nothing to do.)
   - Other failure → log error, exit 1.
3. Parse the response; look for `OLLAMA_MODEL` in `models[].name`.
   - Not found → log error listing what IS pulled, exit 2.
4. Connect to NATS; publish register; start heartbeat task.
5. Start `PullConsumer(agent_id="gemma-1", handler=handle).run()`.

### Why fail-fast, not auto-pull?

`ollama pull <model>` downloads 3–8 GB and takes minutes. Doing this on adapter startup masks "operator forgot to pull" as "adapter is slow" and slows recovery loops during deployment debugging. Operators run `ollama pull` once during setup; the adapter assumes a working environment. The README will document this.

### Shutdown

Inherits `template.main()`'s graceful path:
1. SIGTERM or SIGINT received → set stop event.
2. Cancel heartbeat task.
3. Publish `status: offline` envelope on `agents.gemma-1.status`.
4. Call `pull_consumer.stop()` (sets `_running = False`).
5. Cancel the consumer task.
6. `nc.drain()` to flush in-flight publishes.

The aggregator marks the agent `offline` on the next status reception. After 2× `heartbeat_interval_sec` of silence (Phase 3 watchdog), the agent transitions to `offline` even without a status envelope.

## Testing

### Unit (no live Ollama)

`adapters/gemma/tests/test_gemma.py`:

- `test_handle_command_calls_ollama` — mock `httpx.AsyncClient.post`; assert request body shape `{model, prompt, stream: false, options}` and result envelope shape.
- `test_handle_rejects_non_command` — same shape as `test_shell:test_handle_rejects_non_command`.
- `test_handle_rejects_empty_prompt` — body is `""` or `"   "`, expect `("rejected", "empty_prompt")`.
- `test_handle_ollama_unreachable_returns_failed` — mock `httpx.ConnectError`.
- `test_handle_ollama_timeout_returns_failed` — mock `httpx.ReadTimeout`.
- `test_handle_model_not_loaded_returns_failed` — mock 404 with `{"error": "model not found"}`.
- `test_handle_ollama_5xx_returns_failed` — mock HTTP 500.
- `test_context_id_preserved` — inbound has `context_id`; assert the adapter doesn't strip it (PullConsumer does the actual preservation, but the test verifies the adapter doesn't accidentally consume it).

Eight unit tests. Pattern mirrors `adapters/shell/tests/test_shell.py`.

### Integration (gated on live Ollama)

`adapters/gemma/tests/test_gemma_integration.py`:

If `OLLAMA_URL_TEST` env var is set, actually call `POST {OLLAMA_URL_TEST}/api/generate` against `gemma3:4b` (smallest reasonable default). Assert:
- Response time < 60s.
- `task_state: completed`.
- `payload.body` is non-empty.

Otherwise `pytest.skip`. Pattern mirrors Phase 1's `test_jetstream_bootstrap.py`.

### E2E (Playwright, requires running stack + Ollama)

`e2e/tests/phase2-gemma-smoke.spec.js`:

```javascript
test('gemma round trip — POST /command returns task_id, result completes', async ({ request }) => {
  const post = await request.post(`${API}/api/command/gemma-1`, {
    data: { body: 'What is 2+2? Reply with just the number.' }
  });
  expect(post.status()).toBe(202);
  const { task_id } = await post.json();

  let result;
  for (let i = 0; i < 60; i++) {
    await new Promise(r => setTimeout(r, 1000));
    const q = await request.get(`${API}/api/messages?task_id=${task_id}&type=result`);
    const rows = await q.json();
    if (rows.length) { result = rows[0]; break; }
  }
  expect(result).toBeDefined();
  expect(result.task_state).toBe('completed');
  expect(result.payload.body).toMatch(/4/);
});
```

60s timeout (longer than Phase 1 smoke; LLM inference is slow). The test doesn't assert exact response text — `gemma3:4b` is small enough that exact wording is unstable. Just check the answer contains `4`.

## Deployment

### Dev (local Mac)

```bash
brew install ollama  # one-time
ollama serve &       # background; or use the GUI app
ollama pull gemma3:4b
cd <repo>
docker compose up -d  # NATS + aggregator + dashboard + nginx
PYTHONPATH=. python3 -m adapters.gemma.adapter
```

### Mac Mini (Phase 5)

Out of scope for Phase 2; tracked in `docs/roadmap.md`. Will run Ollama and the Gemma adapter as launchd services (or systemd-style equivalents) alongside the docker-compose stack.

## Files (new + modified)

```
adapters/gemma/                                  [NEW directory]
  __init__.py                                    [empty]
  adapter.py                                     [handle() + main()]
  config.yaml                                    [agent_id: gemma-1, role: reasoner]
  requirements.txt                               [httpx, nats-py, pyyaml, jsonschema]
  README.md                                      [how to run; preflight; ollama pull]
  tests/
    __init__.py                                  [empty if needed for pytest]
    test_gemma.py                                [8 unit tests]
    test_gemma_integration.py                    [gated, live-Ollama]

docs/
  roadmap.md                                     [NEW — Out of scope + Phase handover]
  superpowers/specs/2026-04-24-gemma-adapter-design.md  [NEW — this file]
  superpowers/plans/2026-04-24-gemma-adapter.md  [NEW — written by writing-plans skill]
  CHANGELOG.md                                   [MODIFY — Unreleased entry]

e2e/tests/phase2-gemma-smoke.spec.js             [NEW]

.env.example                                     [MODIFY — add OLLAMA_HOST, OLLAMA_PORT,
                                                  OLLAMA_MODEL, OLLAMA_TIMEOUT_SEC]
```

No changes to schemas, aggregator, or adapters/_common — Phase 2 rides on the v0.1 contract as-is.

## Verification

End-of-phase checks (operator runs against a live stack):

| Check | Command | Expected |
|---|---|---|
| Ollama running | `curl -s http://localhost:11434/api/tags \| jq` | model list includes `OLLAMA_MODEL` |
| Adapter registered | `curl http://localhost:8000/api/agents/gemma-1` | A2A card; `agent_state: online` |
| Adapter heartbeat fresh | `curl http://localhost:8000/api/agents/gemma-1` | `last_heartbeat` within ~30s |
| Round-trip works | `POST /api/command/gemma-1 {body: "What is 2+2?"}` then poll | `task_state: completed`, body contains `4` |
| Timeout handling | `POST /api/command/gemma-1 {body: "...", args: {timeout_sec: 1}}` (long prompt) | `task_state: failed`, `error: ollama_timeout` |
| Model-not-loaded | Set `OLLAMA_MODEL=does-not-exist`, restart adapter | adapter exits 2 with clear error |
| Crash recovery | Kill adapter mid-task; restart | unacked command redelivers (verifies pull_consumer ack semantics under real workload) |
| Queue endpoint | `curl /api/agents/gemma-1/queue` | `{pending, ack_pending}` integers |
| Phase 2 E2E | `cd e2e && npm test -- phase2-gemma-smoke.spec.js` | PASS |

The "crash recovery" check is the most informative — it's the first time we've stressed `max_ack_pending=1` + `ack_wait=300s` + handler redelivery against a real long-running task.

## Future work (cross-references)

See `docs/roadmap.md` for:
- **Out of scope (deferred enhancements):** multi-skill dispatch, conversation memory, token streaming, WebSocket bridge, non-Ollama backends, auto-pull, container packaging.
- **Phase handover:** Phase 3 watchdog + dashboard registry, Phase 4 AG2 + A2A wrapper, Phase 5 Mac Mini deploy.
