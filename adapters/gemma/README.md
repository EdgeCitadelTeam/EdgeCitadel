# Gemma Adapter

Native nats-py adapter wrapping Ollama for Phase 2.5 conversational reasoning. Single-instance per fleet (`agent_id: gemma-1`).

## Identity & skills

- `runtime.kind: native`, `runtime.roles: [reasoner]`.
- `capabilities.streaming: true`.
- Four skills, dispatched by `payload.skill_id`:
  - `reasoning.chat` (default) — free-text Q&A.
  - `text.summarize` — 2-3 sentence summary, faithful to source.
  - `text.classify` — JSON output `{label, confidence}` validated against schema.
  - `code.explain` — line-by-line code walkthrough.

Skill definitions live in `adapters/gemma/config.yaml`. Adding a fifth skill = YAML edit + restart.

## Memory

Conversational continuity per `context_id`. Adapter fetches prior turns from the aggregator's memory service (NATS `memory.turns.get`) at inference start; persists user + assistant turns at end. 30-day retention. Best-effort: memory service downtime → inference proceeds without history.

## Streaming

Ollama `stream: true`. Adapter publishes `task.progress` envelopes on hybrid 8-tokens-or-100ms cadence. Final `result` envelope still emits with full text. Frontend renders streaming text as a synthetic bubble; replaced by the canonical bubble at completion.

## Running (dev)

```bash
docker compose up --build -d
NATS_URL=nats://localhost:4222 NATS_TOKEN=$NATS_TOKEN \
  OLLAMA_HOST=localhost OLLAMA_PORT=11434 OLLAMA_MODEL=gemma3:4b \
  python -m adapters.gemma.adapter
```

Verify:
```bash
curl -s http://localhost/api/agents/gemma-1/card | jq '.skills | length'
# → 4
curl -s http://localhost/api/agents/gemma-1/card | jq '.capabilities.streaming'
# → true
```

## v0.2.5 → v0.3 forward hooks

- `turn_embedding BLOB` column reserved in `conversation_turns`; sqlite-vec loaded at aggregator startup.
- `memory.turns.get` will accept an optional `query_embedding` field for semantic recall.
- Real Gemma tokenizer for `token_count` (currently byte/4 heuristic).
- LLM-summarized eviction (compress old turns into a `role: system` summary before purge).
