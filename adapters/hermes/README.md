# Hermes Bridge Adapter

Bridges a locally-installed [Hermes Agent](https://github.com/NousResearch/hermes-agent) onto the EdgeCitadel NATS fabric as agent `us-mac-hermes`. Adds a second-personality reasoner to the fleet alongside `gemma-1`.

This is a **bridge** adapter — Hermes Agent itself owns reasoning, tool-calling, and memory (under `~/.hermes/`). The adapter is a pure transport translator: NATS envelope ↔ Hermes' OpenAI-compatible HTTP API on `:8642`. Per [ADR-0009](../../docs/adr/0009-bridge-adapter-memory-ownership.md), this adapter does NOT use the aggregator's `memory.turns.*` service.

## Three-step onboarding (Mac)

### Step 1 — Install Hermes Agent and enable the API Server platform

```bash
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
hermes setup                  # interactive: pick model provider, configure SOUL.md
hermes gateway setup          # interactive: enable the "API Server" platform (port 8642)
hermes gateway run &          # foreground gateway with API Server active
```

The API Server platform exposes Hermes over OpenAI-compatible HTTP at `http://localhost:8642/v1` (`POST /v1/chat/completions`, `GET /v1/models`, `POST /v1/runs`, SSE streaming, `X-Hermes-Session-Id` for session continuity). It uses Bearer auth — the token is configured via `hermes config set` or shown by `hermes gateway run`. Copy it for `HERMES_TOKEN` in step 3.

For production / auto-start at login, use `hermes gateway install` to register a launchd service instead of `hermes gateway run &`.

### Step 2 — Get NATS broker host + token

On the aggregator host (or any machine with the repo and a populated `.env`):

```bash
./add-agent.sh us-mac-hermes
```

The script prints the broker IP and `NATS_TOKEN`. Ignore the `./join.sh ...` line — that's for browser-style agents. We only need the broker + token.

### Step 3 — Configure & start the bridge adapter

On the Mac running Hermes:

```bash
cd /path/to/edge-research
cp adapters/hermes/agent.env.example adapters/hermes/agent.env
# Edit adapters/hermes/agent.env: NATS_URL, NATS_TOKEN, HERMES_TOKEN
pip install -r adapters/hermes/requirements.txt
python -m adapters.hermes.adapter
```

The adapter publishes its agent card to `agents.us-mac-hermes.register` and starts heartbeating. Within ~1 second, `us-mac-hermes` appears on the dashboard agent roster.

For production (auto-start at login, restart on crash), use the launchd plist instead — see `scripts/launchd/com.edgecitadel.hermes-bridge.plist`.

## Verifying the install

```bash
# Card visible in API
curl http://<aggregator-host>/api/agents/us-mac-hermes/card | jq '.metadata."runtime.kind"'   # → "bridge"

# Heartbeat flowing
nats sub 'agents.us-mac-hermes.heartbeat' --token "$NATS_TOKEN"      # one envelope every 30s

# Send a smoke command (through the dashboard's chat UI; or via NATS):
nats req 'agents.us-mac-hermes.inbox' '{"v":1,"type":"command","sender_id":"smoke","payload":{"body":"hello"}}'
```

## Fully-local inference (recommended)

By default `hermes setup` configures the Nous Portal cloud provider. To run inference locally on the same Mac:

```bash
# Pull a Hermes 4 GGUF into Ollama (community Modelfile path)
# See: https://huggingface.co/lmstudio-community/Hermes-4-14B-GGUF
ollama create hermes-4-14b -f Modelfile.hermes  # see snippet below
ollama run hermes-4-14b "test"                  # warm up

# Point Hermes at local Ollama
hermes model
# → Provider: Ollama
# → Endpoint: http://localhost:11434
# → Model: hermes-4-14b
```

Modelfile.hermes (snippet — confirm the GGUF file path matches your download):
```
FROM ./Hermes-4-14B-Q5_K_M.gguf
PARAMETER temperature 0.6
PARAMETER top_p 0.95
PARAMETER top_k 20
TEMPLATE """{{ if .System }}<|im_start|>system
{{ .System }}<|im_end|>
{{ end }}{{ if .Prompt }}<|im_start|>user
{{ .Prompt }}<|im_end|>
{{ end }}<|im_start|>assistant
"""
```

(No first-party `nousresearch/hermes4` Ollama tag exists as of 2026-05-06; community Modelfiles wrapping the LM Studio GGUFs are the recommended path.)

## Backups

Hermes' memory store lives under `~/.hermes/`. Operators are responsible for backing this directory up — the aggregator's `conversation_turns` table does NOT mirror it.

## Logs and observability

- Adapter lifecycle / errors: `agents.us-mac-hermes.log` envelopes (visible in the dashboard's LogViewer panel).
- Hermes server logs: wherever `hermes gateway run` writes them (default: stdout; redirected by the launchd plist to `/var/log/edgecitadel/hermes-server.log`).
- Bridge adapter stdout/stderr: under launchd, `/var/log/edgecitadel/hermes-bridge.{log,err}`.

## Common errors

| Symptom | Cause | Fix |
|---|---|---|
| `PreflightError: hermes_unreachable` at startup | `hermes gateway run` not running | `hermes gateway run &` |
| `PreflightError: hermes_auth_failed` at startup | Wrong `HERMES_TOKEN` | Re-copy from `hermes gateway run` startup logs |
| Adapter starts but `us-mac-hermes` doesn't appear on dashboard | Wrong `NATS_URL` or `NATS_TOKEN` | `./add-agent.sh us-mac-hermes` on the aggregator host, copy the printed values |
| Commands return `task_state: failed, error: hermes_request_failed` | Hermes died mid-session, or Hermes' upstream provider is down | Restart `hermes gateway run`; check Hermes' own provider config |
| Streamed bubble never finalizes in the dashboard | Hermes stream stalled | Check `hermes gateway run` logs; bridge will time out after `HERMES_TIMEOUT_SEC` |
| `hermes gateway setup` ends with `Bootstrap failed: 5: Input/output error` and the gateway dies | Setup wizard offered to "restart" the gateway, which calls `launchctl bootstrap` against `~/Library/LaunchAgents/ai.hermes.gateway.plist` — but you ran `hermes gateway run` (foreground) and never `hermes gateway install`, so the plist doesn't exist. Upstream bug ([NousResearch/hermes-agent#11323](https://github.com/NousResearch/hermes-agent/issues/11323)). | Either decline the restart prompt and run `hermes gateway run &` again, or run `hermes gateway install` once first so the launchd unit exists. |
| Weixin send fails with `iLink sendmessage rate limited: ret=-2 errcode=None errmsg=rate limited` | iLink returns `ret=-2` with empty `errmsg` to mean **stale `context_token`**, not actual throttling. The Weixin adapter in older Hermes (~v0.12.0) misclassifies it as rate limit and retries with the same stale token. Upstream issues [#17228](https://github.com/NousResearch/hermes-agent/issues/17228) / [#18100](https://github.com/NousResearch/hermes-agent/issues/18100). | Run `hermes update`. Workaround on the old version: send a DM to the bot from your phone to refresh the token (or delete `~/.hermes/weixin/accounts/<account>.context-tokens.json`). Avoid setting `WEIXIN_HOME_CHANNEL` until that channel has had user→bot activity. |

## Customizing the personality

The adapter forwards prompts as-is; Hermes' personality is configured by:
1. **`SOUL.md`** in Hermes' install directory (`hermes setup` creates this).
2. **Hermes' tools** (`hermes tools`) — 68 built-in tools.
3. **Hermes' provider config** (`hermes model`) — picks the upstream LLM.

To run multiple Hermes personalities, install multiple Hermes instances on different ports (`8642`, `8643`, ...) and start one bridge adapter per instance with distinct `agent_id` values (`us-mac-hermes`, `hermes-2`, ...). Each gets its own `config.yaml` + `agent.env`.
