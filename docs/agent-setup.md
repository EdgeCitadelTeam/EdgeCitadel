# Agent Setup

## Canonical Layout

- `CLAUDE.md` (repo root) is the team-shared coding-agent instruction file. Hard ceiling 200 lines.
- Nested `CLAUDE.md` files in active subprojects (`aggregator/`, `frontend/`, `openclaw-client/`, `e2e/`) add local rules without duplicating the root file.
- Per-subsystem verification recipes live in `.claude/skills/verify-*` and load on demand.

## Claude Files

- Shared Claude project settings live in `.claude/settings.json`.
- Personal Claude overrides belong in `.claude/settings.local.json`, which is intentionally not committed.
- `CLAUDE.local.md` is reserved for personal overrides and is gitignored.
- Project subagents remain in `.claude/agents/`.

## Verification

- Claude Code: open the repo and confirm the root `CLAUDE.md` loads and `.claude/agents/` is visible.
- For repo-structure, shared config, Docker wiring, or agent-workflow changes, restart the stack with `docker compose down && docker compose up --build -d` and run at least one smoke check such as `curl http://localhost:8222/healthz` or `curl http://localhost/api/system/status`.
- For frontend, browser-flow, or operator-workflow changes, also run actual Playwright verification from `e2e/`, for example `npm test -- tests/health.spec.js tests/dashboard-command-pipeline.spec.js` or the narrowest relevant spec set.
- Per-subsystem deeper recipes: invoke `verify-frontend`, `verify-backend`, or `verify-infra` skills.

## Notes

- Prefer `frontend/` for UI work. The runtime service is still named `dashboard`, but its source lives in `frontend/`.
- Keep local-only secrets, machine-specific permissions, and experiments out of committed agent config.

## Hermes Agent (bridge)

Phase 6. Bridges Nous Research's [Hermes Agent](https://github.com/NousResearch/hermes-agent) onto the fleet as agent `us-mac-hermes`. Hermes owns its own reasoning + memory; this adapter is a transport translator. See [adapters/hermes/README.md](../adapters/hermes/README.md) for the full operator guide.

Quick path (Mac):

```bash
# Install Hermes Agent
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
hermes setup            # pick a model provider
hermes gateway setup    # enable the API Server platform (port 8642)
hermes gateway run &    # foreground gateway exposing OpenAI-compat HTTP on :8642

# On the aggregator host:
./add-agent.sh us-mac-hermes   # prints broker IP and NATS_TOKEN

# Back on the Mac running Hermes:
cp adapters/hermes/agent.env.example adapters/hermes/agent.env
# Edit: NATS_URL, NATS_TOKEN, HERMES_TOKEN
pip install -r adapters/hermes/requirements.txt
python -m adapters.hermes.adapter
```

For fully-local inference, configure Hermes to use a local Ollama instance running a Hermes 4 GGUF — see the Modelfile recipe in `adapters/hermes/README.md`.
