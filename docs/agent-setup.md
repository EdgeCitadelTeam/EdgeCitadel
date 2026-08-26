# Agent Setup

## Canonical Layout

- `AGENTS.md` is the single source of truth for repository-wide coding-agent
  policy, commands, and quality gates.
- `.agents/skills/` owns shared operational recipes such as `commit-check` and
  the `verify-*` skills.
- Tool-specific files are integration layers only: `CLAUDE.md` points Claude
  Code to the canonical policy, while `.claude/skills/` links to the shared
  recipes instead of copying them.
- `.claude/agents/` and `.codex/agents/` remain tool-specific because their
  manifest formats and supported tool declarations differ.

## Local Configuration

- Shared Claude Code settings live in `.claude/settings.json`.
- Personal Claude overrides belong in `.claude/settings.local.json` or
  `CLAUDE.local.md`; both stay uncommitted.
- Keep local-only secrets, machine-specific permissions, and experiments out of
  committed agent configuration.

## Verification

After changing agent configuration, verify that every compatibility link resolves
and that the canonical skills are discoverable:

```bash
find -L .claude/skills -type l ! -exec test -e {} \; -print
find .agents/skills -name SKILL.md -print
```

Invoke the relevant `verify-frontend`, `verify-backend`, or `verify-infra` recipe
for product changes. Agent-configuration-only changes do not require a stack
restart.

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
