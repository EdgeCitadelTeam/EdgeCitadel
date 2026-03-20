# Claude Compatibility

Claude Code should load the shared project instructions from `AGENTS.md`.

@AGENTS.md

## Claude-Specific Notes

- Shared Claude settings live in `.claude/settings.json`.
- Personal Claude overrides belong in `.claude/settings.local.json` and should stay untracked.
- Project-specific Claude subagents remain in `.claude/agents/`.
- Use `docs/agent-setup.md` for the cross-tool layout and verification commands.
