# Claude Code Integration

Repository-wide engineering policy, commands, quality gates, and documentation
links are maintained in [`AGENTS.md`](AGENTS.md). Read and follow that file before
making changes; do not duplicate its rules here.

Claude-specific configuration lives under `.claude/`:

- `settings.json` contains shared hooks and permissions.
- `settings.local.json` and `CLAUDE.local.md` are uncommitted personal overrides.
- `commands/` provides lightweight Claude Code workflows that defer to
  `AGENTS.md`.
- `skills/` contains compatibility links to the canonical recipes in
  `.agents/skills/`.
