---
name: edgecitadel
description: Discover EdgeCitadel Agents, delegate work, handle pending Agent tasks, or inspect local task and trace health from an active Codex session.
---

# EdgeCitadel

Use the `edgecitadel_*` MCP tools for EdgeCitadel operations. Check
`edgecitadel_diagnose` before interpreting connectivity failures. List Agents
before delegation when the recipient or skill is unclear, preserve returned
task IDs, and use task status for correlation.

Inbound work is not accepted merely because it appears in the inbox. Describe
the task to the user when approval is needed, then record `accepted`, `running`,
and exactly one terminal state with `edgecitadel_task_update`, including the
completed output or structured failure. Do not claim that this Codex Agent
remains available after the current plugin/MCP session exits.
