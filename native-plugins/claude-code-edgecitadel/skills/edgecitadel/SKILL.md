---
name: edgecitadel
description: Discover EdgeCitadel Agents, delegate work, handle pending Agent tasks, or inspect local task and trace health from an active Claude Code session.
---

# EdgeCitadel

Use the `edgecitadel_*` MCP tools for EdgeCitadel operations. Diagnose local
connectivity before interpreting a failed delegation, preserve returned task
IDs, and query status for correlation.

Inbox visibility is not task acceptance. Ask for user approval when the task
requires it, then record `accepted`, `running`, and exactly one terminal state
with `edgecitadel_task_update`. Do not claim availability after the current
Claude Code plugin/MCP session exits. Include the completed output or structured
failure in the terminal update.
