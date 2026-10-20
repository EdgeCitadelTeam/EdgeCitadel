---
name: det-task
description: Execute one planned deterministic workload task.
---

Accept only `det-worker.v1` requests of the configured run. Wait for the planned
inputs, compute for the planned duration, output deterministic content, and
perform only the planned dispatches and effects (plus the workload's declared
malfunction when tainted). Resume a paused task without repeating completed work.
