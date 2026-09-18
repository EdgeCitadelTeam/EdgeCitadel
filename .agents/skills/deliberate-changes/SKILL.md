---
name: deliberate-changes
description: Check the affected invariants before changing shared infrastructure, messaging contracts, persistence, or substantial refactors.
---

# Verify the assumptions that matter

Before implementation, identify the intended behavior and inspect the callers,
contracts or stored state it affects. Explain material uncertainty and the
smallest viable change in a short update; routine work needs no separate plan.

For NATS changes, check affected publishers/subscribers. For persistence, check
transaction boundaries and the intended resulting state. For deployment, check
the actual entrypoint/configuration. Only investigate surfaces the change touches.

Use the existing focused tests to establish behavior where practical, then verify
the change. Reconsider the affected decision if an assumption proves wrong;
do not restart the entire process automatically. Ask the user only for a material
unresolved choice, not permission already supplied by the task.
