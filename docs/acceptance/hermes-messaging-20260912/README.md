# Hermes messaging dogfood proof

[Watch/download the live dashboard recording](roundtrip.mp4) (76 seconds, H.264 MP4).

Recorded on 2026-09-12 against the running Core dashboard on jim-eq, with
`us-mac-pro-codex` on the Mac and `jim-eq-hermes` on its separate NATS Leaf.
The runtime fixes in this PR were applied to those installed runtimes for this
test. Hermes also had its package recipient grant and scoped MCP configuration
installed; those machine-specific settings are not a released package change.

## Bug and fix

- Incoming terminal replies changed task state but discarded the result payload.
  Preserve that payload so the caller can read the actual returned answer.
- Durable inbox delivery omitted the sender outbox mirror consumed by the
  dashboard. Publish the same envelope to that outbox after the durable publish
  acknowledgment, before retiring the local spool entry.
- Hermes could execute incoming work but its model had no supported scoped MCP
  delegation path. Add delegation/status-only Managed Agent MCP mode, checking
  the installed package's recipient grant on every new task without opening a
  competing execution session.

## Recorded exchange

| Time in video (approximately) | Actual event |
| --- | --- |
| 0:02 | Codex sends the outer request to Hermes. |
| 0:26 | Hermes uses its MCP tool to delegate a child task back to Codex. |
| 0:27 | Codex completes the child with `DOGFOOD_20260912_RECORDED_CODEX_ACK`. |
| 0:34 | Hermes returns that acknowledgment and the child task ID in its own completed result. |

Outer task: `e6e20c32-dbdb-4ea3-bcb3-e82e1e9a7d83`.
Child task: `a8648db2-6881-47ca-a231-ce3d19ea8b79`.
The earlier `DOGFOOD_20260912` warm-up exchange is also visible; the full fresh
exchange uses the distinct `DOGFOOD_20260912_RECORDED` marker.

The recording continuously samples the actual browser viewport (100 frames,
about 1.3 captures/second), preserving capture timing when encoded at 10 fps.
It contains no synthetic messages or reconstructed UI. Capture started at
07:34:13.873 UTC and the last frame was captured at 07:35:28.197 UTC; the dashboard
displays local Pacific time. No audio was recorded.

[Message records](messages.json) are the Core API export for those two task IDs;
[task status](task-status.json) contains the caller's completed task records,
including both non-null result payloads. Core records establish the two command
and two completed-result envelopes. The outer result explicitly names the child;
this does not imply that automatic parent/child trace correlation is implemented.

## Verification

- Ruff lint and format checks passed across the maintained Python directories.
- Both maintained strict type gates passed (SDK and validation/JetStream modules).
- Agent Runtime: 563 passed, 5 skipped.
- Aggregator: 186 passed, 5 skipped; compile check passed.
- Root: 338 passed, 9 skipped. Separate scripts gate: 266 passed, 9 skipped.
- Earlier verification of these changes: full local Compose restart and health
  checks, plus the operator journey E2E run (22 harness tests and 1 Playwright
  test passed). External infrastructure tests skipped by the unit suites are not
  claimed as passing.
- Encoded video metadata and a decoded completed-state frame were checked.

This is review evidence for the shared EdgeCitadel runtime changes. The existing
jim-eq Core/Leaf deployment supplied the live environment.
