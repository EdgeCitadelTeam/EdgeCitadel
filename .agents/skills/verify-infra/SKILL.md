---
name: verify-infra
description: Verify deployment and shared configuration changes at their affected boundary; documentation and instruction edits need no stack.
---

# Infrastructure verification

Choose the check for the actual change:

- Instructions, plans and workflow documentation: review content and referenced
  commands/paths. Do not start Docker or run application suites.
- Config renderers or deployment helpers: run their focused tests and validate
  the rendered configuration.
- Compose, nginx, NATS or service-startup behavior: exercise the affected path in
  an owned stack. Use `e2e`'s disposable runner when a Playwright workflow applies;
  it builds, waits for health and cleans up its own resources.
- Shared end-to-end behavior: broaden to the relevant integration suites. Use the
  full Playwright suite when the change affects the whole application.

Do not run `docker compose down` against a shared stack just to verify an edit.
A separate restart is unnecessary when the owned runner already rebuilt and
started the affected services. Health endpoints are useful diagnostics, not a
replacement for testing the changed behavior. State any environment limitation
and the behavior left unverified.
