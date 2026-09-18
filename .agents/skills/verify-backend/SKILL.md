---
name: verify-backend
description: Verify changed backend behavior with focused tests; use integration checks when the affected boundary requires them.
---

# Backend verification

- Run the relevant tests in `aggregator/tests`; expand to the suite when shared
  backend behavior changes. Tests that import the changed modules cover syntax.
- For API changes, use the affected API tests. For NATS or persistence changes,
  include the relevant broker or database integration test.
- Use a live smoke check only when service wiring/startup is part of the change.
  Prefer an owned fixture; do not restart a shared stack for an internal edit.
- Documentation-only edits need no runtime checks. Removing unused modules needs
  reference/caller inspection and appropriate regression tests, not a deployment.
- Report unavailable integration evidence without claiming it passed. Curl alone
  does not verify application behavior.
