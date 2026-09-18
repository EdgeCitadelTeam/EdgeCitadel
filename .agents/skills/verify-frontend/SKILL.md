---
name: verify-frontend
description: Verify frontend changes with scoped tests and build; exercise changed interactions in the browser when needed.
---

# Frontend verification

For frontend code changes, run `npm run lint`, the affected unit tests, and
`npm run build` from `frontend/`. Documentation-only changes need none of these.

For changed user interactions or page composition, run the relevant Playwright
spec from `e2e/` (`npm test -- <spec>`). Inspect visible layout changes in a browser.
Broaden to the full suite for shared navigation or application-wide behavior.
A unit-only internal change does not automatically need browser testing.

Reuse checks for unchanged frontend code. If browser/integration testing is
unavailable, state the unverified behavior; a build or curl response is not proof
of UI correctness.
