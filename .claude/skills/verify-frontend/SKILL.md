---
name: verify-frontend
description: Use when verifying changes that touch frontend/ — runs production build and targeted Playwright spec, with explicit fallbacks if the e2e environment is unavailable.
---

# Verify a frontend change

## Steps

1. Run the production build:
   ```bash
   cd frontend && npm run build
   ```
   Build must succeed with zero errors.

2. Run targeted Playwright coverage for the affected feature:
   ```bash
   cd e2e && npm test -- <relevant spec or feature pattern>
   ```
   For broader changes (multiple components, routing, layout), run the full suite: `cd e2e && npm test`.

3. If the change is visible in the UI, open the affected page in a browser and confirm visually. Test the golden path and one edge case.

## Rules

- Curl-only checks are NOT sufficient for UI changes. Playwright is the gate.
- If the e2e environment is not available (test stack not running, network unavailable), say so explicitly. Do not claim success based on build alone.
- If you cannot run a browser, say so explicitly. Type checking and build success do not verify feature correctness.
