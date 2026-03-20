# Frontend Guide

## Scope

- This is the active dashboard UI.
- Use `src/` here for all UI changes. The runtime service is still named `dashboard`, but it is backed by this directory.

## Local Rules

- Keep React and Vite patterns consistent with the existing codebase.
- Prefer focused component or store updates over sweeping UI rewrites.
- Preserve the current API and WebSocket integration shape unless the task requires coordinated backend changes.
- If UI changes depend on new backend fields, routes, or socket events, keep the interface contract explicit and update related tests or docs.

## Commands

- Install deps: `npm install`
- Dev server: `npm run dev`
- Production build: `npm run build`

## Validation

- Run `npm run build` after frontend changes.
- Run actual Playwright coverage for frontend-affecting changes. Prefer targeted specs first, but do not stop at build-only verification.
- If UI behavior should be covered end-to-end, note and run the relevant `e2e/` tests.
- For workflow or config changes that affect local dev or deployment behavior, follow the root-level stack restart and smoke-check policy.
