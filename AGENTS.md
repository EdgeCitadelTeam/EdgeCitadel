# Engineering in EdgeCitadel

## Reason from the actual problem
- Establish the intended behavior, then read the relevant implementation and tests.
  Distinguish observed facts from assumptions; investigate the uncertainty that
  could change the solution rather than surveying the whole repository.
- Fix the cause at the layer that owns the behavior. When designs differ materially,
  compare their complexity, failure behavior and maintenance cost before choosing.
- Solve current requirements. Add an abstraction, dependency or configuration option
  only when it removes concrete complexity or supports an actual use case.
- Make routine decisions independently. Ask when a material product/design choice
  remains unresolved. A short explanation is enough for most changes.

## Design for clarity
- Give each module a coherent responsibility and each piece of state a clear owner.
  Keep business rules close to that owner; avoid duplicate representations that
  need to be synchronized. Derive values when storing them adds no benefit.
- Prefer direct data flow and explicit interfaces. Reuse a sound existing pattern,
  but simplify an awkward one instead of extending it with another special case.
- Backward compatibility is not a project requirement. Update current callers,
  producers, consumers, schemas and tests together; remove obsolete paths instead
  of adding compatibility shims, dual implementations or legacy fallbacks.
- Keep changes to one invariant in one transaction where atomicity is required.
  Make ownership, cancellation and cleanup of connections/tasks explicit. Add
  retries only for identified transient failures and account for repeated effects.
- Optimize measured bottlenecks. Introduce concurrency, caching or batching when
  the workload justifies the extra state and failure modes.

## Write readable code
- Use descriptive names, straightforward control flow and cohesive functions.
  Extract helpers when they clarify a responsibility or meaningful reuse, not
  merely to move a few lines elsewhere. Avoid generic utility dumping grounds.
- Follow the surrounding Python/React style and let Ruff/ESLint handle mechanics.
  Use types to clarify interfaces and data shapes; avoid broad suppressions or
  unchecked casts that hide a design problem.
- Validate external input at its boundary. Handle expected failures specifically;
  propagate unexpected failures rather than hiding them behind successful defaults.
  Diagnostics should explain the failure without leaking credentials or payloads.
- Explain non-obvious decisions and invariants in comments. Prefer clearer code to
  narration, and remove comments, branches and tests made obsolete by the change.
- In React, keep state with its owner and compute derived values directly; avoid
  effects whose only purpose is keeping duplicate state in sync.

## Test useful behavior
- Check existing coverage before adding tests. Test observable behavior and failure
  boundaries; avoid tests that merely repeat constants or mirror implementation.
- Use the smallest test that catches the regression. Exercise a real integration
  boundary when a mock would conceal the relevant failure.
- Run affected tests first and broaden for shared behavior or failures. Reuse valid
  results while code, dependencies and relevant configuration/environment are unchanged.
- For prose edits, review content and links; check executable examples when changed.
  Report meaningful verification gaps instead of treating skips as passes.

## Working references
- Setup, commands and repository map: `CONTRIBUTING.md`.
- Detailed verification recipes: `.agents/skills/`; runtime/packages: `agent-runtime/README.md`.
- Keep credentials and local runtime data out of committed code. Update affected
  user documentation and `.env.example` when behavior or configuration changes.
