We are beginning work on a new feature or change. Before writing any code:

**Topic:** $ARGUMENTS

## Phase 1: Research
- Read ALL files relevant to this change. Understand the current implementation deeply.
- Use subagents for broad codebase exploration if needed.
- Output findings to `thoughts/research/$(date +%Y%m%d)-research.md`.

## Phase 2: Design Options
Propose 2-3 implementation approaches. For each:
- What changes to which files (be specific — file paths and line ranges)
- Tradeoffs: complexity, performance, breaking changes, test impact
- Impact on NATS subject structure and message schemas (if any)
- Estimated number of files touched

## Phase 3: Recommended Plan
Write a detailed plan to `thoughts/plans/$(date +%Y%m%d)-plan.md` with:
- One-sentence goal statement
- Step-by-step implementation phases (each phase should be independently testable)
- Files to modify with specific changes described
- New files to create (if any — prefer editing existing files)
- Test strategy: what to test, how to verify
- Rollback approach: what to revert if something breaks
- Risks and open questions

## Phase 4: Present for Review
- Summarize the recommended approach in 3-5 bullet points
- Call out any assumptions that need human verification
- **DO NOT IMPLEMENT YET.** Wait for explicit approval before writing any code.
