Review recent changes for correctness, quality, and completeness.

**Scope:** $ARGUMENTS

## Review Checklist

1. **Read the diff**: Examine every changed file. Understand what changed and why.
2. **Correctness**: Does the code do what it claims? Are there edge cases missed?
3. **NATS contract**: If NATS subjects or message schemas changed, verify all publishers and subscribers are updated consistently.
4. **Database**: If SQLite schema changed, verify migrations and backward compatibility.
5. **Security**: No hardcoded secrets, no injection vectors, no auth bypasses.
6. **Tests**: Are new behaviors covered by tests? Do existing tests still pass?
7. **Simplicity**: Is there a simpler way to achieve the same result? Any unnecessary abstractions?
8. **Gotchas**: Check against the CLAUDE.md gotchas list — sync SQLite, no thread bridging, nginx prefix stripping, etc.

## Output
- List of issues found (severity: critical/warning/nit)
- Suggested fixes for each issue
- Overall assessment: ship / fix-then-ship / rethink
