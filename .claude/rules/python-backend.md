---
paths:
  - "aggregator/**/*.py"
  - "tests/**/*.py"
  - "scripts/**/*.py"
---

# Python Backend Rules

## Style
- Python 3.12+ features allowed (match-case, type unions with `|`, etc.)
- Type annotations required on all function signatures
- Use `ruff` for linting and formatting (not black, not isort separately)
- f-strings preferred over `.format()` or `%` formatting

## FastAPI Patterns
- All endpoints must have `summary` and `description` in decorator
- Use Pydantic models for request/response bodies (never raw dicts)
- Return consistent error format: `{"error": str, "detail": str, "status": int}`
- Use `HTTPException` for error responses, not manual Response objects
- Dependency injection for database and NATS connections

## Database (SQLite)
- Sync operations only — module-level connection in `database.py`
- NEVER use from multiple threads
- ALWAYS use parameterized queries: `cursor.execute("SELECT * FROM t WHERE id=?", (id,))`
- NEVER use f-strings or string concatenation in SQL
- Add indices for columns used in WHERE clauses

## NATS (async nats-py)
- Subscriptions run as `asyncio.create_task()` in FastAPI startup
- Use `await nc.publish()` — never blocking publish
- Handle disconnection/reconnection gracefully
- Always JSON-encode payloads: `json.dumps(payload).encode()`
- Include `correlation_id` in request-reply patterns

## Testing
- Use `pytest` with `pytest-asyncio` for async tests
- Prefer integration tests over unit tests for NATS message flows
- Test file naming: `test_<module>.py`
- Use fixtures for database setup/teardown
