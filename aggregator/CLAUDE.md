# Aggregator Guide

## Scope

- This directory contains the FastAPI backend, NATS integration, and SQLite persistence layer.
- Primary entrypoints are `main.py`, `aggregator.py`, `database.py`, and `models.py`.

## Local Rules

- Keep changes consistent with the current lightweight structure: direct modules, no ORM, sync SQLite access.
- Preserve the subject naming and transport assumptions already documented in the code and repo docs.
- Avoid incidental refactors across unrelated endpoints, parsers, and DB helpers.
- When backend changes alter API shape, message schema, or runtime assumptions, update the relevant docs and call out any required frontend or e2e follow-up.

## Commands

- Install contributor deps: `pip install -r requirements-dev.txt`
- Run dev server: `uvicorn main:app --host 0.0.0.0 --port 8000 --reload`
- Syntax check: `python3 -m py_compile *.py`

## Validation

- For Python changes, run `python3 -m py_compile *.py`.
- For backend behavior changes, prefer a stack-backed smoke check in addition to syntax validation when the environment is available.
- If behavior depends on NATS or Docker services, mention any runtime checks you could not execute.
