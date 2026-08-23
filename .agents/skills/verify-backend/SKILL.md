---
name: verify-backend
description: Use when verifying changes to aggregator/ — Python syntax check plus runtime smoke when the stack is available.
---

# Verify a backend change

## Steps

1. Syntax check all Python in aggregator:
   ```bash
   cd aggregator && python3 -m py_compile *.py
   ```
   Must complete with no errors.

2. If the change touches API behavior, NATS subscriptions, or persistence, run the runtime smoke when the stack is available:
   ```bash
   curl http://localhost:8222/healthz
   curl http://localhost/api/system/status
   ```
   Both must return 2xx.

3. If the change touches messaging contracts (subjects, payloads), confirm `docs/05-messaging.md` was updated in the same PR.

## Rules

- If the stack is not running, syntax check is the minimum. Say so explicitly — do not claim runtime correctness without runtime evidence.
- For changes that only affect internal helpers (no API, no messaging, no persistence), syntax check is sufficient.
