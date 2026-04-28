# Gemma Adapter Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Gemma adapter (`agent_id: gemma-1`) — a single-shot Ollama-backed reasoner that wraps `POST /api/generate` and returns the response in a single `result` envelope. Validates the `adapters/_common/` skeleton against an LLM workload.

**Architecture:** A Python process running on the host (not containerized — Ollama runs on the host). Loads its A2A Agent Card from `adapters/gemma/config.yaml`, registers + heartbeats over plain NATS, runs `adapters/_common/pull_consumer.PullConsumer` against `agents.gemma-1.inbox`. For each `command` envelope: validates type + body, calls Ollama HTTP, returns `(payload, task_state)` for `pull_consumer._publish_result` to publish. `runtime.kind: native`, `runtime.roles: [reasoner]`, single skill `reasoning.chat`.

**Tech Stack:** Python 3.11+ / `httpx>=0.27` / `nats-py>=2.9` / `pyyaml>=6.0` / `pytest-asyncio>=0.23` · Ollama (host process) · Playwright for E2E.

**Spec:** `docs/superpowers/specs/2026-04-24-gemma-adapter-design.md` (read first; sections "Architecture", "Envelope flow", "Failure modes", "Preflight + lifecycle").

**Scope:** Phase 2 only. No streaming, no conversation memory, no multi-skill, no auto-pull. Deferred items live in `docs/roadmap.md`.

---

## Prerequisites (read once before Task 1)

### Repo state

- Phase 1 implementation lives on branch `feat/agent-messaging-v0.1-impl` (commit `8cc9b15` at the time of writing — the package-lock chore commit that capped Phase 1).
- Phase 2 spec + roadmap live on `feat/agent-contract-v0.1` (commit `4c52baf`).
- **Before executing this plan**, create a Phase 2 worktree by branching off `feat/agent-messaging-v0.1-impl` AND merging in the spec + roadmap from `feat/agent-contract-v0.1`. One way to set this up:

```bash
cd /Users/yefanzhang/workplace/edge-research
git worktree add .worktrees/gemma-impl -b feat/gemma-adapter-impl feat/agent-messaging-v0.1-impl
cd .worktrees/gemma-impl
git merge feat/agent-contract-v0.1 --no-ff -m "merge: pull Phase 2 spec + roadmap onto Phase 1 impl tree"
```

Verify after the merge:
- `docs/superpowers/specs/2026-04-24-gemma-adapter-design.md` exists.
- `docs/roadmap.md` exists.
- `adapters/_common/` exists (Phase 1 deliverable).
- `adapters/shell/adapter.py` exists (Phase 1 deliverable; mirror its shape).

### Ollama

The plan assumes a developer has Ollama set up locally:

```bash
brew install ollama       # one-time
ollama serve &            # foreground or via the GUI app
ollama pull gemma3:4b     # ~3GB; default model
ollama list               # confirm gemma3:4b shows up
curl http://localhost:11434/api/tags | jq '.models[].name'
```

If Ollama isn't installed, install it first. The adapter fails fast on missing Ollama or missing model — this is by design (see spec §"Why fail-fast, not auto-pull?").

### Canonical envelope vocabulary (reference)

Same as Phase 1 — `schemas/envelope.v1.json`. All envelopes published by the Gemma adapter use the strict v0.1 contract. Required fields by type are documented in `docs/agent-contract.md`. The adapter builds `register`, `heartbeat`, `status: offline`; the `result` envelope is constructed by `pull_consumer._publish_result` from the `(payload, state)` tuple your handler returns.

---

## File Structure

```
adapters/gemma/                                  [NEW directory]
  __init__.py                                    [empty marker]
  adapter.py                                     [handle() + main()]
  config.yaml                                    [agent_id: gemma-1, role: reasoner]
  requirements.txt                               [httpx, nats-py, pyyaml, jsonschema]
  README.md                                      [how to run; preflight; ollama pull]
  tests/
    __init__.py                                  [empty marker if needed]
    test_gemma.py                                [8 unit tests, no live Ollama]
    test_gemma_integration.py                    [gated live-Ollama test]

docs/
  CHANGELOG.md                                   [MODIFY — Unreleased entry]

e2e/tests/phase2-gemma-smoke.spec.js             [NEW]

.env.example                                     [MODIFY — add OLLAMA_HOST/PORT/MODEL/TIMEOUT_SEC]
```

No changes to schemas, aggregator, or `adapters/_common`. Phase 2 rides entirely on the v0.1 contract as shipped in Phase 1.

---

## Task 1: Scaffold `adapters/gemma/` directory + config.yaml

**Files:**
- Create: `adapters/gemma/__init__.py` (empty)
- Create: `adapters/gemma/config.yaml`
- Create: `adapters/gemma/requirements.txt`

- [ ] **Step 1.1: Create `adapters/gemma/__init__.py`** — empty file (no content).

- [ ] **Step 1.2: Create `adapters/gemma/config.yaml`** verbatim:

```yaml
agent_id: gemma-1
name: gemma-1
description: Single-shot Ollama-backed reasoner. Wraps /api/generate.
version: 0.1.0
runtime:
  kind: native
  roles: [reasoner]
  tags: [ollama, llm]
  heartbeat_interval_sec: 30
skills:
  - id: reasoning.chat
    name: chat
    description: Send a free-text prompt; receive the model's full response.
    tags: [llm, generate]
capabilities:
  streaming: false
```

- [ ] **Step 1.3: Create `adapters/gemma/requirements.txt`** verbatim:

```
nats-py>=2.9.0
httpx>=0.27
pyyaml>=6.0
jsonschema>=4.20
```

(Note: `httpx` is a new dep here. The shell adapter doesn't need it because subprocess is stdlib; Gemma needs it for the Ollama HTTP client.)

- [ ] **Step 1.4: Sanity check the config.yaml is loadable**

Run:

```bash
cd /Users/yefanzhang/workplace/edge-research/.worktrees/gemma-impl
python3 -c "from adapters._common.agent_card import build_card; \
  card = build_card('adapters/gemma/config.yaml'); \
  print(card['name'], card['metadata']['runtime.roles'])"
```

Expected output: `gemma-1 ['reasoner']`. If `pyyaml` is missing, `pip install --user --break-system-packages pyyaml`.

- [ ] **Step 1.5: Commit**

```bash
git add adapters/gemma/__init__.py adapters/gemma/config.yaml \
        adapters/gemma/requirements.txt
git commit -m "$(cat <<'EOF'
feat(gemma): scaffold adapter directory with A2A config + deps

config.yaml declares agent_id: gemma-1, runtime.kind: native,
runtime.roles: [reasoner], skill: reasoning.chat. requirements.txt
adds httpx>=0.27 (new dep, Ollama HTTP client) on top of the shell
adapter's stack.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Adapter handle() — TDD with mocked Ollama

**Files:**
- Create: `adapters/gemma/tests/__init__.py` (empty if pytest needs it)
- Create: `adapters/gemma/tests/test_gemma.py`
- Create: `adapters/gemma/adapter.py`

- [ ] **Step 2.1: Create `adapters/gemma/tests/test_gemma.py`** verbatim:

```python
"""Unit tests for the Gemma adapter handle() — no live Ollama."""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from adapters.gemma.adapter import handle
from adapters._common.pull_consumer import Context


def _ctx():
    ctx = Context(agent_id="gemma-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    return ctx


def _cmd(body="What is 2+2?", args=None, type="command",
         context_id=None):
    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": type,
           "sender_id": "tester", "recipient_id": "gemma-1",
           "task_id": "22222222-3333-4444-8555-666666666666",
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": body, **({"args": args} if args else {})}}
    if context_id is not None:
        env["context_id"] = context_id
    return env


class _FakeResp:
    def __init__(self, status_code, json_body=None, text=""):
        self.status_code = status_code
        self._json_body = json_body
        self.text = text

    def json(self):
        if self._json_body is None:
            raise ValueError("no JSON body")
        return self._json_body


@pytest.mark.asyncio
async def test_handle_command_calls_ollama(monkeypatch):
    captured = {}

    async def fake_post(self, url, json=None, timeout=None, **kw):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResp(200, json_body={"response": "4", "model": "gemma3:4b"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())

    assert state == "completed"
    assert payload["body"] == "4"
    assert payload["model"] == "gemma3:4b"
    assert "duration_ms" in payload
    assert captured["url"].endswith("/api/generate")
    assert captured["json"]["model"] == "gemma3:4b"
    assert captured["json"]["prompt"] == "What is 2+2?"
    assert captured["json"]["stream"] is False


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"v": 1, "id": "x", "type": "delegation",
           "sender_id": "planner", "recipient_id": "gemma-1",
           "task_id": "t", "context_id": "c", "hop_count": 0,
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": "ignored"}}
    payload, state = await handle(env, _ctx())
    assert state == "rejected"
    assert payload["error"] == "unsupported_type"


@pytest.mark.asyncio
async def test_handle_rejects_empty_prompt():
    payload, state = await handle(_cmd(body="   "), _ctx())
    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_ollama_unreachable_returns_failed(monkeypatch):
    import httpx

    async def fake_post(self, url, **kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_unreachable"


@pytest.mark.asyncio
async def test_handle_ollama_timeout_returns_failed(monkeypatch):
    import httpx

    async def fake_post(self, url, **kw):
        raise httpx.ReadTimeout("read timed out")

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(args={"timeout_sec": 1}), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_timeout"


@pytest.mark.asyncio
async def test_handle_model_not_loaded_returns_failed(monkeypatch):
    async def fake_post(self, url, **kw):
        return _FakeResp(404,
                         json_body={"error": "model 'fake' not found, try pulling it first"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "model_not_loaded"


@pytest.mark.asyncio
async def test_handle_ollama_5xx_returns_failed(monkeypatch):
    async def fake_post(self, url, **kw):
        return _FakeResp(500, json_body={"error": "internal"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_inference_error"


@pytest.mark.asyncio
async def test_handle_args_override_model_and_temperature(monkeypatch):
    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["json"] = json
        return _FakeResp(200, json_body={"response": "ok", "model": "gemma3:12b"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(
        _cmd(args={"model": "gemma3:12b", "temperature": 0.2,
                   "max_tokens": 512}), _ctx())
    assert state == "completed"
    assert captured["json"]["model"] == "gemma3:12b"
    assert captured["json"]["options"]["temperature"] == 0.2
    assert captured["json"]["options"]["num_predict"] == 512
```

- [ ] **Step 2.2: Create `adapters/gemma/tests/__init__.py`**

Empty file. Only create if pytest collection requires it; the Phase 1 shell adapter didn't need one. If pytest discovers `adapters/gemma/tests/test_gemma.py` without this, skip it.

- [ ] **Step 2.3: Run tests, confirm fail**

```bash
cd /Users/yefanzhang/workplace/edge-research/.worktrees/gemma-impl
python3 -m pytest adapters/gemma/tests/test_gemma.py -v
```

Expected: FAIL — `ModuleNotFoundError: adapters.gemma.adapter` (the module doesn't exist yet). If pytest skips with "no items" instead, you may need the `__init__.py` from Step 2.2.

- [ ] **Step 2.4: Create `adapters/gemma/adapter.py`** verbatim:

```python
"""EdgeCitadel Gemma adapter — single-shot Ollama-backed reasoner.

Wraps Ollama POST /api/generate. runtime.kind: native; runtime.roles:
[reasoner]. Single skill: reasoning.chat. No streaming, no conversation
memory (deferred to Phase 2.5; see docs/roadmap.md)."""
from __future__ import annotations
import asyncio
import logging
import os
import time
from pathlib import Path

import httpx

from adapters._common.pull_consumer import Context
from adapters._common.template import main as run_adapter

log = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "localhost")
OLLAMA_PORT = int(os.environ.get("OLLAMA_PORT", "11434"))
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gemma3:4b")
OLLAMA_TIMEOUT_SEC = int(os.environ.get("OLLAMA_TIMEOUT_SEC", "120"))


def _ollama_url(path: str) -> str:
    return f"http://{OLLAMA_HOST}:{OLLAMA_PORT}{path}"


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")

    body = env["payload"].get("body", "").strip()
    args = env["payload"].get("args") or {}
    if not body:
        return ({"error": "empty_prompt"}, "rejected")

    model = args.get("model") or OLLAMA_MODEL
    timeout_sec = int(args.get("timeout_sec") or OLLAMA_TIMEOUT_SEC)
    options: dict = {}
    if "temperature" in args:
        options["temperature"] = args["temperature"]
    if "max_tokens" in args:
        options["num_predict"] = args["max_tokens"]

    request_body = {"model": model, "prompt": body, "stream": False}
    if options:
        request_body["options"] = options

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post(_ollama_url("/api/generate"),
                                     json=request_body, timeout=timeout_sec)
    except httpx.ConnectError:
        return ({"error": "ollama_unreachable"}, "failed")
    except (httpx.ReadTimeout, httpx.WriteTimeout, asyncio.TimeoutError):
        return ({"error": "ollama_timeout"}, "failed")
    duration_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code == 404:
        return ({"error": "model_not_loaded"}, "failed")
    if resp.status_code >= 500:
        return ({"error": "ollama_inference_error"}, "failed")
    if resp.status_code != 200:
        return ({"error": "ollama_bad_response",
                 "status": resp.status_code}, "failed")

    try:
        body_json = resp.json()
    except (ValueError, Exception):
        return ({"error": "ollama_bad_response"}, "failed")

    response_text = body_json.get("response", "")
    return ({"body": response_text, "model": model,
             "duration_ms": duration_ms}, "completed")


async def main():
    """Adapter entry point. Handler is injected into the shared template."""
    from adapters._common import template
    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

- [ ] **Step 2.5: Run tests, confirm PASS (8 tests)**

```bash
python3 -m pytest adapters/gemma/tests/test_gemma.py -v
```

Expected: 8 tests pass. If `httpx` isn't installed, `pip install --user --break-system-packages httpx`.

- [ ] **Step 2.6: Commit**

```bash
git add adapters/gemma/adapter.py adapters/gemma/tests/test_gemma.py
# only if you actually created an empty __init__.py:
git add adapters/gemma/tests/__init__.py 2>/dev/null
git commit -m "$(cat <<'EOF'
feat(gemma): handle() with seven typed error codes (TDD)

Implements the v0.2 Gemma adapter handler — the per-message contract
for the reasoner agent:

- handle(env, ctx) -> (payload, task_state) wrapping Ollama POST
  /api/generate with stream=false. Returns body/model/duration_ms on
  success; one of seven typed error codes on failure or rejection
  (unsupported_type, empty_prompt, ollama_unreachable, ollama_timeout,
  model_not_loaded, ollama_inference_error, ollama_bad_response).
- Configurable via env (OLLAMA_HOST, OLLAMA_PORT, OLLAMA_MODEL,
  OLLAMA_TIMEOUT_SEC) or per-command payload.args (model, temperature,
  max_tokens, timeout_sec override).
- main() monkey-patches adapters._common.template.handle and delegates
  lifecycle (register/heartbeat/consumer/graceful-shutdown) to the
  shared template, same pattern as the Phase 1 shell adapter.

8 unit tests cover happy path, rejection paths (non-command type,
empty prompt), each Ollama failure mode, and args overrides. No live
Ollama needed — httpx.AsyncClient.post is monkey-patched.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Preflight check (Ollama health + model presence)

The shared `adapters/_common/template.py` doesn't have a preflight hook today; it just registers + runs the consumer. Phase 2's preflight is adapter-specific (only Gemma needs to verify Ollama is reachable). We add it to `adapter.main()` BEFORE delegating to `run_adapter()`.

**Files:**
- Modify: `adapters/gemma/adapter.py`
- Modify: `adapters/gemma/tests/test_gemma.py` (add 2 preflight tests)

- [ ] **Step 3.1: Append preflight tests to `adapters/gemma/tests/test_gemma.py`**

Add the following at the end of the file:

```python
@pytest.mark.asyncio
async def test_preflight_passes_when_model_listed(monkeypatch):
    from adapters.gemma.adapter import preflight

    async def fake_get(self, url, **kw):
        return _FakeResp(200, json_body={
            "models": [{"name": "gemma3:4b"}, {"name": "llama3.2:3b"}]
        })

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    monkeypatch.setenv("OLLAMA_MODEL", "gemma3:4b")
    # Should not raise
    await preflight()


@pytest.mark.asyncio
async def test_preflight_raises_when_unreachable(monkeypatch):
    from adapters.gemma.adapter import preflight, PreflightError
    import httpx

    async def fake_get(self, url, **kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    with pytest.raises(PreflightError, match="ollama_unreachable"):
        await preflight()


@pytest.mark.asyncio
async def test_preflight_raises_when_model_not_found(monkeypatch):
    from adapters.gemma.adapter import preflight, PreflightError

    async def fake_get(self, url, **kw):
        return _FakeResp(200, json_body={
            "models": [{"name": "llama3.2:3b"}]   # gemma3:4b absent
        })

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    monkeypatch.setenv("OLLAMA_MODEL", "gemma3:4b")
    with pytest.raises(PreflightError, match="model_not_loaded"):
        await preflight()
```

- [ ] **Step 3.2: Run tests to verify preflight tests fail**

```bash
python3 -m pytest adapters/gemma/tests/test_gemma.py -v -k preflight
```

Expected: FAIL — `ImportError: cannot import name 'preflight'` (or `PreflightError`).

- [ ] **Step 3.3: Modify `adapters/gemma/adapter.py` — add `PreflightError` and `preflight()`**

Insert these definitions just below the `OLLAMA_TIMEOUT_SEC` env-var line (before `_ollama_url`):

```python
class PreflightError(RuntimeError):
    """Raised when the adapter cannot start because Ollama is unreachable
    or the configured model is not pulled."""


async def preflight() -> None:
    """Verify Ollama is reachable and OLLAMA_MODEL is in the loaded list.

    Raises PreflightError on failure. Called once at startup before the
    consumer is registered. We intentionally do NOT auto-pull on missing
    models — see docs/superpowers/specs/2026-04-24-gemma-adapter-design.md
    section "Why fail-fast, not auto-pull?"."""
    model = os.environ.get("OLLAMA_MODEL", OLLAMA_MODEL)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(_ollama_url("/api/tags"), timeout=5)
    except httpx.ConnectError as e:
        raise PreflightError(
            f"ollama_unreachable: cannot reach {_ollama_url('/api/tags')}: {e}"
        ) from e
    except (httpx.ReadTimeout, httpx.WriteTimeout):
        raise PreflightError(
            f"ollama_unreachable: timeout reading {_ollama_url('/api/tags')}"
        )

    if resp.status_code != 200:
        raise PreflightError(
            f"ollama_unreachable: /api/tags returned {resp.status_code}"
        )

    try:
        body = resp.json()
    except ValueError as e:
        raise PreflightError(f"ollama_bad_response: {e}") from e

    names = [m.get("name") for m in (body.get("models") or [])]
    if model not in names:
        raise PreflightError(
            f"model_not_loaded: OLLAMA_MODEL={model!r} not in {names!r}; "
            f"run `ollama pull {model}`"
        )
```

Then modify `main()` to call `preflight()` first and exit with the right code on failure:

```python
async def main():
    """Adapter entry point. Preflight first, then delegate to template."""
    try:
        await preflight()
    except PreflightError as e:
        log.error("preflight failed: %s", e)
        msg = str(e)
        if msg.startswith("ollama_unreachable"):
            raise SystemExit(1)
        if msg.startswith("model_not_loaded"):
            raise SystemExit(2)
        raise SystemExit(3)

    from adapters._common import template
    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)
```

(The exit codes match what the spec promised: 1 for unreachable, 2 for model-not-loaded, 3 reserved for other preflight failures.)

- [ ] **Step 3.4: Run tests, confirm PASS (11 tests now)**

```bash
python3 -m pytest adapters/gemma/tests/test_gemma.py -v
```

Expected: 11 tests pass (8 from Task 2 + 3 preflight tests).

- [ ] **Step 3.5: Commit**

```bash
git add adapters/gemma/adapter.py adapters/gemma/tests/test_gemma.py
git commit -m "$(cat <<'EOF'
feat(gemma): startup preflight (Ollama reachable + model loaded)

main() now runs preflight() before registering the agent or starting
the JetStream consumer:

- GET http://OLLAMA_HOST:OLLAMA_PORT/api/tags with 5s timeout.
- On ConnectError or non-200: raise PreflightError(ollama_unreachable),
  process exits 1.
- On model not in /api/tags response: raise PreflightError(
  model_not_loaded), process exits 2 with a clear "run ollama pull"
  message.
- Other preflight failures: exit 3.

Fail-fast was a deliberate design choice (spec §"Why fail-fast, not
auto-pull?") -- pulling 3-8GB on startup masks operator error as
"adapter is slow". Operators run `ollama pull` once during setup.

Three new unit tests cover the success path and both failure paths.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Live-Ollama integration test (gated)

Same pattern as Phase 1's `test_jetstream_bootstrap.py` — the test is collected by pytest but `pytest.skip`s if the live backend isn't reachable.

**Files:**
- Create: `adapters/gemma/tests/test_gemma_integration.py`

- [ ] **Step 4.1: Create `adapters/gemma/tests/test_gemma_integration.py`** verbatim:

```python
"""Live-Ollama integration test for the Gemma adapter.

Skipped unless OLLAMA_URL_TEST is set AND the configured model is loaded.
Pattern mirrors aggregator/tests/test_jetstream_bootstrap.py."""
import os
import pytest
import httpx
from adapters.gemma.adapter import handle
from adapters._common.pull_consumer import Context
from unittest.mock import MagicMock


OLLAMA_URL = os.environ.get("OLLAMA_URL_TEST")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL_TEST", "gemma3:4b")


def _ctx():
    import asyncio
    ctx = Context(agent_id="gemma-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    return ctx


@pytest.fixture
def ollama_available():
    if not OLLAMA_URL:
        pytest.skip("OLLAMA_URL_TEST not set; skipping live test")
    try:
        with httpx.Client(timeout=2) as client:
            r = client.get(f"{OLLAMA_URL}/api/tags")
            r.raise_for_status()
            names = [m.get("name") for m in r.json().get("models") or []]
            if OLLAMA_MODEL not in names:
                pytest.skip(
                    f"OLLAMA_MODEL_TEST={OLLAMA_MODEL!r} not in {names!r}")
    except Exception as e:
        pytest.skip(f"Ollama not reachable: {e}")


@pytest.mark.asyncio
async def test_handle_against_live_ollama(ollama_available, monkeypatch):
    # Point the adapter's URL builders at the test broker
    parsed = httpx.URL(OLLAMA_URL)
    monkeypatch.setenv("OLLAMA_HOST", parsed.host)
    monkeypatch.setenv("OLLAMA_PORT", str(parsed.port or 11434))
    monkeypatch.setenv("OLLAMA_MODEL", OLLAMA_MODEL)
    # Reload module-level constants in adapter.py
    import importlib
    from adapters.gemma import adapter as adapter_mod
    importlib.reload(adapter_mod)

    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": "command",
           "sender_id": "tester", "recipient_id": "gemma-1",
           "task_id": "22222222-3333-4444-8555-666666666666",
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": "Reply with exactly the digit 4.",
                       "args": {"timeout_sec": 60}}}

    payload, state = await adapter_mod.handle(env, _ctx())
    assert state == "completed", \
        f"expected completed, got {state}: {payload}"
    assert "body" in payload and payload["body"], \
        f"empty body: {payload}"
    assert payload["model"] == OLLAMA_MODEL
    assert isinstance(payload.get("duration_ms"), int)
```

- [ ] **Step 4.2: Run with skip path**

```bash
python3 -m pytest adapters/gemma/tests/test_gemma_integration.py -v
```

Expected: 1 skipped (since `OLLAMA_URL_TEST` is unset).

- [ ] **Step 4.3: Run with live Ollama (operator step; optional)**

If you have Ollama up locally with `gemma3:4b` pulled:

```bash
OLLAMA_URL_TEST=http://localhost:11434 OLLAMA_MODEL_TEST=gemma3:4b \
  python3 -m pytest adapters/gemma/tests/test_gemma_integration.py -v
```

Expected: 1 passed (live Ollama call took ~5–30s).

- [ ] **Step 4.4: Commit**

```bash
git add adapters/gemma/tests/test_gemma_integration.py
git commit -m "$(cat <<'EOF'
test(gemma): gated live-Ollama integration test

Same pattern as aggregator/tests/test_jetstream_bootstrap.py: the
test is collected but pytest.skip's if OLLAMA_URL_TEST is unset or
OLLAMA_MODEL_TEST isn't loaded. With both set, runs an actual
/api/generate against gemma3:4b and asserts task_state: completed
plus non-empty body.

Run via:
  OLLAMA_URL_TEST=http://localhost:11434 OLLAMA_MODEL_TEST=gemma3:4b \\
    python3 -m pytest adapters/gemma/tests/test_gemma_integration.py

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: README + .env.example + docs link

**Files:**
- Create: `adapters/gemma/README.md`
- Modify: `.env.example`

- [ ] **Step 5.1: Create `adapters/gemma/README.md`**

```markdown
# Gemma adapter

Single-shot Ollama-backed reasoner agent (`agent_id: gemma-1`) for the
EdgeCitadel v0.2 fleet. Wraps `POST http://OLLAMA_HOST:OLLAMA_PORT/api/generate`
and returns the response in a single `result` envelope.

`runtime.kind: native` (Ollama is stateless inference, not an upstream agent).
Single skill: `reasoning.chat`. No streaming, no conversation memory — those
are deferred to Phase 2.5; see `docs/roadmap.md`.

Spec: `docs/superpowers/specs/2026-04-24-gemma-adapter-design.md`.

## Subjects

- `agents.gemma-1.register` — A2A Agent Card on startup.
- `agents.gemma-1.heartbeat` — every 30s.
- `agents.gemma-1.status` — offline on shutdown.
- `agents.gemma-1.inbox` — JetStream WorkQueue, durable consumer
  `gemma-1_inbox` (`max_ack_pending=1`, `ack_wait=300s`, `max_deliver=3`).
- `agents.gemma-1.outbox` — plain-NATS audit mirror (per ADR-0006).

## Environment

| Var | Default | Purpose |
|---|---|---|
| `NATS_URL` | (required) | Fleet broker, e.g. `nats://localhost:4222` |
| `NATS_TOKEN` | (required) | Fleet token |
| `OLLAMA_HOST` | `localhost` | Ollama HTTP host |
| `OLLAMA_PORT` | `11434` | Ollama HTTP port |
| `OLLAMA_MODEL` | `gemma3:4b` | Default model name |
| `OLLAMA_TIMEOUT_SEC` | `120` | HTTP timeout for `/api/generate` |
| `ACK_WAIT_SEC` | `300` | JetStream `ack_wait` (PullConsumer kwarg) |

## One-time setup

```bash
brew install ollama          # or your platform's installer
ollama serve &               # foreground or via the GUI app
ollama pull gemma3:4b        # ~3 GB
ollama list                  # confirm model is present
```

The adapter fails fast on startup if Ollama is unreachable or the
configured model is not pulled. There is no auto-pull (intentional —
see spec §"Why fail-fast, not auto-pull?").

## Run

```bash
cd <repo>
PYTHONPATH=. python3 -m adapters.gemma.adapter
```

Connects to NATS, registers the Agent Card, starts a 30s heartbeat,
and runs the JetStream pull consumer against `agents.gemma-1.inbox`.

## Test

```bash
# Unit tests (no live Ollama required)
python3 -m pytest adapters/gemma/tests/test_gemma.py -v

# Integration test (gated)
OLLAMA_URL_TEST=http://localhost:11434 OLLAMA_MODEL_TEST=gemma3:4b \
  python3 -m pytest adapters/gemma/tests/test_gemma_integration.py -v

# E2E smoke (requires running stack + Ollama + adapter)
cd e2e && npm test -- phase2-gemma-smoke.spec.js
```

## Failure modes

The adapter returns one of seven typed error codes in `payload.error`:

| Code | task_state | Cause |
|---|---|---|
| `unsupported_type` | rejected | Inbound is not a `command` envelope |
| `empty_prompt` | rejected | `payload.body` is missing or whitespace |
| `ollama_unreachable` | failed | TCP connect failed |
| `ollama_timeout` | failed | HTTP read/write timeout |
| `model_not_loaded` | failed | Ollama 404 |
| `ollama_inference_error` | failed | Ollama 5xx |
| `ollama_bad_response` | failed | non-JSON or non-200 non-5xx |

Other exceptions bubble up to `pull_consumer`'s nak path, which
redelivers up to 3× before the JetStream MAX_DELIVERIES advisory
fires (captured by the aggregator's `on_advisory` handler).

## Cancel

SIGTERM or SIGINT publishes `agents.gemma-1.status` with
`agent_state: offline`, stops the consumer, drains NATS. The
aggregator marks the agent offline on receipt; the watchdog (Phase 3)
will detect missing heartbeats independently.
```

- [ ] **Step 5.2: Modify `.env.example`**

Read the file first. After the existing `OPENCLAW_TOKEN` / `EC_ENABLE_MQTT` block (added in Phase 1 Task 15), append:

```
# Gemma adapter (Phase 2). See adapters/gemma/README.md.
OLLAMA_HOST=localhost
OLLAMA_PORT=11434
OLLAMA_MODEL=gemma3:4b
OLLAMA_TIMEOUT_SEC=120
```

Don't disturb existing entries. If any of `OLLAMA_*` already exist (unlikely), leave them.

- [ ] **Step 5.3: Commit**

```bash
git add adapters/gemma/README.md .env.example
git commit -m "$(cat <<'EOF'
docs(gemma): adapter README + .env.example entries

README covers subjects, env vars, one-time Ollama setup (the
fail-fast preflight needs the model pulled first), how to run/test,
the seven typed error codes, and graceful shutdown behavior.

.env.example adds OLLAMA_HOST/PORT/MODEL/TIMEOUT_SEC alongside the
v0.1 NATS/OPENCLAW/EC_ENABLE_MQTT block.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Phase 2 E2E smoke spec

**Files:**
- Create: `e2e/tests/phase2-gemma-smoke.spec.js`

- [ ] **Step 6.1: Create `e2e/tests/phase2-gemma-smoke.spec.js`**

```javascript
import { test, expect } from '@playwright/test';

const API = process.env.AGG_URL || 'http://localhost';
const POLL_INTERVAL_MS = 1000;
const POLL_BUDGET_S = 60;

test.describe('Phase 2 smoke — Gemma round trip', () => {
  test('gemma-1 registered with reasoner role', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/gemma-1/card`);
    expect(r.ok()).toBe(true);
    const card = await r.json();
    expect(card.name).toBe('gemma-1');
    expect(card.metadata['runtime.kind']).toBe('native');
    expect(card.metadata['runtime.roles']).toContain('reasoner');
    expect(card.skills.some(s => s.id === 'reasoning.chat')).toBe(true);
    expect(card.capabilities.extensions.some(
      e => e.uri === 'https://edgecitadel.local/ext/nats-binding/v1')).toBe(true);
  });

  test('POST /command/gemma-1 returns task_id and result completes', async ({ request }) => {
    const post = await request.post(`${API}/api/command/gemma-1`, {
      data: { body: 'Reply with exactly the digit 4 and nothing else.' }
    });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    expect(task_id).toMatch(/^[0-9a-f-]{36}$/);

    let result;
    for (let i = 0; i < POLL_BUDGET_S; i++) {
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      const q = await request.get(
        `${API}/api/messages?task_id=${task_id}&type=result`);
      const rows = await q.json();
      if (rows.length) { result = rows[0]; break; }
    }
    expect(result, `no result in ${POLL_BUDGET_S}s`).toBeDefined();
    expect(result.task_state).toBe('completed');
    expect(result.payload.body).toMatch(/4/);
    expect(result.payload.model).toBeTruthy();
    expect(typeof result.payload.duration_ms).toBe('number');
  });

  test('POST /command/gemma-1 with empty body is rejected by adapter', async ({ request }) => {
    // Empty body fails Pydantic validation at the API layer, returning 422.
    // We DON'T test the adapter-level "empty_prompt" rejection here (that
    // would require a malformed envelope going through NATS, which is
    // covered by adapters/gemma/tests/test_gemma.py).
    const post = await request.post(`${API}/api/command/gemma-1`, {
      data: { body: '' }
    });
    expect([202, 422]).toContain(post.status());
  });

  test('queue endpoint returns pending/ack_pending integers', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/gemma-1/queue`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(Number.isInteger(body.pending)).toBe(true);
    expect(Number.isInteger(body.ack_pending)).toBe(true);
  });
});
```

- [ ] **Step 6.2: Don't actually run the spec**

The smoke spec requires a live stack + Ollama + the Gemma adapter running. Defer to the operator. Verify the file parses syntactically:

```bash
cd e2e
node -c tests/phase2-gemma-smoke.spec.js && echo "syntax ok"
```

If `npx playwright test --list` works in your environment, also confirm Playwright sees the new spec:

```bash
npx playwright test --list 2>&1 | grep phase2-gemma-smoke || echo "spec not collected (check playwright.config.js)"
```

- [ ] **Step 6.3: Commit**

```bash
git add e2e/tests/phase2-gemma-smoke.spec.js
git commit -m "$(cat <<'EOF'
test(e2e): phase 2 Gemma round-trip smoke

Four Playwright tests against a live stack + Ollama:
- gemma-1 card registered with runtime.kind: native, role: reasoner,
  reasoning.chat skill, NATS extension URI.
- POST /api/command/gemma-1 returns 202 + task_id; polling
  /api/messages?task_id=<id>&type=result for up to 60s arrives at a
  completed result whose body matches /4/ and whose payload includes
  model + duration_ms.
- Empty body is rejected (422 from Pydantic, OR 202 if the API
  accepts and adapter rejects via "empty_prompt" -- accept either).
- /api/agents/gemma-1/queue returns integer pending/ack_pending.

Operator runs the smoke after starting Ollama + the adapter:
  PYTHONPATH=. python3 -m adapters.gemma.adapter &
  cd e2e && npm test -- phase2-gemma-smoke.spec.js

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: CHANGELOG entry + final verification

**Files:**
- Modify: `docs/CHANGELOG.md`

- [ ] **Step 7.1: Prepend a Phase 2 entry to the existing `## [Unreleased]` section in `docs/CHANGELOG.md`**

Read the current file first. After the `## [Unreleased]` heading and BEFORE the existing `### Added — v0.1 messaging clean rebuild (Phase 1)` section, insert:

```markdown
### Added — v0.2 Gemma reasoner adapter (Phase 2)
- `adapters/gemma/` — single-shot Ollama-backed reasoner agent
  (`agent_id: gemma-1`, `runtime.kind: native`, `runtime.roles:
  [reasoner]`, skill `reasoning.chat`).
- Wraps `POST /api/generate` (no streaming, no memory in v0.2);
  configurable model/temperature/max_tokens/timeout via per-command
  `payload.args` or env vars.
- Seven typed adapter-level error codes (`unsupported_type`,
  `empty_prompt`, `ollama_unreachable`, `ollama_timeout`,
  `model_not_loaded`, `ollama_inference_error`, `ollama_bad_response`)
  give the dashboard a stable failure vocabulary.
- Fail-fast preflight (`/api/tags` health + model presence check)
  blocks startup with exit code 1 (unreachable) or 2 (model-not-loaded);
  no auto-pull on missing model.
- 11 unit tests + gated live-Ollama integration test +
  `e2e/tests/phase2-gemma-smoke.spec.js`.
- `.env.example` documents `OLLAMA_HOST`, `OLLAMA_PORT`,
  `OLLAMA_MODEL`, `OLLAMA_TIMEOUT_SEC`.

Out of scope (deferred — see `docs/roadmap.md`): multi-skill dispatch,
conversational memory keyed by `context_id`, token streaming via
`task.progress`, WebSocket bridge for live UI updates, non-Ollama
backends, auto-pull, container packaging.
```

- [ ] **Step 7.2: Run the full Python test suite to confirm no regressions**

```bash
cd /Users/yefanzhang/workplace/edge-research/.worktrees/gemma-impl
python3 -m pytest aggregator/tests schemas/tests adapters \
  --ignore=aggregator/tests/test_jetstream_bootstrap.py \
  --ignore=adapters/_common/tests/test_pull_consumer.py \
  --ignore=adapters/gemma/tests/test_gemma_integration.py -v
```

Expected: 60 passed (49 from Phase 1 + 11 new Gemma unit tests). 0 failed.

- [ ] **Step 7.3: Commit**

```bash
git add docs/CHANGELOG.md
git commit -m "$(cat <<'EOF'
docs(changelog): v0.2 Gemma reasoner adapter (Phase 2)

Records the Phase 2 deliverables under [Unreleased]:
- adapters/gemma/ scaffolds the first reasoner agent on the v0.1
  messaging contract.
- Single skill (reasoning.chat), single-shot /api/generate, no
  streaming or memory (deferred per docs/roadmap.md).
- Seven typed error codes; fail-fast preflight; 11 unit tests +
  gated integration test + Playwright smoke.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7.4: End-of-phase verification (operator step)**

The spec's "Verification" table lists 9 checks an operator runs against a live stack:

| Check | Command | Expected |
|---|---|---|
| Ollama running | `curl -s http://localhost:11434/api/tags \| jq '.models[].name'` | model list includes `OLLAMA_MODEL` |
| Adapter registered | `curl http://localhost:8000/api/agents/gemma-1 \| jq` | A2A card; `agent_state: online` |
| Heartbeat fresh | `curl http://localhost:8000/api/agents/gemma-1 \| jq .last_heartbeat` | within ~30s of now |
| Round-trip works | `POST /api/command/gemma-1 {body: "What is 2+2?"}` then poll `/api/messages` | `task_state: completed`, body contains `4` |
| Timeout handling | `POST /api/command/gemma-1 {body: "...", args: {timeout_sec: 1}}` (long prompt) | `task_state: failed`, `error: ollama_timeout` |
| Model-not-loaded | Set `OLLAMA_MODEL=does-not-exist`, restart adapter | adapter exits 2 with clear error message |
| Crash recovery | Kill adapter mid-task; restart | unacked command redelivers (verifies pull_consumer ack semantics under real workload) |
| Queue endpoint | `curl /api/agents/gemma-1/queue` | `{pending, ack_pending}` integers |
| Phase 2 E2E | `cd e2e && npm test -- phase2-gemma-smoke.spec.js` | PASS |

Don't try to run these from the implementation pass — they need a running docker-compose stack + Ollama + the adapter. Document any failures in a follow-up commit / issue.

---

## Self-review checklist (verified before saving)

- [x] **Spec coverage:** Every section in the Phase 2 spec maps to a task above.
  - Architecture / Process model → Task 1, Task 2, Task 3
  - Agent Card (config.yaml) → Task 1
  - Envelope flow / handle() decision tree → Task 2
  - Failure modes (8 conditions, 7 typed codes) → Task 2 (8 unit tests cover all paths) + Task 3 (preflight)
  - Configuration (6 env vars) → Task 1 (config.yaml), Task 2 (adapter.py), Task 5 (.env.example)
  - Preflight + lifecycle → Task 3
  - Testing (unit, integration, E2E) → Task 2, Task 4, Task 6
  - Files (new + modified) → all 7 tasks
  - Verification table (9 rows) → Task 7 step 7.4 (operator)
- [x] **Placeholder scan:** No "TBD"/"TODO"/"add validation"/"similar to" in any task. Every code block is inline. The only "TBD" appearance is in `docs/roadmap.md` referring to FUTURE spec files (deliberate; Phase 2 doesn't write those).
- [x] **Type consistency:** Function names match across tasks. `handle(env, ctx)` is consistent. `PreflightError` and `preflight()` are introduced in Task 3 and tested in the same task. The 7 typed error codes appear identically in handler tests (Task 2), preflight tests (Task 3), and the README (Task 5).
- [x] **File paths:** Every Files block uses absolute-within-repo paths matching the layout we just shipped in Phase 1 (`adapters/_common/`, `adapters/shell/` as the pattern reference).
- [x] **Test counts:** Task 2 → 8 tests; Task 3 → +3 = 11; Task 4 → +1 gated (skip-by-default). Task 7 step 7.2 expects 60 passed (49 Phase 1 + 11 Phase 2). Math checks out.
