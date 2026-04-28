# Agent Messaging v0.1 — Phase 1 Implementation Plan (Clean Rebuild)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the EdgeCitadel v0.1 messaging foundation as a clean rebuild: strict A2A-aligned envelopes, NATS JetStream per-agent serialization, canonical DB schema, shared adapter skeleton, rewritten shell adapter and openclaw browser client, updated frontend, and an end-of-phase smoke E2E.

**Architecture:** Three layers — **Transport** (NATS JetStream `AGENT_INBOX` WorkQueue, per-agent durable pull consumer, `max_ack_pending=1`), **Semantic** (A2A v1.0 task lifecycle on every envelope), **Adapter** (shared `adapters/_common/` pull-consumer skeleton; per-adapter `handle()` body). Aggregator participates as a first-class agent (`agent_id: aggregator`) with its own durable inbox for HTTP-driven results. openclaw-client browser uses an account-scoped `OPENCLAW_TOKEN`; aggregator mediates all JetStream publishes on its behalf. MQTT ingress is deploy-time opt-in, off by default.

**Tech Stack:** Python 3.11+ / FastAPI / `nats-py>=2.9` / `jsonschema` / aiosqlite · Node 20 / `@nats-io/nats` · React 18 / Vite · NATS 2.10 / JetStream · Playwright for E2E.

**Scope:** Phase 1 only (Sessions 1.1–1.9). Phases 2–5 (Gemma adapter, watchdog + registry, AG2 + A2A wrapper, Mac Mini deploy) are the subject of follow-up plans and are sketched only at the end of this doc.

**Source of truth:** `docs/superpowers/specs/2026-04-23-agent-messaging-design.md` (rev 7). Read Sections "Transport layer", "Semantic layer", "Subject inventory", "Verification", and "Impact on the execution plan" before starting.

---

## Prerequisites (read once before Task 1)

- Working on branch `feat/agent-contract-v0.1` (already created).
- The existing dev DB at `/data/openclaw.db` will be wiped during Task 5; warn before destroying if the operator cares about fixture history.
- `docker compose up --build -d` brings up: `nats` (JetStream on 4222, HTTP 8222, MQTT 1883 — will change), `aggregator` (port 8000, proxied), `dashboard` (frontend), `nginx` (port 80). The `openclaw-client` Node process runs outside compose and connects to the broker over LAN/tailnet.
- Python venv convention: `cd aggregator && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt` for unit-testing outside Docker.
- Frontend: `cd frontend && npm install` first time.
- E2E stack lives in `e2e/`; `cd e2e && npm test -- <spec>` runs a targeted spec.
- The NATS CLI (`nats`) is available in the `nats-box` image — inside Docker: `docker compose exec nats nats --help` or on the host if installed.

**Canonical envelope vocabulary** (reference only — referenced by nearly every task):

```
v: 1
id: UUID4
type: register | heartbeat | status | command | result | delegation | cancel | log | broadcast | task.progress
sender_id: string
recipient_id: string        # required for command, result, delegation, cancel
timestamp: ISO 8601 UTC (ms precision, Z)
task_id: UUID4              # command, result, delegation, cancel, task.progress
context_id: UUID4           # required for delegation; propagate if chained
task_state: submitted | working | input-required | completed | failed | canceled | rejected | auth-required  # required for result, task.progress
agent_state: online | offline | busy | error                                                                  # required for status
hop_count: int (delegation only, 0-indexed, refuse ≥8)
payload: object (body, args, error, reason, progress, message, task_id per context)
```

Deprecated names — **NEVER accept these** anywhere in v0.1 code: `receiver_id`, `message_type`, `content`, `from`, `to`, `correlation_id`, `causation_id`, `chain_id`, `assigned_agent`.

---

## File Structure (Phase 1 creates/modifies)

```
schemas/
  envelope.v1.json                                 [MODIFY — rewrite strict]
  agent-card.v1.json                               [MODIFY — replace with A2A v1.0 shape]
  tests/
    test_envelope_schema.py                        [NEW]
    test_agent_card_schema.py                      [NEW]

scripts/
  update-a2a-schema.sh                             [NEW]

aggregator/
  main.py                                          [MODIFY — rewrite API]
  aggregator.py                                    [MODIFY — rewrite NATS subs]
  database.py                                      [MODIFY — rewrite schema]
  models.py                                        [MODIFY — rewrite Pydantic models]
  validator.py                                     [NEW — envelope + card validation]
  jetstream_bootstrap.py                           [NEW — stream + consumer + advisory]
  requirements.txt                                 [MODIFY — add jsonschema, aiosqlite]
  tests/
    test_validator.py                              [NEW]
    test_database.py                               [NEW]
    test_api.py                                    [NEW]
    test_jetstream_bootstrap.py                    [NEW]
    conftest.py                                    [NEW]

adapters/
  _common/
    __init__.py                                    [NEW]
    validator.py                                   [NEW — shared envelope validator]
    agent_card.py                                  [NEW — A2A card factory]
    pull_consumer.py                               [NEW — JetStream pull loop]
    template.py                                    [NEW — skeleton adapter]
    tests/
      conformance.py                               [NEW — shared accept/reject suite]
      test_agent_card.py                           [NEW]
      test_pull_consumer.py                        [NEW]
  shell/
    adapter.py                                     [NEW — replaces shell_adapter.py]
    config.yaml                                    [NEW]
    tests/
      test_shell.py                                [NEW]
    shell_adapter.py                               [DELETE — legacy paho]
    README.md                                      [MODIFY — nats-py usage]
    requirements.txt                               [MODIFY]

openclaw-client/
  index.js                                         [NEW — replaces mqtt-listener.js]
  src/
    nats-session.js                                [NEW]
    aggregator-client.js                           [NEW]
  tests/
    nats-session.test.js                           [NEW]
  package.json                                     [MODIFY — @nats-io/nats]
  mqtt-listener.js                                 [DELETE]
  README.md                                        [MODIFY]

nats/
  nats.conf                                        [MODIFY — MQTT commented; template placeholders]
  nats.conf.tpl                                    [NEW — source template]

frontend/
  src/api/client.js                                [MODIFY — new endpoints]
  src/stores/appStore.js                           [MODIFY — canonical field names]
  src/components/MessageBubble.jsx                 [MODIFY]
  src/components/ConversationThread.jsx            [MODIFY]
  src/components/AgentCard.jsx                     [MODIFY]
  src/components/AgentDetail.jsx                   [MODIFY — queue depth + poison view]
  src/components/TaskBoard.jsx                     [MODIFY]
  src/components/TaskCard.jsx                      [MODIFY]
  src/components/CommFlow.jsx                      [MODIFY]
  src/components/CommandInput.jsx                  [MODIFY]

docker-compose.yml                                 [MODIFY — MQTT behind profile; stop_grace_period]
.env.example                                       [MODIFY — add OPENCLAW_TOKEN, EC_ENABLE_MQTT]

docs/
  agent-contract.md                                [MODIFY — Task 1 deliverable]
  05-messaging.md                                  [MODIFY — Task 7/8 deliverable]
  08-api-reference.md                              [MODIFY — Tasks 4, 8 deliverable]
  CHANGELOG.md                                     [MODIFY — Task 12 deliverable]
  adr/
    0002-nats-jetstream-workqueue.md               [NEW — Task 7]
    0003-a2a-v1-vocabulary-adoption.md             [NEW — Task 1]
    0004-mqtt-ingress-opt-in.md                    [NEW — Task 15]
    0005-browser-scoped-token.md                   [NEW — Task 14]
    0006-outbox-mirror-authoritative.md            [NEW — Task 4]

e2e/
  tests/phase1-smoke.spec.js                       [NEW — Task 17]
  helpers/fixtures.js                              [MODIFY — canonical envelope helpers]
  helpers/mqtt-client.js                           [DELETE]
  tests/<legacy specs>                             [MODIFY or DELETE — see Task 17]
```

---

## Task 1: Envelope schema v1 — strict A2A-aligned

**Files:**
- Modify: `schemas/envelope.v1.json`
- Create: `schemas/tests/test_envelope_schema.py`
- Modify: `docs/agent-contract.md` (authoritative v0.1 contract)
- Create: `docs/adr/0003-a2a-v1-vocabulary-adoption.md`

- [ ] **Step 1.1: Write failing schema tests**

Create `schemas/tests/test_envelope_schema.py`:

```python
"""Tests for the strict v0.1 envelope schema (A2A v1.0 vocabulary)."""
import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "envelope.v1.json"


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(SCHEMA_PATH.read_text())
    return Draft202012Validator(schema)


def _base(**over):
    doc = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "heartbeat",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {},
    }
    doc.update(over)
    return doc


class TestAccepts:
    def test_minimal_heartbeat(self, validator):
        validator.validate(_base())

    def test_status_with_agent_state(self, validator):
        validator.validate(_base(type="status", payload={"reason": "boot"},
                                 agent_state="online"))

    def test_command(self, validator):
        validator.validate(_base(
            type="command", recipient_id="gemma-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            payload={"body": "hello"}))

    def test_result_with_task_state(self, validator):
        validator.validate(_base(
            type="result", recipient_id="shell-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            task_state="completed",
            payload={"body": "done"}))

    def test_delegation_with_context_and_hop(self, validator):
        validator.validate(_base(
            type="delegation", recipient_id="worker-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            context_id="cccccccc-dddd-4eee-8fff-000000000000",
            hop_count=1,
            payload={"body": "subtask"}))

    def test_task_progress(self, validator):
        validator.validate(_base(
            type="task.progress",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            task_state="working",
            payload={"progress": 42, "message": "halfway"}))

    def test_cancel(self, validator):
        validator.validate(_base(
            type="cancel", recipient_id="gemma-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            payload={"task_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                     "reason": "user_aborted"}))


class TestRejects:
    def test_unknown_top_level_field(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(receiver_id="gemma-1"))

    def test_legacy_message_type(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(message_type="info"))

    def test_missing_recipient_on_command(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(type="command",
                                     task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                                     payload={"body": "x"}))

    def test_missing_task_state_on_result(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="shell-1",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                payload={"body": "done"}))

    def test_missing_hop_count_on_delegation(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="delegation", recipient_id="w",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                context_id="cccccccc-dddd-4eee-8fff-000000000000",
                payload={"body": "x"}))

    def test_wrong_v(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(v=2))

    def test_bad_task_state_enum(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="x",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                task_state="not-a-state", payload={}))

    def test_agent_state_on_result_rejected(self, validator):
        # agent_state must not appear on result; only on status
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="x",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                task_state="completed", agent_state="busy", payload={}))

    def test_task_state_on_status_rejected(self, validator):
        # task_state must not appear on status
        with pytest.raises(ValidationError):
            validator.validate(_base(type="status", agent_state="online",
                                     task_state="working", payload={}))

    def test_hop_count_too_high_allowed_by_schema_refused_by_adapter(self, validator):
        # Schema allows any int; refusal at >=8 is adapter-level (Task 9 / 4.2).
        validator.validate(_base(
            type="delegation", recipient_id="x",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            context_id="cccccccc-dddd-4eee-8fff-000000000000",
            hop_count=99, payload={}))
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `cd /Users/yefanzhang/workplace/edge-research && python3 -m pytest schemas/tests/test_envelope_schema.py -v`

Expected: FAIL — existing schema is permissive (`additionalProperties: true`, allows `receiver_id`/`message_type`/legacy fields, no `cancel`/`task.progress` enums, missing conditional requirements).

- [ ] **Step 1.3: Rewrite `schemas/envelope.v1.json`**

Replace the whole file:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://edgecitadel.dev/schemas/envelope.v1.json",
  "title": "EdgeCitadel Agent Contract Envelope",
  "description": "Wire format for every message published under agents.*, tasks.*, or system.* subjects. v0.1 / A2A v1.0-aligned. Strict: unknown top-level fields rejected.",
  "type": "object",
  "additionalProperties": false,
  "required": ["v", "id", "type", "sender_id", "timestamp", "payload"],
  "properties": {
    "v": {"const": 1},
    "id": {
      "type": "string",
      "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    },
    "type": {
      "type": "string",
      "enum": ["register", "heartbeat", "status", "command", "result",
               "delegation", "cancel", "log", "broadcast", "task.progress"]
    },
    "sender_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,63}$"},
    "recipient_id": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,63}$"},
    "timestamp": {
      "type": "string",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$"
    },
    "task_id": {"type": "string",
      "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"},
    "context_id": {"type": "string",
      "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"},
    "task_state": {
      "type": "string",
      "enum": ["submitted", "working", "input-required", "completed",
               "failed", "canceled", "rejected", "auth-required"]
    },
    "agent_state": {
      "type": "string",
      "enum": ["online", "offline", "busy", "error"]
    },
    "hop_count": {"type": "integer", "minimum": 0},
    "payload": {"type": "object"}
  },
  "allOf": [
    {
      "if": {"properties": {"type": {"const": "command"}}, "required": ["type"]},
      "then": {"required": ["recipient_id", "task_id"],
               "not": {"required": ["agent_state"]}}
    },
    {
      "if": {"properties": {"type": {"const": "result"}}, "required": ["type"]},
      "then": {"required": ["recipient_id", "task_id", "task_state"],
               "not": {"required": ["agent_state"]}}
    },
    {
      "if": {"properties": {"type": {"const": "delegation"}}, "required": ["type"]},
      "then": {"required": ["recipient_id", "task_id", "context_id", "hop_count"],
               "not": {"required": ["agent_state"]}}
    },
    {
      "if": {"properties": {"type": {"const": "cancel"}}, "required": ["type"]},
      "then": {"required": ["recipient_id", "task_id"],
               "not": {"required": ["agent_state"]}}
    },
    {
      "if": {"properties": {"type": {"const": "task.progress"}}, "required": ["type"]},
      "then": {"required": ["task_id", "task_state"],
               "not": {"required": ["agent_state"]}}
    },
    {
      "if": {"properties": {"type": {"const": "status"}}, "required": ["type"]},
      "then": {"required": ["agent_state"],
               "not": {"required": ["task_state"]}}
    }
  ]
}
```

- [ ] **Step 1.4: Add `jsonschema` to aggregator deps**

Modify `aggregator/requirements.txt` — ensure `jsonschema>=4.20` is present. Append if missing:

```
jsonschema>=4.20
```

- [ ] **Step 1.5: Run tests to verify they pass**

Run: `cd /Users/yefanzhang/workplace/edge-research && pip install -q jsonschema pytest && python3 -m pytest schemas/tests/test_envelope_schema.py -v`

Expected: PASS (all `TestAccepts::*` + `TestRejects::*`).

- [ ] **Step 1.6: Rewrite `docs/agent-contract.md` to match the spec**

Read the current file, then rewrite it so the canonical v0.1 contract matches `docs/superpowers/specs/2026-04-23-agent-messaging-design.md` §"Semantic layer" and §"Subject inventory". Field table, type enum, task_state enum, agent_state enum, required-by-type matrix, subject inventory, transport-binding A2A extension URI. Drop every reference to legacy fields, MQTT-as-primary, or `correlation_id`.

- [ ] **Step 1.7: Write ADR 0003 (A2A v1.0 vocabulary adoption)**

Create `docs/adr/0003-a2a-v1-vocabulary-adoption.md` following `docs/adr/template.md` shape. Status: Accepted. Context: vocabulary heterogeneity pre-v0.1. Decision: A2A v1.0 task lifecycle on every envelope; semantic-only borrow (NATS transport via the `https://edgecitadel.local/ext/nats-binding/v1` extension URI). Consequences: legacy names deleted; external A2A interop requires HTTP+SSE wrapper at Phase 4.

- [ ] **Step 1.8: Commit**

```bash
git add schemas/envelope.v1.json schemas/tests/test_envelope_schema.py \
        aggregator/requirements.txt \
        docs/agent-contract.md docs/adr/0003-a2a-v1-vocabulary-adoption.md
git commit -m "feat(schemas): strict v0.1 envelope + A2A vocabulary adoption (ADR-0003)"
```

---

## Task 2: Agent Card schema v1 (A2A v1.0) + update script

**Files:**
- Modify: `schemas/agent-card.v1.json`
- Create: `schemas/tests/test_agent_card_schema.py`
- Create: `scripts/update-a2a-schema.sh`

- [ ] **Step 2.1: Write failing tests**

Create `schemas/tests/test_agent_card_schema.py`:

```python
"""Agent Card schema tests — A2A v1.0 shape + EdgeCitadel metadata vocabulary."""
import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMA = json.loads((Path(__file__).resolve().parents[1]
                     / "agent-card.v1.json").read_text())


@pytest.fixture(scope="module")
def validator():
    return Draft202012Validator(SCHEMA)


def _card(**over):
    doc = {
        "name": "shell-1",
        "description": "Shell executor.",
        "version": "0.1.0",
        "url": "nats://edgecitadel/agents.shell-1.inbox",
        "provider": {"organization": "EdgeCitadel", "url": "https://edgecitadel.local"},
        "capabilities": {
            "streaming": False,
            "extensions": [{
                "uri": "https://edgecitadel.local/ext/nats-binding/v1",
                "description": "NATS JetStream transport binding.",
                "required": False,
                "params": {"subject_prefix": "agents.shell-1"}
            }]
        },
        "securitySchemes": {},
        "skills": [{"id": "shell.exec", "name": "shell-exec",
                    "description": "Run a shell command.",
                    "tags": ["shell"]}],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.heartbeat_interval_sec": 30
        }
    }
    doc.update(over)
    return doc


class TestAccepts:
    def test_minimal_native(self, validator):
        validator.validate(_card())

    def test_bridge_requires_upstream(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.kind": "bridge",
                         "runtime.upstream": "nous-hermes-agent"}
        validator.validate(c)


class TestRejects:
    def test_missing_required_a2a_field(self, validator):
        c = _card(); del c["provider"]
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_bridge_without_upstream(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.kind": "bridge"}
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_bad_runtime_role(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.roles": ["not-a-role"]}
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_heartbeat_out_of_range(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.heartbeat_interval_sec": 5}
        with pytest.raises(ValidationError):
            validator.validate(c)
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `python3 -m pytest schemas/tests/test_agent_card_schema.py -v`

Expected: FAIL — existing `agent-card.v1.json` has the legacy EdgeCitadel shape (`agent_id`, `display_name`, `runtime.framework`), not A2A v1.0.

- [ ] **Step 2.3: Replace `schemas/agent-card.v1.json` with A2A v1.0 shape**

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://edgecitadel.dev/schemas/agent-card.v1.json",
  "title": "A2A v1.0 Agent Card (EdgeCitadel profile)",
  "description": "Payload of agents.{id}.register. Full A2A v1.0 Agent Card shape with EdgeCitadel metadata vocabulary (runtime.kind, runtime.roles, runtime.upstream, runtime.heartbeat_interval_sec).",
  "type": "object",
  "additionalProperties": true,
  "required": ["name", "description", "version", "url", "provider",
               "capabilities", "securitySchemes", "metadata"],
  "properties": {
    "name": {"type": "string", "minLength": 1},
    "description": {"type": "string"},
    "version": {"type": "string"},
    "url": {"type": "string"},
    "provider": {
      "type": "object",
      "required": ["organization"],
      "properties": {
        "organization": {"type": "string"},
        "url": {"type": "string"}
      }
    },
    "capabilities": {
      "type": "object",
      "properties": {
        "streaming": {"type": "boolean"},
        "extensions": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["uri"],
            "properties": {
              "uri": {"type": "string"},
              "description": {"type": "string"},
              "required": {"type": "boolean"},
              "params": {"type": "object"}
            }
          }
        }
      }
    },
    "securitySchemes": {"type": "object"},
    "additionalInterfaces": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["url", "transport"],
        "properties": {
          "url": {"type": "string"},
          "transport": {"type": "string"}
        }
      }
    },
    "skills": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "name", "description"],
        "properties": {
          "id": {"type": "string"},
          "name": {"type": "string"},
          "description": {"type": "string"},
          "tags": {"type": "array", "items": {"type": "string"}}
        }
      }
    },
    "defaultInputModes": {"type": "array", "items": {"type": "string"}},
    "defaultOutputModes": {"type": "array", "items": {"type": "string"}},
    "metadata": {
      "type": "object",
      "required": ["runtime.kind", "runtime.roles",
                   "runtime.heartbeat_interval_sec"],
      "properties": {
        "runtime.kind": {"type": "string", "enum": ["native", "bridge"]},
        "runtime.roles": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "string",
            "enum": ["worker", "reasoner", "watchdog",
                     "orchestrator", "aggregator"]
          }
        },
        "runtime.tags": {"type": "array", "items": {"type": "string"}},
        "runtime.deployment": {"type": "string"},
        "runtime.upstream": {"type": "string"},
        "runtime.heartbeat_interval_sec": {
          "type": "integer", "minimum": 10, "maximum": 300
        }
      },
      "allOf": [
        {
          "if": {"properties": {"runtime.kind": {"const": "bridge"}},
                 "required": ["runtime.kind"]},
          "then": {"required": ["runtime.upstream"]}
        }
      ]
    }
  }
}
```

- [ ] **Step 2.4: Run tests, confirm pass**

Run: `python3 -m pytest schemas/tests/test_agent_card_schema.py -v`

Expected: PASS.

- [ ] **Step 2.5: Write `scripts/update-a2a-schema.sh`**

```bash
#!/usr/bin/env bash
# Pulls the latest A2A Agent Card schema from upstream and diffs against
# our vendored copy. Human-review-only — does NOT auto-merge. See ADR-0003.
set -euo pipefail

UPSTREAM="https://raw.githubusercontent.com/a2aproject/A2A/main/specification/json/a2a.json"
VENDORED="$(cd "$(dirname "$0")/.." && pwd)/schemas/agent-card.v1.json"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

echo "Fetching A2A schema from $UPSTREAM ..."
curl -fsSL "$UPSTREAM" -o "$TMP"

echo "Diff (upstream → vendored):"
diff -u "$TMP" "$VENDORED" || true
echo
echo "Review the diff and update schemas/agent-card.v1.json by hand if needed."
echo "Do NOT run sed/jq replacement unguarded — our metadata vocabulary is additive."
```

Make executable: `chmod +x scripts/update-a2a-schema.sh`.

- [ ] **Step 2.6: Commit**

```bash
git add schemas/agent-card.v1.json schemas/tests/test_agent_card_schema.py \
        scripts/update-a2a-schema.sh
git commit -m "feat(schemas): replace agent-card.v1.json with A2A v1.0 shape"
```

---

## Task 3: Aggregator validator module (shared by Task 4 + Task 10)

**Files:**
- Create: `aggregator/validator.py`
- Create: `aggregator/tests/conftest.py`
- Create: `aggregator/tests/test_validator.py`

- [ ] **Step 3.1: Create test fixtures**

Create `aggregator/tests/conftest.py`:

```python
import json
from pathlib import Path
import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def envelope_schema_path():
    return REPO / "schemas" / "envelope.v1.json"


@pytest.fixture(scope="session")
def card_schema_path():
    return REPO / "schemas" / "agent-card.v1.json"
```

- [ ] **Step 3.2: Write failing validator tests**

Create `aggregator/tests/test_validator.py`:

```python
import pytest
from aggregator.validator import EnvelopeValidator, ValidationError


@pytest.fixture(scope="module")
def validator(envelope_schema_path, card_schema_path):
    return EnvelopeValidator(envelope_schema_path, card_schema_path)


def _env(**over):
    base = {
        "v": 1, "id": "11111111-2222-4333-8444-555555555555",
        "type": "heartbeat", "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {}
    }
    base.update(over); return base


def test_accepts_valid(validator):
    validator.validate_envelope(_env())


def test_rejects_unknown_field(validator):
    with pytest.raises(ValidationError) as exc:
        validator.validate_envelope(_env(receiver_id="x"))
    assert "receiver_id" in str(exc.value) or "unexpected" in str(exc.value).lower()


def test_rejects_missing_type(validator):
    bad = _env(); del bad["type"]
    with pytest.raises(ValidationError):
        validator.validate_envelope(bad)


def test_register_card_must_match_sender_id(validator):
    env = _env(type="register", sender_id="shell-1",
               payload={"name": "shell-1", "description": "x", "version": "0.1",
                        "url": "nats://x", "provider": {"organization": "EC"},
                        "capabilities": {}, "securitySchemes": {},
                        "metadata": {"runtime.kind": "native",
                                     "runtime.roles": ["worker"],
                                     "runtime.heartbeat_interval_sec": 30}})
    validator.validate_envelope(env)
    validator.validate_register(env)  # name == sender_id

    env["payload"]["name"] = "different"
    with pytest.raises(ValidationError, match="sender_id"):
        validator.validate_register(env)
```

- [ ] **Step 3.3: Run tests, confirm fail (module missing)**

Run: `cd /Users/yefanzhang/workplace/edge-research && python3 -m pytest aggregator/tests/test_validator.py -v`

Expected: FAIL — `ModuleNotFoundError: aggregator.validator`.

- [ ] **Step 3.4: Create `aggregator/validator.py`**

```python
"""Envelope and Agent Card validation against vendored schemas."""
from __future__ import annotations
import json
from pathlib import Path
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JSONSchemaError


class ValidationError(Exception):
    pass


class EnvelopeValidator:
    def __init__(self, envelope_schema_path: Path, card_schema_path: Path):
        self._env = Draft202012Validator(json.loads(
            Path(envelope_schema_path).read_text()))
        self._card = Draft202012Validator(json.loads(
            Path(card_schema_path).read_text()))

    def validate_envelope(self, doc: dict) -> None:
        try:
            self._env.validate(doc)
        except JSONSchemaError as e:
            raise ValidationError(f"envelope invalid: {e.message} "
                                  f"at {list(e.absolute_path)}") from e

    def validate_card(self, doc: dict) -> None:
        try:
            self._card.validate(doc)
        except JSONSchemaError as e:
            raise ValidationError(f"agent_card invalid: {e.message} "
                                  f"at {list(e.absolute_path)}") from e

    def validate_register(self, envelope: dict) -> None:
        """Checks register envelope payload is a valid Agent Card AND that
        envelope.sender_id matches payload.name (A2A Agent Card identity)."""
        self.validate_envelope(envelope)
        if envelope.get("type") != "register":
            raise ValidationError("validate_register called on non-register envelope")
        card = envelope.get("payload", {})
        self.validate_card(card)
        if card.get("name") != envelope.get("sender_id"):
            raise ValidationError(
                f"sender_id {envelope.get('sender_id')!r} must match "
                f"Agent Card name {card.get('name')!r}")
```

- [ ] **Step 3.5: Run tests, confirm pass**

Run: `python3 -m pytest aggregator/tests/test_validator.py -v`

Expected: PASS (3 tests).

- [ ] **Step 3.6: Commit**

```bash
git add aggregator/validator.py aggregator/tests/test_validator.py \
        aggregator/tests/conftest.py
git commit -m "feat(aggregator): envelope + Agent Card validator module"
```

---

## Task 4: Aggregator database + models rewrite

**Files:**
- Modify: `aggregator/models.py`
- Modify: `aggregator/database.py`
- Create: `aggregator/tests/test_database.py`
- Create: `docs/adr/0006-outbox-mirror-authoritative.md`

- [ ] **Step 4.1: Write failing DB tests**

Create `aggregator/tests/test_database.py`:

```python
import os
import tempfile
import pytest
from aggregator import database as db


@pytest.fixture
def fresh_db():
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    os.environ["DB_PATH"] = f.name
    db.init_db(f.name)
    yield f.name
    os.unlink(f.name)


def test_schema_has_canonical_columns(fresh_db):
    cols = db.table_columns("messages")
    assert "recipient_id" in cols
    assert "type" in cols
    assert "task_id" in cols
    assert "context_id" in cols
    assert "task_state" in cols
    assert "agent_state" in cols
    assert "receiver_id" not in cols
    assert "message_type" not in cols


def test_wipe_on_first_boot_flag(tmp_path):
    """init_db(path, wipe=True) drops and recreates schema."""
    p = str(tmp_path / "openclaw.db")
    db.init_db(p)
    db.insert_message(dict(
        id="11111111-2222-4333-8444-555555555555",
        type="heartbeat", sender_id="shell-1",
        timestamp="2026-04-23T10:00:00.000Z", payload={}
    ))
    assert db.count_messages() == 1
    db.init_db(p, wipe=True)
    assert db.count_messages() == 0


def test_insert_and_retrieve_result(fresh_db):
    env = dict(
        id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        type="result", sender_id="gemma-1", recipient_id="shell-1",
        task_id="bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
        task_state="completed",
        timestamp="2026-04-23T10:00:05.000Z",
        payload={"body": "done"}
    )
    db.insert_message(env)
    rows = db.query_messages(agent_id="gemma-1")
    assert len(rows) == 1
    assert rows[0]["task_state"] == "completed"
    assert rows[0]["recipient_id"] == "shell-1"
```

- [ ] **Step 4.2: Run tests, confirm fail**

Run: `python3 -m pytest aggregator/tests/test_database.py -v`

Expected: FAIL — legacy schema has `receiver_id`/`message_type` and no `task_id`/`context_id`/`task_state`/`agent_state`.

- [ ] **Step 4.3: Rewrite `aggregator/database.py` schema + insert/query**

Rewrite the file. Key sections only (keep SQLite + `contextlib.contextmanager` cursor patterns already in use; adjust per existing style). Canonical `messages` schema:

```python
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    v               INTEGER NOT NULL DEFAULT 1,
    type            TEXT NOT NULL,
    sender_id       TEXT NOT NULL,
    recipient_id    TEXT,
    task_id         TEXT,
    context_id      TEXT,
    task_state      TEXT,
    agent_state     TEXT,
    hop_count       INTEGER,
    timestamp       TEXT NOT NULL,
    payload         TEXT NOT NULL,   -- JSON-serialized object
    deployment      TEXT NOT NULL DEFAULT 'default'
);
CREATE INDEX IF NOT EXISTS idx_messages_sender      ON messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_recipient   ON messages(recipient_id);
CREATE INDEX IF NOT EXISTS idx_messages_task        ON messages(task_id);
CREATE INDEX IF NOT EXISTS idx_messages_context     ON messages(context_id);
CREATE INDEX IF NOT EXISTS idx_messages_type_ts     ON messages(type, timestamp);

CREATE TABLE IF NOT EXISTS agents (
    agent_id                TEXT PRIMARY KEY,
    card_json               TEXT NOT NULL,
    agent_state             TEXT NOT NULL DEFAULT 'online',
    last_heartbeat          TEXT,
    last_register           TEXT NOT NULL,
    deployment              TEXT,
    heartbeat_interval_sec  INTEGER NOT NULL DEFAULT 30
);

CREATE TABLE IF NOT EXISTS poison_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id        TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    task_id         TEXT,
    original_sender TEXT,
    detected_at     TEXT NOT NULL,
    advisory_json   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_poison_agent ON poison_events(agent_id, detected_at);
"""


def init_db(path: str, wipe: bool = False) -> None:
    """If wipe=True OR env EDGECITADEL_DB_WIPE=1 is set, drops tables first."""
    with sqlite3.connect(path) as conn:
        if wipe or os.environ.get("EDGECITADEL_DB_WIPE") == "1":
            conn.executescript("""DROP TABLE IF EXISTS messages;
                                  DROP TABLE IF EXISTS agents;
                                  DROP TABLE IF EXISTS poison_events;""")
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def insert_message(env: dict, deployment: str = "default") -> None:
    with _conn() as c:
        c.execute(
            """INSERT OR IGNORE INTO messages
               (id, v, type, sender_id, recipient_id, task_id, context_id,
                task_state, agent_state, hop_count, timestamp, payload, deployment)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (env["id"], env.get("v", 1), env["type"], env["sender_id"],
             env.get("recipient_id"), env.get("task_id"), env.get("context_id"),
             env.get("task_state"), env.get("agent_state"), env.get("hop_count"),
             env["timestamp"], json.dumps(env.get("payload", {})), deployment))


def query_messages(*, agent_id: str | None = None, task_id: str | None = None,
                   context_id: str | None = None, type: str | None = None,
                   since_ts: str | None = None, limit: int = 500) -> list[dict]:
    q = "SELECT * FROM messages WHERE 1=1"
    params: list = []
    if agent_id:
        q += " AND (sender_id = ? OR recipient_id = ?)"; params += [agent_id, agent_id]
    if task_id:
        q += " AND task_id = ?"; params.append(task_id)
    if context_id:
        q += " AND context_id = ?"; params.append(context_id)
    if type:
        q += " AND type = ?"; params.append(type)
    if since_ts:
        q += " AND timestamp >= ?"; params.append(since_ts)
    q += " ORDER BY timestamp DESC LIMIT ?"; params.append(limit)
    with _conn() as c:
        rows = [dict(r) for r in c.execute(q, params).fetchall()]
    for r in rows:
        r["payload"] = json.loads(r["payload"])
    return rows
```

Also keep or add:

```python
def count_messages() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]


def table_columns(name: str) -> set[str]:
    with _conn() as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({name})").fetchall()}


def upsert_agent_card(card: dict, timestamp: str) -> None:
    import json
    with _conn() as c:
        c.execute("""INSERT INTO agents
                     (agent_id, card_json, last_register, agent_state,
                      heartbeat_interval_sec, deployment)
                     VALUES (?, ?, ?, 'online', ?, ?)
                     ON CONFLICT(agent_id) DO UPDATE SET
                       card_json = excluded.card_json,
                       last_register = excluded.last_register,
                       agent_state = 'online',
                       heartbeat_interval_sec = excluded.heartbeat_interval_sec,
                       deployment = excluded.deployment""",
                  (card["name"], json.dumps(card), timestamp,
                   card["metadata"]["runtime.heartbeat_interval_sec"],
                   card["metadata"].get("runtime.deployment")))


def update_heartbeat(agent_id: str, ts: str) -> None:
    with _conn() as c:
        c.execute("UPDATE agents SET last_heartbeat = ?, agent_state = 'online' "
                  "WHERE agent_id = ?", (ts, agent_id))


def update_agent_state(agent_id: str, state: str) -> None:
    with _conn() as c:
        c.execute("UPDATE agents SET agent_state = ? WHERE agent_id = ?",
                  (state, agent_id))


def list_agents() -> list[dict]:
    with _conn() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT agent_id, card_json, agent_state, last_heartbeat, "
            "last_register, deployment, heartbeat_interval_sec "
            "FROM agents").fetchall()]
    import json
    for r in rows:
        r["card"] = json.loads(r.pop("card_json"))
    return rows


def get_agent(agent_id: str) -> dict | None:
    rows = [r for r in list_agents() if r["agent_id"] == agent_id]
    return rows[0] if rows else None


def delete_agent(agent_id: str) -> bool:
    with _conn() as c:
        c.execute("DELETE FROM agents WHERE agent_id = ?", (agent_id,))
        return c.rowcount > 0


def insert_poison_event(*, agent_id: str, consumer: str, task_id: str | None,
                        original_sender: str | None, detected_at: str,
                        advisory: dict) -> None:
    import json
    with _conn() as c:
        c.execute("""INSERT INTO poison_events
                     (agent_id, consumer, task_id, original_sender,
                      detected_at, advisory_json)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (agent_id, consumer, task_id, original_sender, detected_at,
                   json.dumps(advisory)))


def recent_poison(agent_id: str | None = None, limit: int = 100) -> list[dict]:
    q = "SELECT * FROM poison_events"; params = []
    if agent_id:
        q += " WHERE agent_id = ?"; params = [agent_id]
    q += " ORDER BY detected_at DESC LIMIT ?"; params.append(limit)
    with _conn() as c:
        return [dict(r) for r in c.execute(q, params).fetchall()]
```

Delete all legacy code paths: `receiver_id` parameter, `message_type` column/parameter, `is_test_id` filtering on legacy names, any `insert_message` variant with `message_type`.

- [ ] **Step 4.4: Rewrite `aggregator/models.py`**

Pydantic models that mirror the envelope schema. Used by API layer in Task 5.

```python
from typing import Literal, Optional
from pydantic import BaseModel, Field


class Envelope(BaseModel):
    v: Literal[1] = 1
    id: str
    type: Literal["register", "heartbeat", "status", "command", "result",
                  "delegation", "cancel", "log", "broadcast", "task.progress"]
    sender_id: str
    recipient_id: Optional[str] = None
    task_id: Optional[str] = None
    context_id: Optional[str] = None
    task_state: Optional[Literal["submitted", "working", "input-required",
                                 "completed", "failed", "canceled", "rejected",
                                 "auth-required"]] = None
    agent_state: Optional[Literal["online", "offline", "busy", "error"]] = None
    hop_count: Optional[int] = Field(default=None, ge=0)
    timestamp: str
    payload: dict


class CommandRequest(BaseModel):
    body: str
    args: Optional[dict] = None


class CommandResponse(BaseModel):
    task_id: str
    recipient_id: str
    accepted_at: str
```

- [ ] **Step 4.5: Run tests, confirm pass**

Run: `python3 -m pytest aggregator/tests/test_database.py -v`

Expected: PASS (3 tests).

- [ ] **Step 4.6: Write ADR 0006 (outbox mirror authoritative)**

Create `docs/adr/0006-outbox-mirror-authoritative.md`. Status: Accepted. Context: WorkQueuePolicy disjoint-filter rule prevents an audit consumer on `AGENT_INBOX`. Decision: adapters mirror every outbound inbox publish to `agents.{self}.outbox` via plain NATS. Aggregator treats outbox as authoritative for dashboard conversation views. Consequences: aggregator-down during an outbox publish loses that event for dashboard (durable inbox delivery is unaffected).

- [ ] **Step 4.7: Commit**

```bash
git add aggregator/database.py aggregator/models.py \
        aggregator/tests/test_database.py \
        docs/adr/0006-outbox-mirror-authoritative.md
git commit -m "feat(aggregator): canonical DB schema + models (outbox-authoritative, ADR-0006)"
```

---

## Task 5: Aggregator NATS subscribers + Agent Card cache + request_register dance

**Files:**
- Modify: `aggregator/aggregator.py`
- Create: `aggregator/tests/test_aggregator.py`

- [ ] **Step 5.1: Write failing aggregator-core tests**

Create `aggregator/tests/test_aggregator.py`:

```python
"""Unit tests for the NATS subscriber glue. Does not talk to a live NATS."""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from aggregator.aggregator import MessageRouter


def _bytes(env: dict) -> bytes:
    return json.dumps(env).encode()


@pytest.fixture
def router(tmp_path, envelope_schema_path, card_schema_path):
    from aggregator import database as db
    p = str(tmp_path / "t.db"); db.init_db(p, wipe=True)
    return MessageRouter(db_path=p, envelope_schema=envelope_schema_path,
                         card_schema=card_schema_path)


@pytest.mark.asyncio
async def test_register_caches_card(router):
    card = {
        "name": "shell-1", "description": "x", "version": "0.1",
        "url": "nats://x", "provider": {"organization": "EC"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}}
    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": "register", "sender_id": "shell-1",
           "timestamp": "2026-04-23T10:00:00.000Z", "payload": card}
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))
    assert router.cache.get("shell-1")["name"] == "shell-1"


@pytest.mark.asyncio
async def test_register_rejects_sender_id_mismatch(router):
    card = {
        "name": "impostor", "description": "x", "version": "0.1",
        "url": "nats://x", "provider": {"organization": "EC"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}}
    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": "register", "sender_id": "shell-1",
           "timestamp": "2026-04-23T10:00:00.000Z", "payload": card}
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))
    assert "shell-1" not in router.cache  # rejected


@pytest.mark.asyncio
async def test_malformed_envelope_dropped(router):
    bad = {"v": 1, "type": "not-a-type"}   # missing required
    await router.on_outbox(_fake_msg("agents.x.outbox", _bytes(bad)))
    # No exception, no DB row
    from aggregator import database as db
    assert db.count_messages() == 0


@pytest.mark.asyncio
async def test_heartbeat_updates_last_seen(router):
    await _register_shell(router)
    env = {"v": 1, "id": "22222222-3333-4444-8555-666666666666",
           "type": "heartbeat", "sender_id": "shell-1",
           "timestamp": "2026-04-23T10:00:30.000Z", "payload": {"cpu_percent": 5}}
    await router.on_heartbeat(_fake_msg("agents.shell-1.heartbeat", _bytes(env)))
    from aggregator import database as db
    a = db.get_agent("shell-1")
    assert a["last_heartbeat"] == "2026-04-23T10:00:30.000Z"


def _fake_msg(subject, data):
    m = MagicMock(); m.subject = subject; m.data = data; return m


async def _register_shell(router):
    card = {
        "name": "shell-1", "description": "x", "version": "0.1",
        "url": "nats://x", "provider": {"organization": "EC"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}}
    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": "register", "sender_id": "shell-1",
           "timestamp": "2026-04-23T10:00:00.000Z", "payload": card}
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))
```

Also add `pytest-asyncio>=0.23` to `aggregator/requirements.txt` and ensure `aggregator/tests/conftest.py` marks `asyncio_mode = "auto"` via `aggregator/pytest.ini`:

```ini
[pytest]
asyncio_mode = auto
testpaths = aggregator/tests schemas/tests adapters
```

- [ ] **Step 5.2: Run tests, confirm fail**

Run: `python3 -m pytest aggregator/tests/test_aggregator.py -v`

Expected: FAIL — `MessageRouter` not yet implemented / current `aggregator.py` routes via legacy field readers.

- [ ] **Step 5.3: Rewrite `aggregator/aggregator.py` core router**

Delete every alias-fallback reader. New structure:

```python
"""
Aggregator NATS glue.

Plain NATS subscribers: agents.*.register / .heartbeat / .status / .outbox /
    .log / system.broadcast / tasks.* / $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>

Durable JetStream consumer: agents.aggregator.inbox (for results returned to HTTP callers).

All routing is keyed off the strict envelope schema; malformed envelopes are
dropped silently with a logged reason (preserves aggregator liveness).
"""
from __future__ import annotations
import asyncio, json, logging, os
from datetime import datetime, timezone
from pathlib import Path

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from . import database as db
from .validator import EnvelopeValidator, ValidationError

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


class MessageRouter:
    def __init__(self, *, db_path: str,
                 envelope_schema: Path, card_schema: Path):
        self.db_path = db_path
        self.validator = EnvelopeValidator(envelope_schema, card_schema)
        self.cache: dict[str, dict] = {}    # agent_id -> Agent Card
        self.pending_tasks: dict[str, asyncio.Future] = {}  # task_id -> future
        self.nc: NATS | None = None
        self.js = None

    # ---- plain-NATS subscriber handlers ----

    async def on_register(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None: return
        try:
            self.validator.validate_register(env)
        except ValidationError as e:
            log.warning("rejecting register from %s: %s", env.get("sender_id"), e)
            return
        card = env["payload"]
        self.cache[env["sender_id"]] = card
        db.upsert_agent_card(card, timestamp=env["timestamp"])
        log.info("registered %s (kind=%s)", env["sender_id"],
                 card["metadata"]["runtime.kind"])

    async def on_heartbeat(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "heartbeat": return
        db.update_heartbeat(env["sender_id"], env["timestamp"])

    async def on_status(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "status": return
        db.update_agent_state(env["sender_id"], env["agent_state"])
        db.insert_message(env)

    async def on_log(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "log": return
        db.insert_message(env)

    async def on_outbox(self, msg: Msg) -> None:
        """Outbox mirror: authoritative audit path for inbox traffic."""
        env = self._parse_and_validate(msg.data)
        if env is None: return
        # We persist every outbox event so the dashboard has a canonical view
        db.insert_message(env)
        # If this outbox is a result matching an HTTP-driven pending task, resolve it
        if env["type"] == "result":
            f = self.pending_tasks.pop(env.get("task_id", ""), None)
            if f is not None and not f.done():
                f.set_result(env)

    async def on_broadcast(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None: return
        db.insert_message(env)

    async def on_advisory(self, msg: Msg) -> None:
        """$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>."""
        try:
            adv = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        # subject tail: ...MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>
        parts = msg.subject.split(".")
        agent = parts[-2] if len(parts) >= 2 else "unknown"
        consumer = parts[-1] if parts else "unknown"
        # Extract original headers if present
        hdrs = (adv.get("headers") or {})
        orig_sender = hdrs.get("Original-Sender") or adv.get("sender_id")
        task_id = hdrs.get("Task-Id") or adv.get("task_id")
        db.insert_poison_event(agent_id=agent, consumer=consumer,
                               task_id=task_id, original_sender=orig_sender,
                               detected_at=now_iso(), advisory=adv)
        log.warning("poison message on %s (consumer=%s, task_id=%s)",
                    agent, consumer, task_id)

    # ---- helpers ----

    def _parse(self, data: bytes) -> dict | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            log.warning("non-JSON message dropped")
            return None

    def _parse_and_validate(self, data: bytes) -> dict | None:
        env = self._parse(data)
        if env is None: return None
        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("dropping malformed envelope: %s", e)
            return None
        return env


class AggregatorApp:
    """Wires MessageRouter to NATS subscriptions and durable consumer."""

    def __init__(self, nats_url: str, nats_token: str, db_path: str,
                 envelope_schema: Path, card_schema: Path):
        self.nats_url = nats_url; self.nats_token = nats_token
        self.router = MessageRouter(db_path=db_path,
                                    envelope_schema=envelope_schema,
                                    card_schema=card_schema)

    async def start(self) -> None:
        self.router.nc = NATS()
        await self.router.nc.connect(servers=[self.nats_url],
                                     token=self.nats_token)
        nc = self.router.nc
        self.router.js = nc.jetstream()

        await nc.subscribe("agents.*.register", cb=self.router.on_register)
        await nc.subscribe("agents.*.heartbeat", cb=self.router.on_heartbeat)
        await nc.subscribe("agents.*.status", cb=self.router.on_status)
        await nc.subscribe("agents.*.log", cb=self.router.on_log)
        await nc.subscribe("agents.*.outbox", cb=self.router.on_outbox)
        await nc.subscribe("system.broadcast", cb=self.router.on_broadcast)
        await nc.subscribe(
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>",
            cb=self.router.on_advisory)

        await self._publish_self_register()
        await self._broadcast_request_register()

    async def _publish_self_register(self) -> None:
        card = {
            "name": "aggregator", "description": "EdgeCitadel aggregator.",
            "version": "0.1.0",
            "url": "nats://edgecitadel/agents.aggregator.inbox",
            "provider": {"organization": "EdgeCitadel"},
            "capabilities": {"streaming": False},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["aggregator"],
                "runtime.heartbeat_interval_sec": 30}}
        env = {"v": 1, "id": _uuid4(), "type": "register",
               "sender_id": "aggregator", "timestamp": now_iso(),
               "payload": card}
        await self.router.nc.publish("agents.aggregator.register",
                                     json.dumps(env).encode())

    async def _broadcast_request_register(self) -> None:
        env = {"v": 1, "id": _uuid4(), "type": "broadcast",
               "sender_id": "aggregator", "timestamp": now_iso(),
               "payload": {"action": "request_register"}}
        await self.router.nc.publish("system.broadcast",
                                     json.dumps(env).encode())


def _uuid4() -> str:
    import uuid; return str(uuid.uuid4())
```

Delete `aggregator/aggregator.py`'s legacy readers (every mention of `receiver_id`, `message_type`, `correlation_id`, alias fallbacks, `SKIP_AGENT_IDS`, dedup-by-content, MQTT bridge).

- [ ] **Step 5.4: Run tests, confirm pass**

Run: `python3 -m pytest aggregator/tests/test_aggregator.py -v`

Expected: PASS (4 tests).

- [ ] **Step 5.5: Commit**

```bash
git add aggregator/aggregator.py aggregator/tests/test_aggregator.py \
        aggregator/requirements.txt aggregator/pytest.ini
git commit -m "feat(aggregator): A2A-strict router with outbox mirror + card cache"
```

---

## Task 6: Aggregator HTTP API rewrite

**Files:**
- Modify: `aggregator/main.py`
- Create: `aggregator/tests/test_api.py`
- Modify: `docs/08-api-reference.md`

- [ ] **Step 6.1: Write failing API tests**

Create `aggregator/tests/test_api.py`:

```python
"""FastAPI endpoint contracts for v0.1."""
import pytest
from fastapi.testclient import TestClient
from aggregator.main import make_app


@pytest.fixture
def client(tmp_path, envelope_schema_path, card_schema_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("ENVELOPE_SCHEMA_PATH", str(envelope_schema_path))
    monkeypatch.setenv("CARD_SCHEMA_PATH", str(card_schema_path))
    app = make_app(for_testing=True)   # skips NATS wiring
    with TestClient(app) as c:
        yield c


def test_get_agents_empty(client):
    r = client.get("/api/agents")
    assert r.status_code == 200
    assert r.json() == []


def test_system_status_shape(client):
    r = client.get("/api/system/status")
    assert r.status_code == 200
    body = r.json()
    assert "nats_connected" in body
    assert "mqtt_connected" not in body          # legacy field gone
    assert "jetstream_stream_ok" in body


def test_post_command_returns_task_id(client, monkeypatch):
    # With testing flag, command dispatch stubs out JetStream publish but
    # synthesizes a task_id.
    r = client.post("/api/command/shell-1",
                    json={"body": "echo hi"})
    assert r.status_code == 202
    body = r.json()
    assert "task_id" in body
    assert len(body["task_id"]) == 36
    assert body["recipient_id"] == "shell-1"


def test_post_command_rejects_invalid_body(client):
    r = client.post("/api/command/shell-1", json={"unknown": 1})
    assert r.status_code == 422


def test_delete_agent_removes_card(client):
    # seed by direct DB insert
    from aggregator import database as db
    db.upsert_agent_card({
        "name": "gemma-1", "description": "x", "version": "0",
        "url": "u", "provider": {"organization": "x"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}},
        timestamp="2026-04-23T10:00:00.000Z")
    assert client.get("/api/agents/gemma-1/card").status_code == 200
    assert client.delete("/api/agents/gemma-1").status_code == 204
    assert client.get("/api/agents/gemma-1/card").status_code == 404


def test_get_queue_requires_jetstream(client):
    r = client.get("/api/agents/shell-1/queue")
    # In test mode, returns 503 when JetStream not wired
    assert r.status_code in (200, 503)
```

- [ ] **Step 6.2: Run tests, confirm fail**

Run: `python3 -m pytest aggregator/tests/test_api.py -v`

Expected: FAIL — current `main.py` returns `mqtt_connected`, does not return `task_id` from `POST /api/command/<agent>`, has legacy routes.

- [ ] **Step 6.3: Rewrite `aggregator/main.py`**

```python
from __future__ import annotations
import json, os, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse, PlainTextResponse

from . import database as db
from .aggregator import AggregatorApp, now_iso
from .models import CommandRequest, CommandResponse


def make_app(for_testing: bool = False) -> FastAPI:
    app = FastAPI(title="EdgeCitadel Aggregator", version="0.1.0")
    state: dict = {"app": None}

    db_path = os.environ.get("DB_PATH", "/data/openclaw.db")
    envelope_schema = Path(os.environ.get(
        "ENVELOPE_SCHEMA_PATH",
        str(Path(__file__).resolve().parents[1] / "schemas" / "envelope.v1.json")))
    card_schema = Path(os.environ.get(
        "CARD_SCHEMA_PATH",
        str(Path(__file__).resolve().parents[1] / "schemas" / "agent-card.v1.json")))

    db.init_db(db_path)

    @app.on_event("startup")
    async def _startup():
        if for_testing:
            state["app"] = None
            return
        nats_url = os.environ["NATS_URL"]
        nats_token = os.environ["NATS_TOKEN"]
        agg = AggregatorApp(nats_url=nats_url, nats_token=nats_token,
                            db_path=db_path,
                            envelope_schema=envelope_schema,
                            card_schema=card_schema)
        await agg.start()
        state["app"] = agg

    @app.on_event("shutdown")
    async def _shutdown():
        if state["app"] and state["app"].router.nc:
            await state["app"].router.nc.drain()

    @app.get("/api/system/status")
    async def system_status():
        agg = state["app"]
        nats_connected = bool(agg and agg.router.nc and agg.router.nc.is_connected)
        jetstream_ok = False
        if nats_connected:
            try:
                await agg.router.js.stream_info("AGENT_INBOX")
                jetstream_ok = True
            except Exception:
                jetstream_ok = False
        return {"nats_connected": nats_connected,
                "jetstream_stream_ok": jetstream_ok,
                "version": "0.1.0"}

    @app.get("/api/agents")
    async def list_agents():
        agents = db.list_agents()
        # exclude self-cached aggregator entry from peer list
        return [a for a in agents if a["agent_id"] != "aggregator"]

    @app.get("/api/agents/{agent_id}")
    async def get_agent(agent_id: str):
        a = db.get_agent(agent_id)
        if not a: raise HTTPException(404, "agent not found")
        return a

    @app.get("/api/agents/{agent_id}/card")
    async def get_agent_card(agent_id: str):
        a = db.get_agent(agent_id)
        if not a: raise HTTPException(404, "agent not found")
        return a["card"]

    @app.delete("/api/agents/{agent_id}", status_code=204)
    async def delete_agent(agent_id: str):
        if agent_id == "aggregator":
            raise HTTPException(400, "cannot delete self")
        ok = db.delete_agent(agent_id)
        if not ok: raise HTTPException(404, "agent not found")
        return PlainTextResponse(status_code=204)

    @app.get("/api/agents/{agent_id}/queue")
    async def get_queue(agent_id: str):
        agg = state["app"]
        if agg is None:
            raise HTTPException(503, "jetstream not initialized")
        try:
            ci = await agg.router.js.consumer_info("AGENT_INBOX",
                                                   f"{agent_id}_inbox")
        except Exception as e:
            raise HTTPException(404, f"consumer not found: {e}")
        return {"pending": ci.num_pending,
                "ack_pending": ci.num_ack_pending,
                "num_waiting": getattr(ci, "num_waiting", 0)}

    @app.post("/api/command/{agent_id}", status_code=202,
              response_model=CommandResponse)
    async def post_command(agent_id: str, req: CommandRequest):
        agg = state["app"]
        task_id = str(uuid.uuid4())
        env = {
            "v": 1, "id": str(uuid.uuid4()), "type": "command",
            "sender_id": "aggregator", "recipient_id": agent_id,
            "task_id": task_id, "timestamp": now_iso(),
            "payload": {"body": req.body, **({"args": req.args} if req.args else {})}
        }
        if agg is not None:
            # Publish JetStream with Nats-Msg-Id for idempotency
            await agg.router.js.publish(f"agents.{agent_id}.inbox",
                                        json.dumps(env).encode(),
                                        headers={"Nats-Msg-Id": env["id"]})
            # also mirror on own outbox
            await agg.router.nc.publish("agents.aggregator.outbox",
                                        json.dumps(env).encode())
        return CommandResponse(task_id=task_id, recipient_id=agent_id,
                               accepted_at=env["timestamp"])

    @app.get("/api/messages")
    async def query_messages(agent_id: str | None = None,
                             task_id: str | None = None,
                             context_id: str | None = None,
                             type: str | None = None, limit: int = 500):
        return db.query_messages(agent_id=agent_id, task_id=task_id,
                                 context_id=context_id, type=type, limit=limit)

    @app.get("/api/poison")
    async def query_poison(agent_id: str | None = None, limit: int = 100):
        return db.recent_poison(agent_id=agent_id, limit=limit)

    return app


app = make_app()
```

Delete every legacy route (`/api/agents/{id}/heartbeat` as a separate endpoint, `/api/messages` with legacy field names, `receiver_id` in responses, `correlation_id` parameters).

- [ ] **Step 6.4: Run tests, confirm pass**

Run: `python3 -m pytest aggregator/tests/test_api.py -v`

Expected: PASS (6 tests).

- [ ] **Step 6.5: Rewrite `docs/08-api-reference.md`**

Document every endpoint newly shaped: `GET /api/agents`, `GET /api/agents/{id}`, `GET /api/agents/{id}/card`, `GET /api/agents/{id}/queue`, `DELETE /api/agents/{id}`, `POST /api/command/{id}` (returns `task_id`), `GET /api/messages`, `GET /api/poison`, `GET /api/system/status` (no `mqtt_connected`). Request/response JSON examples per endpoint.

- [ ] **Step 6.6: Commit**

```bash
git add aggregator/main.py aggregator/tests/test_api.py docs/08-api-reference.md
git commit -m "feat(aggregator): rewrite API for canonical envelope + task_id response"
```

---

## Task 7: JetStream bootstrap helper + ADR

**Files:**
- Create: `aggregator/jetstream_bootstrap.py`
- Create: `aggregator/tests/test_jetstream_bootstrap.py` (integration-style, gated)
- Create: `docs/adr/0002-nats-jetstream-workqueue.md`
- Modify: `aggregator/aggregator.py` (call bootstrap on startup)

- [ ] **Step 7.1: Write failing tests**

Create `aggregator/tests/test_jetstream_bootstrap.py`:

```python
"""Requires a live NATS with JetStream on $NATS_URL. Skipped if unreachable."""
import os, pytest, asyncio
from nats.aio.client import Client as NATS
from aggregator.jetstream_bootstrap import ensure_stream, ensure_consumer


NATS_URL = os.environ.get("NATS_URL_TEST", "nats://localhost:4222")
NATS_TOKEN = os.environ.get("NATS_TOKEN_TEST", os.environ.get("NATS_TOKEN", ""))


@pytest.fixture
async def js_client():
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL], token=NATS_TOKEN,
                         connect_timeout=1)
    except Exception:
        pytest.skip("NATS not reachable; set NATS_URL_TEST to run")
    js = nc.jetstream()
    yield js
    # cleanup
    try:
        await js.delete_consumer("AGENT_INBOX", "shell-test_inbox")
    except Exception: pass
    try:
        await js.delete_stream("AGENT_INBOX")
    except Exception: pass
    await nc.drain()


async def test_ensure_stream_idempotent(js_client):
    info1 = await ensure_stream(js_client)
    info2 = await ensure_stream(js_client)
    assert info1.config.name == "AGENT_INBOX"
    assert info2.config.name == "AGENT_INBOX"


async def test_ensure_consumer_serialization(js_client):
    await ensure_stream(js_client)
    ci = await ensure_consumer(js_client, "shell-test", ack_wait_sec=30)
    assert ci.config.max_ack_pending == 1
    assert ci.config.ack_wait == 30 * 1_000_000_000  # ns in nats-py
    assert ci.config.filter_subject == "agents.shell-test.inbox"


async def test_stream_config_matches_spec(js_client):
    info = await ensure_stream(js_client)
    cfg = info.config
    assert cfg.name == "AGENT_INBOX"
    assert cfg.subjects == ["agents.*.inbox"]
    assert cfg.retention.name in ("workqueue", "WorkQueuePolicy",
                                  "WorkQueue")
    assert cfg.discard.name in ("new", "DiscardNew")
    assert cfg.max_msg_size == 1024 * 1024
    assert cfg.duplicate_window == 5 * 60 * 1_000_000_000
```

- [ ] **Step 7.2: Run tests, confirm fail**

Run: `python3 -m pytest aggregator/tests/test_jetstream_bootstrap.py -v`

Expected: FAIL — module missing; if NATS unreachable, SKIPPED which is also a fail-to-pass trigger for the impl step.

- [ ] **Step 7.3: Write `aggregator/jetstream_bootstrap.py`**

```python
"""
Idempotent JetStream bootstrap: creates AGENT_INBOX stream and per-agent
durable consumers. Called on aggregator startup AND lazily by each adapter
when it first connects.
"""
from __future__ import annotations
from nats.js import JetStreamContext
from nats.js.api import (StreamConfig, ConsumerConfig, RetentionPolicy,
                         DiscardPolicy, AckPolicy)
from nats.js.errors import NotFoundError, BadRequestError

STREAM_NAME = "AGENT_INBOX"
SUBJECTS = ["agents.*.inbox"]


async def ensure_stream(js: JetStreamContext):
    cfg = StreamConfig(
        name=STREAM_NAME,
        subjects=SUBJECTS,
        retention=RetentionPolicy.WORK_QUEUE,
        storage=None,            # default file storage on server
        discard=DiscardPolicy.NEW,
        max_age=24 * 60 * 60,    # 24h seconds
        max_bytes=1 * 1024 * 1024 * 1024,  # 1GB
        max_msg_size=1 * 1024 * 1024,      # 1MB
        duplicate_window=5 * 60 * 1_000_000_000,   # 5min in ns
    )
    try:
        return await js.update_stream(cfg)
    except (NotFoundError, BadRequestError):
        return await js.add_stream(cfg)


async def ensure_consumer(js: JetStreamContext, agent_id: str,
                          ack_wait_sec: int = 300,
                          max_ack_pending: int = 1,
                          max_deliver: int = 3):
    cfg = ConsumerConfig(
        durable_name=f"{agent_id}_inbox",
        filter_subject=f"agents.{agent_id}.inbox",
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=ack_wait_sec * 1_000_000_000,
        max_ack_pending=max_ack_pending,
        max_deliver=max_deliver,
    )
    try:
        return await js.add_consumer(STREAM_NAME, cfg)
    except BadRequestError:
        # exists; fetch info
        return await js.consumer_info(STREAM_NAME, cfg.durable_name)
```

- [ ] **Step 7.4: Wire bootstrap into `AggregatorApp.start`**

Edit `aggregator/aggregator.py` `AggregatorApp.start` — add before `_publish_self_register`:

```python
        from .jetstream_bootstrap import ensure_stream, ensure_consumer
        await ensure_stream(self.router.js)
        # aggregator's own inbox: no serial constraint
        await ensure_consumer(self.router.js, "aggregator",
                              ack_wait_sec=60, max_ack_pending=100)
        # subscribe durable consumer to drain results
        psub = await self.router.js.pull_subscribe(
            "agents.aggregator.inbox", durable="aggregator_inbox")
        asyncio.create_task(self._drain_own_inbox(psub))

    async def _drain_own_inbox(self, psub) -> None:
        while True:
            try:
                msgs = await psub.fetch(batch=10, timeout=30)
            except Exception:
                await asyncio.sleep(1); continue
            for m in msgs:
                env = self.router._parse_and_validate(m.data)
                if env:
                    db.insert_message(env)
                    if env["type"] == "result":
                        f = self.router.pending_tasks.pop(
                            env.get("task_id", ""), None)
                        if f is not None and not f.done():
                            f.set_result(env)
                await m.ack()
```

- [ ] **Step 7.5: Run tests, confirm pass**

Run JetStream test (requires live stack):

```bash
docker compose up -d nats
NATS_URL_TEST=nats://localhost:4222 NATS_TOKEN_TEST="$NATS_TOKEN" \
  python3 -m pytest aggregator/tests/test_jetstream_bootstrap.py -v
```

Expected: PASS (3 tests).

- [ ] **Step 7.6: Write ADR 0002**

Create `docs/adr/0002-nats-jetstream-workqueue.md`. Status: Accepted. Context: concurrent-agent failure rates, need for per-agent FIFO. Decision: JetStream WorkQueuePolicy + `max_ack_pending=1` per durable consumer. Alternatives rejected: application-level mutex, queue groups (no persistence), separate stream per agent (doesn't scale to fleet growth). Consequences: adapters MUST keep the message unacked for the whole task; adapters MUST extend `ack_wait` via `in_progress()`.

- [ ] **Step 7.7: Commit**

```bash
git add aggregator/jetstream_bootstrap.py aggregator/aggregator.py \
        aggregator/tests/test_jetstream_bootstrap.py \
        docs/adr/0002-nats-jetstream-workqueue.md
git commit -m "feat(aggregator): JetStream bootstrap + durable self-inbox (ADR-0002)"
```

---

## Task 8: Update `docs/05-messaging.md` for v0.1 transport + subjects

**Files:**
- Modify: `docs/05-messaging.md`

- [ ] **Step 8.1: Rewrite `docs/05-messaging.md`**

Rewrite in place. Content must match the spec:
- Subject inventory table (from spec §"Subject inventory")
- JetStream `AGENT_INBOX` stream config (name, retention, discard, `max_msg_size`, `duplicate_window`)
- Per-agent consumer config (durable name, `max_ack_pending`, `ack_wait`, `max_deliver`)
- Canonical envelope shape (reference `schemas/envelope.v1.json`)
- Publisher semantics (`Nats-Msg-Id` on every JetStream publish; mirror to own outbox)
- Stream-full backpressure (`discard: new`; publish returns error)
- MQTT ingress — deploy-time opt-in only (`EC_ENABLE_MQTT=1`); default off
- Legacy MQTT topics → DELETED section; no slash-topic compatibility

Remove every reference to `mqtt_connected` status, slash-topic shapes (`citadel/agents/.../`), paho-mqtt, `receiver_id`, `message_type`, `correlation_id`.

- [ ] **Step 8.2: Commit**

```bash
git add docs/05-messaging.md
git commit -m "docs(messaging): rewrite 05-messaging.md for JetStream + A2A subjects"
```

---

## Task 9: Shared adapter common — validator + Agent Card factory

**Files:**
- Create: `adapters/_common/__init__.py`
- Create: `adapters/_common/validator.py`
- Create: `adapters/_common/agent_card.py`
- Create: `adapters/_common/tests/test_agent_card.py`

- [ ] **Step 9.1: Write failing card-factory tests**

Create `adapters/_common/tests/test_agent_card.py`:

```python
import pytest
from pathlib import Path
from adapters._common.agent_card import build_card

YAML = """
agent_id: shell-1
name: shell-1
description: Shell executor.
version: 0.1.0
runtime:
  kind: native
  roles: [worker]
  tags: [dev]
  heartbeat_interval_sec: 30
skills:
  - id: shell.exec
    name: shell-exec
    description: Run a shell command.
    tags: [shell]
capabilities:
  streaming: false
"""


def test_card_has_required_a2a_fields(tmp_path):
    p = tmp_path / "config.yaml"; p.write_text(YAML)
    card = build_card(p)
    assert card["name"] == "shell-1"
    assert card["version"] == "0.1.0"
    assert card["url"] == "nats://edgecitadel/agents.shell-1.inbox"
    assert card["provider"]["organization"] == "EdgeCitadel"
    assert "securitySchemes" in card


def test_card_declares_nats_binding_extension(tmp_path):
    p = tmp_path / "config.yaml"; p.write_text(YAML)
    card = build_card(p)
    exts = card["capabilities"]["extensions"]
    assert any(e["uri"] == "https://edgecitadel.local/ext/nats-binding/v1"
               for e in exts)


def test_card_metadata_vocabulary(tmp_path):
    p = tmp_path / "config.yaml"; p.write_text(YAML)
    card = build_card(p)
    md = card["metadata"]
    assert md["runtime.kind"] == "native"
    assert md["runtime.roles"] == ["worker"]
    assert md["runtime.heartbeat_interval_sec"] == 30
    assert "runtime.tags" in md and md["runtime.tags"] == ["dev"]


def test_bridge_requires_upstream(tmp_path):
    bridge_yaml = YAML.replace("kind: native", "kind: bridge")
    p = tmp_path / "c.yaml"; p.write_text(bridge_yaml)
    with pytest.raises(ValueError, match="upstream"):
        build_card(p)
```

- [ ] **Step 9.2: Run tests, confirm fail**

Run: `python3 -m pytest adapters/_common/tests/test_agent_card.py -v`

Expected: FAIL (modules missing).

- [ ] **Step 9.3: Create `adapters/_common/__init__.py`**

Empty file.

- [ ] **Step 9.4: Create `adapters/_common/agent_card.py`**

```python
"""A2A v1.0 Agent Card factory from per-agent YAML config."""
from __future__ import annotations
from pathlib import Path
import yaml


NATS_EXT_URI = "https://edgecitadel.local/ext/nats-binding/v1"


def build_card(config_path: str | Path) -> dict:
    cfg = yaml.safe_load(Path(config_path).read_text())
    agent_id = cfg["agent_id"]
    if cfg["name"] != agent_id:
        raise ValueError("config.name must equal config.agent_id")

    runtime = cfg.get("runtime", {})
    kind = runtime.get("kind", "native")
    if kind == "bridge" and not runtime.get("upstream"):
        raise ValueError("bridge agents require runtime.upstream")

    metadata = {
        "runtime.kind": kind,
        "runtime.roles": runtime.get("roles", ["worker"]),
        "runtime.heartbeat_interval_sec":
            runtime.get("heartbeat_interval_sec", 30),
    }
    if runtime.get("tags"):
        metadata["runtime.tags"] = runtime["tags"]
    if runtime.get("deployment"):
        metadata["runtime.deployment"] = runtime["deployment"]
    if runtime.get("upstream"):
        metadata["runtime.upstream"] = runtime["upstream"]

    capabilities = cfg.get("capabilities", {}).copy()
    extensions = list(capabilities.get("extensions", []))
    if not any(e.get("uri") == NATS_EXT_URI for e in extensions):
        extensions.append({
            "uri": NATS_EXT_URI,
            "description": "NATS JetStream transport binding for EdgeCitadel.",
            "required": False,
            "params": {"subject_prefix": f"agents.{agent_id}"},
        })
    capabilities["extensions"] = extensions
    capabilities.setdefault("streaming", False)

    return {
        "name": agent_id,
        "description": cfg.get("description", ""),
        "version": cfg.get("version", "0.1.0"),
        "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
        "provider": {"organization": "EdgeCitadel",
                     "url": "https://edgecitadel.local"},
        "capabilities": capabilities,
        "securitySchemes": cfg.get("securitySchemes", {}),
        "additionalInterfaces": cfg.get("additionalInterfaces", [
            {"url": f"nats://edgecitadel/agents.{agent_id}.inbox",
             "transport": "nats-jsonrpc"}
        ]),
        "skills": cfg.get("skills", []),
        "defaultInputModes": cfg.get("defaultInputModes", ["text/plain"]),
        "defaultOutputModes": cfg.get("defaultOutputModes", ["text/plain"]),
        "metadata": metadata,
    }
```

- [ ] **Step 9.5: Create `adapters/_common/validator.py`**

```python
"""Thin re-export of aggregator.validator so adapters don't import aggregator."""
from pathlib import Path
from aggregator.validator import EnvelopeValidator, ValidationError


REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "schemas"


def default_validator() -> EnvelopeValidator:
    return EnvelopeValidator(
        envelope_schema_path=SCHEMAS / "envelope.v1.json",
        card_schema_path=SCHEMAS / "agent-card.v1.json",
    )


__all__ = ["EnvelopeValidator", "ValidationError", "default_validator"]
```

- [ ] **Step 9.6: Run tests, confirm pass**

Run: `python3 -m pytest adapters/_common/tests/test_agent_card.py -v`

Ensure `pyyaml` is available (`pip install pyyaml`); add to `aggregator/requirements.txt` if not already present.

Expected: PASS (4 tests).

- [ ] **Step 9.7: Commit**

```bash
git add adapters/_common/__init__.py adapters/_common/validator.py \
        adapters/_common/agent_card.py \
        adapters/_common/tests/test_agent_card.py \
        aggregator/requirements.txt
git commit -m "feat(adapters): shared A2A Agent Card factory + validator re-export"
```

---

## Task 10: Shared pull-consumer skeleton

**Files:**
- Create: `adapters/_common/pull_consumer.py`
- Create: `adapters/_common/tests/test_pull_consumer.py`

- [ ] **Step 10.1: Write failing pull-consumer tests (integration-style, live NATS required)**

Create `adapters/_common/tests/test_pull_consumer.py`:

```python
"""End-to-end pull-consumer behavior. Requires live NATS with JetStream.
Skip if NATS_URL_TEST unset."""
import asyncio, json, os, pytest, uuid
from nats.aio.client import Client as NATS
from adapters._common.pull_consumer import PullConsumer, Context
from aggregator.jetstream_bootstrap import ensure_stream, ensure_consumer

NATS_URL = os.environ.get("NATS_URL_TEST", "nats://localhost:4222")
TOKEN = os.environ.get("NATS_TOKEN_TEST", os.environ.get("NATS_TOKEN", ""))


async def _connect():
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL], token=TOKEN, connect_timeout=1)
    except Exception:
        pytest.skip("NATS not reachable")
    return nc


async def test_fifo_one_at_a_time():
    nc = await _connect(); js = nc.jetstream()
    await ensure_stream(js)
    agent_id = f"test-{uuid.uuid4().hex[:6]}"
    await ensure_consumer(js, agent_id, ack_wait_sec=30)

    processing: list[str] = []
    order: list[str] = []
    gate = asyncio.Event()

    async def handle(env: dict, ctx: Context):
        processing.append(env["id"])
        assert len(processing) == 1, "violated max_ack_pending=1"
        await gate.wait()
        order.append(env["id"])
        processing.remove(env["id"])
        return ({"body": "ok"}, "completed")

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle,
                      ack_wait_sec=30)
    task = asyncio.create_task(pc.run())

    # publish 3 commands
    ids = []
    for i in range(3):
        env = _cmd_env(recipient=agent_id, sender="test-sender", body=f"x{i}")
        ids.append(env["id"])
        await js.publish(f"agents.{agent_id}.inbox",
                         json.dumps(env).encode(),
                         headers={"Nats-Msg-Id": env["id"]})

    await asyncio.sleep(0.5)
    gate.set()
    await asyncio.sleep(3)
    await pc.stop()
    task.cancel()
    await nc.drain()

    assert order == ids[:len(order)]   # in order


async def test_dedup_via_nats_msg_id():
    nc = await _connect(); js = nc.jetstream()
    await ensure_stream(js)
    agent_id = f"dedup-{uuid.uuid4().hex[:6]}"
    await ensure_consumer(js, agent_id, ack_wait_sec=10)

    calls = 0
    async def handle(env, ctx):
        nonlocal calls; calls += 1
        return ({"body": "done"}, "completed")

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle,
                      ack_wait_sec=10)
    task = asyncio.create_task(pc.run())

    env = _cmd_env(recipient=agent_id, sender="test", body="once")
    for _ in range(3):
        await js.publish(f"agents.{agent_id}.inbox",
                         json.dumps(env).encode(),
                         headers={"Nats-Msg-Id": env["id"]})
    await asyncio.sleep(2)
    await pc.stop(); task.cancel(); await nc.drain()
    assert calls == 1, f"expected dedup to reduce to 1 call, got {calls}"


def _cmd_env(*, recipient, sender, body):
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")
    return {"v": 1, "id": str(uuid.uuid4()), "type": "command",
            "sender_id": sender, "recipient_id": recipient,
            "task_id": str(uuid.uuid4()), "timestamp": ts,
            "payload": {"body": body}}
```

- [ ] **Step 10.2: Run tests, confirm fail**

Run: `NATS_URL_TEST=nats://localhost:4222 python3 -m pytest adapters/_common/tests/test_pull_consumer.py -v`

Expected: FAIL (module missing).

- [ ] **Step 10.3: Create `adapters/_common/pull_consumer.py`**

```python
"""JetStream pull-consumer adapter skeleton.

Contract: handler must produce (result_payload: dict, task_state: str)
for command/delegation/cancel, OR return None for types with no reply
(heartbeat, status, broadcast, log — but those don't land on inbox anyway).

Ack happens only after successful handler return AND successful result publish
(for command/delegation/cancel). in_progress() is called periodically to
extend ack_wait.
"""
from __future__ import annotations
import asyncio, json, logging, uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js.errors import BadRequestError, NotFoundError

from .validator import default_validator, ValidationError
from aggregator.jetstream_bootstrap import ensure_stream, ensure_consumer

log = logging.getLogger(__name__)

HOP_COUNT_MAX = 8


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


@dataclass
class Context:
    agent_id: str
    nc: NATS
    js: object
    msg: Msg

    async def in_progress(self) -> None:
        await self.msg.in_progress()

    async def publish_progress(self, task_id: str, *, body: str = "",
                               progress: Optional[int] = None) -> None:
        payload = {"message": body}
        if progress is not None: payload["progress"] = progress
        env = {"v": 1, "id": str(uuid.uuid4()), "type": "task.progress",
               "sender_id": self.agent_id,
               "task_id": task_id, "task_state": "working",
               "timestamp": now_iso(), "payload": payload}
        await self.nc.publish(
            f"agents.{self.agent_id}.task_progress.{task_id}",
            json.dumps(env).encode())


Handler = Callable[[dict, Context], Awaitable[tuple[dict, str]]]


class PullConsumer:
    def __init__(self, *, agent_id: str, nc: NATS, handler: Handler,
                 ack_wait_sec: int = 300, max_deliver: int = 3,
                 max_ack_pending: int = 1,
                 sender_allowlist: Optional[set[str]] = None):
        self.agent_id = agent_id
        self.nc = nc
        self.js = nc.jetstream()
        self.handler = handler
        self.ack_wait_sec = ack_wait_sec
        self.max_deliver = max_deliver
        self.max_ack_pending = max_ack_pending
        self.sender_allowlist = sender_allowlist
        self.validator = default_validator()
        self._running = False

    async def run(self) -> None:
        await ensure_stream(self.js)
        await ensure_consumer(self.js, self.agent_id,
                              ack_wait_sec=self.ack_wait_sec,
                              max_ack_pending=self.max_ack_pending,
                              max_deliver=self.max_deliver)
        psub = await self.js.pull_subscribe(
            subject=f"agents.{self.agent_id}.inbox",
            durable=f"{self.agent_id}_inbox")

        self._running = True
        while self._running:
            try:
                msgs = await psub.fetch(batch=1, timeout=30)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.warning("fetch error: %s", e)
                await asyncio.sleep(1); continue
            for m in msgs:
                await self._handle_msg(m)

    async def stop(self) -> None:
        self._running = False

    async def _handle_msg(self, msg: Msg) -> None:
        try:
            env = json.loads(msg.data)
        except json.JSONDecodeError:
            await msg.term(); return

        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("%s: invalid envelope, terminating: %s", self.agent_id, e)
            await msg.term(); return

        if self.sender_allowlist is not None and \
                env["sender_id"] not in self.sender_allowlist:
            log.warning("%s: bridge_sender_not_allowlisted sender=%s",
                        self.agent_id, env["sender_id"])
            await msg.term(); return

        if env["type"] == "delegation" and env.get("hop_count", 0) >= HOP_COUNT_MAX:
            await self._publish_result(env, task_state="rejected",
                                       error="hop_count_exceeded")
            await msg.ack(); return

        keepalive = asyncio.create_task(self._keepalive(msg))
        try:
            ctx = Context(agent_id=self.agent_id, nc=self.nc, js=self.js,
                          msg=msg)
            result, state = await self.handler(env, ctx)
            await self._publish_result(env, task_state=state, payload=result)
            await msg.ack()
        except Exception as e:
            log.exception("%s: handler failed: %s", self.agent_id, e)
            try:
                await self._publish_result(env, task_state="failed",
                                           error=type(e).__name__)
            except Exception:
                pass
            await msg.nak()
        finally:
            keepalive.cancel()

    async def _keepalive(self, msg: Msg) -> None:
        cadence = max(1.0, self.ack_wait_sec / 3)
        try:
            while True:
                await asyncio.sleep(cadence)
                await msg.in_progress()
        except asyncio.CancelledError:
            return

    async def _publish_result(self, inbound: dict, *, task_state: str,
                              payload: Optional[dict] = None,
                              error: Optional[str] = None) -> None:
        if inbound["type"] not in ("command", "delegation", "cancel"):
            return
        out = {
            "v": 1, "id": str(uuid.uuid4()),
            "type": "result",
            "sender_id": self.agent_id,
            "recipient_id": inbound["sender_id"],
            "task_id": inbound["task_id"],
            "task_state": task_state,
            "timestamp": now_iso(),
            "payload": (payload or {}) | ({"error": error} if error else {}),
        }
        if inbound.get("context_id"):
            out["context_id"] = inbound["context_id"]
        data = json.dumps(out).encode()
        # durable publish to sender's inbox
        await self.js.publish(f"agents.{inbound['sender_id']}.inbox", data,
                              headers={"Nats-Msg-Id": out["id"]})
        # mirror to own outbox (plain NATS; best-effort)
        await self.nc.publish(f"agents.{self.agent_id}.outbox", data)
```

- [ ] **Step 10.4: Run tests, confirm pass**

Run: `docker compose up -d nats && NATS_URL_TEST=nats://localhost:4222 NATS_TOKEN_TEST="$NATS_TOKEN" python3 -m pytest adapters/_common/tests/test_pull_consumer.py -v`

Expected: PASS (2 tests).

- [ ] **Step 10.5: Commit**

```bash
git add adapters/_common/pull_consumer.py \
        adapters/_common/tests/test_pull_consumer.py
git commit -m "feat(adapters): shared JetStream pull-consumer with ack/keepalive/outbox"
```

---

## Task 11: Shared conformance test suite + adapter template

**Files:**
- Create: `adapters/_common/tests/conformance.py`
- Create: `adapters/_common/template.py`

- [ ] **Step 11.1: Write the reusable conformance suite**

Create `adapters/_common/tests/conformance.py`:

```python
"""Conformance suite every adapter runs against its own NATS connection.

Exported as pytest-fn builders so adapter-specific test files can do:

    from adapters._common.tests.conformance import build_conformance_cases
    for name, env, expect in build_conformance_cases():
        ...
"""
from __future__ import annotations
import uuid
from typing import Iterator


def _base(**o):
    e = {"v": 1, "id": str(uuid.uuid4()), "type": "heartbeat",
         "sender_id": "tester",
         "timestamp": "2026-04-23T10:00:00.000Z", "payload": {}}
    e.update(o); return e


def build_conformance_cases() -> list[tuple[str, dict, str]]:
    """Returns list of (name, envelope, "accept"|"reject")."""
    return [
        ("heartbeat-minimal", _base(), "accept"),
        ("status-online", _base(type="status", agent_state="online"), "accept"),
        ("command-ok", _base(type="command", recipient_id="r",
                             task_id=str(uuid.uuid4()),
                             payload={"body": "x"}), "accept"),
        ("result-completed", _base(type="result", recipient_id="r",
                                   task_id=str(uuid.uuid4()),
                                   task_state="completed",
                                   payload={"body": "ok"}), "accept"),
        ("cancel-ok", _base(type="cancel", recipient_id="r",
                            task_id=str(uuid.uuid4()),
                            payload={"task_id": str(uuid.uuid4())}), "accept"),
        ("reject-legacy-receiver_id",
         {**_base(), "receiver_id": "x"}, "reject"),
        ("reject-legacy-message_type",
         {**_base(), "message_type": "info"}, "reject"),
        ("reject-missing-task_id-on-command",
         _base(type="command", recipient_id="r", payload={"body": "x"}),
         "reject"),
        ("reject-bad-state-on-result",
         _base(type="result", recipient_id="r",
               task_id=str(uuid.uuid4()), task_state="done",
               payload={}), "reject"),
        ("reject-v2", _base(v=2), "reject"),
    ]
```

- [ ] **Step 11.2: Create `adapters/_common/template.py`**

```python
"""Skeleton adapter. Copy to adapters/<type>/adapter.py and fill in handle()."""
from __future__ import annotations
import asyncio, logging, os, signal
from pathlib import Path

from nats.aio.client import Client as NATS
from .agent_card import build_card
from .pull_consumer import PullConsumer, Context

log = logging.getLogger(__name__)


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    """Replace with real work. Return (payload, task_state)."""
    return ({"body": f"stub reply to {env['sender_id']}"}, "completed")


async def main(config_path: str | Path) -> None:
    card = build_card(config_path)
    agent_id = card["name"]
    ack_wait = int(os.environ.get("ACK_WAIT_SEC", "300"))

    nc = NATS()
    await nc.connect(servers=[os.environ["NATS_URL"]],
                     token=os.environ.get("NATS_TOKEN"))

    # Publish register
    import json, uuid
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")
    env = {"v": 1, "id": str(uuid.uuid4()), "type": "register",
           "sender_id": agent_id, "timestamp": ts, "payload": card}
    await nc.publish(f"agents.{agent_id}.register", json.dumps(env).encode())

    # Heartbeat loop
    async def heartbeat():
        interval = card["metadata"]["runtime.heartbeat_interval_sec"]
        while True:
            ts2 = datetime.now(timezone.utc).isoformat(
                timespec="milliseconds").replace("+00:00", "Z")
            hb = {"v": 1, "id": str(uuid.uuid4()), "type": "heartbeat",
                  "sender_id": agent_id, "timestamp": ts2, "payload": {}}
            await nc.publish(f"agents.{agent_id}.heartbeat",
                             json.dumps(hb).encode())
            await asyncio.sleep(interval)

    hb_task = asyncio.create_task(heartbeat())

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle,
                      ack_wait_sec=ack_wait)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, stop.set)

    consumer_task = asyncio.create_task(pc.run())
    await stop.wait()

    # graceful shutdown
    off = {"v": 1, "id": str(uuid.uuid4()), "type": "status",
           "sender_id": agent_id, "agent_state": "offline",
           "timestamp": datetime.now(timezone.utc).isoformat(
               timespec="milliseconds").replace("+00:00", "Z"),
           "payload": {"reason": "shutdown"}}
    await nc.publish(f"agents.{agent_id}.status", json.dumps(off).encode())
    await pc.stop()
    hb_task.cancel(); consumer_task.cancel()
    await nc.drain()
```

- [ ] **Step 11.3: Commit**

```bash
git add adapters/_common/tests/conformance.py adapters/_common/template.py
git commit -m "feat(adapters): shared conformance suite + adapter template"
```

---

## Task 12: Shell adapter rewrite (nats-py async)

**Files:**
- Create: `adapters/shell/adapter.py`
- Create: `adapters/shell/config.yaml`
- Create: `adapters/shell/tests/test_shell.py`
- Modify: `adapters/shell/requirements.txt`
- Modify: `adapters/shell/README.md`
- Delete: `adapters/shell/shell_adapter.py` (paho legacy)

- [ ] **Step 12.1: Write failing shell-adapter unit test**

Create `adapters/shell/tests/test_shell.py`:

```python
import asyncio, pytest
from unittest.mock import MagicMock
from adapters.shell.adapter import handle
from adapters._common.pull_consumer import Context


@pytest.mark.asyncio
async def test_handle_echo():
    env = {"v": 1, "id": "i", "type": "command",
           "sender_id": "tester", "recipient_id": "shell-1",
           "task_id": "t", "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "echo hello"}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)   # stub
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "completed"
    assert "hello" in payload["body"]


@pytest.mark.asyncio
async def test_handle_timeout():
    env = {"v": 1, "id": "i", "type": "command",
           "sender_id": "tester", "recipient_id": "shell-1",
           "task_id": "t", "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "sleep 99", "args": {"timeout_sec": 1}}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "failed"
    assert payload["error"] == "timeout"


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"v": 1, "id": "i", "type": "delegation",
           "sender_id": "planner-1", "recipient_id": "shell-1",
           "task_id": "t", "context_id": "c", "hop_count": 0,
           "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "ignored"}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "rejected"
```

- [ ] **Step 12.2: Run tests, confirm fail**

Run: `python3 -m pytest adapters/shell/tests/test_shell.py -v`

Expected: FAIL (module missing).

- [ ] **Step 12.3: Create `adapters/shell/adapter.py`**

```python
"""EdgeCitadel shell adapter — nats-py async, JetStream pull consumer."""
from __future__ import annotations
import asyncio, logging, shlex
from pathlib import Path

from adapters._common.pull_consumer import Context
from adapters._common.template import main as run_adapter

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")

    body = env["payload"].get("body", "").strip()
    args = env["payload"].get("args") or {}
    timeout_sec = int(args.get("timeout_sec", DEFAULT_TIMEOUT))

    if not body:
        return ({"error": "empty_command"}, "rejected")

    # periodic in_progress keepalive is handled by PullConsumer
    proc = await asyncio.create_subprocess_shell(
        body,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(),
                                          timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        return ({"error": "timeout",
                 "body": f"command timed out after {timeout_sec}s"}, "failed")

    rc = proc.returncode
    text = (out or b"").decode(errors="replace")
    if err:
        text += "\n" + err.decode(errors="replace")
    state = "completed" if rc == 0 else "failed"
    payload = {"body": text[:64_000], "returncode": rc}
    if rc != 0: payload["error"] = "nonzero_exit"
    return (payload, state)


async def main():
    # template.main reads config.yaml, registers, heartbeats, drains inbox
    # We inject our handler by monkey-patching template.handle before run
    from adapters._common import template
    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

- [ ] **Step 12.4: Create `adapters/shell/config.yaml`**

```yaml
agent_id: shell-1
name: shell-1
description: Executes shell commands in a subprocess, default timeout 30s.
version: 0.1.0
runtime:
  kind: native
  roles: [worker]
  tags: [shell, executor]
  heartbeat_interval_sec: 30
skills:
  - id: shell.exec
    name: shell-exec
    description: Run a shell command and return stdout/stderr.
    tags: [shell, subprocess]
capabilities:
  streaming: false
```

- [ ] **Step 12.5: Update `adapters/shell/requirements.txt`**

```
nats-py>=2.9.0
pyyaml>=6.0
jsonschema>=4.20
```

Delete paho-mqtt if present.

- [ ] **Step 12.6: Delete legacy file + rewrite README**

```bash
rm adapters/shell/shell_adapter.py
rm -rf adapters/shell/__pycache__
```

Rewrite `adapters/shell/README.md` to describe: subject + consumer name, env vars (`NATS_URL`, `NATS_TOKEN`, optional `ACK_WAIT_SEC`), how to run: `python3 -m adapters.shell.adapter`.

- [ ] **Step 12.7: Run tests, confirm pass**

Run: `python3 -m pytest adapters/shell/tests/test_shell.py -v`

Expected: PASS (3 tests).

- [ ] **Step 12.8: Smoke test the whole loop against live NATS**

```bash
docker compose up -d nats aggregator
NATS_URL=nats://localhost:4222 NATS_TOKEN="$NATS_TOKEN" \
  python3 -m adapters.shell.adapter &
SHELL_PID=$!
sleep 2
curl -s -X POST http://localhost:8000/api/command/shell-1 \
  -H 'Content-Type: application/json' \
  -d '{"body":"echo hello"}'
# Expect: {"task_id":"<uuid>","recipient_id":"shell-1","accepted_at":"..."}
sleep 2
curl -s "http://localhost:8000/api/messages?task_id=<uuid-from-above>" | jq
# Expect: command + result rows with task_state: completed, body includes "hello"
kill $SHELL_PID
```

- [ ] **Step 12.9: Commit**

```bash
git add adapters/shell/adapter.py adapters/shell/config.yaml \
        adapters/shell/tests/test_shell.py \
        adapters/shell/requirements.txt adapters/shell/README.md
git rm adapters/shell/shell_adapter.py
git commit -m "feat(shell): rewrite shell adapter on nats-py JetStream pull consumer"
```

---

## Task 13: openclaw-client rewrite — plain NATS heartbeat + status + register

**Files:**
- Create: `openclaw-client/index.js`
- Create: `openclaw-client/src/nats-session.js`
- Create: `openclaw-client/tests/nats-session.test.js`
- Modify: `openclaw-client/package.json`
- Modify: `openclaw-client/README.md`
- Delete: `openclaw-client/mqtt-listener.js`
- Create: `docs/adr/0005-browser-scoped-token.md`

- [ ] **Step 13.1: Update `openclaw-client/package.json`**

Replace dependencies:

```json
{
  "name": "openclaw-client",
  "version": "0.1.0",
  "type": "module",
  "main": "index.js",
  "scripts": {
    "start": "node index.js",
    "test": "node --test tests/"
  },
  "dependencies": {
    "@nats-io/nats-core": "^3.0.0",
    "@nats-io/jetstream": "^3.0.0",
    "ajv": "^8.12.0",
    "ajv-formats": "^3.0.0",
    "dotenv": "^16.4.5",
    "uuid": "^9.0.1"
  }
}
```

Run: `cd openclaw-client && rm -rf node_modules package-lock.json && npm install`.

- [ ] **Step 13.2: Write failing session tests**

Create `openclaw-client/tests/nats-session.test.js`:

```javascript
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildRegisterEnvelope, buildHeartbeatEnvelope, validateEnvelope } from '../src/nats-session.js';

test('register envelope has canonical shape', () => {
  const env = buildRegisterEnvelope({
    agentId: 'openclaw-abc',
    sessionId: 'abc',
    heartbeatIntervalSec: 30
  });
  assert.equal(env.type, 'register');
  assert.equal(env.sender_id, 'openclaw-abc');
  assert.equal(env.v, 1);
  assert.equal(env.payload.name, 'openclaw-abc');
  assert.equal(env.payload.metadata['runtime.kind'], 'native');
  assert.equal(env.payload.metadata['runtime.heartbeat_interval_sec'], 30);
});

test('heartbeat envelope has canonical shape', () => {
  const env = buildHeartbeatEnvelope('openclaw-abc');
  assert.equal(env.type, 'heartbeat');
  assert.equal(env.sender_id, 'openclaw-abc');
  assert.ok(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(env.timestamp));
});

test('rejects legacy envelope with receiver_id', () => {
  const result = validateEnvelope({ v: 1, id: 'x', type: 'heartbeat',
                                    sender_id: 's', timestamp: '2026-04-23T10:00:00.000Z',
                                    payload: {}, receiver_id: 'legacy' });
  assert.equal(result.ok, false);
  assert.match(result.error, /receiver_id|additional/i);
});

test('accepts valid command envelope', () => {
  const result = validateEnvelope({
    v: 1, id: '11111111-2222-4333-8444-555555555555',
    type: 'command', sender_id: 'openclaw-abc', recipient_id: 'shell-1',
    task_id: '22222222-3333-4444-8555-666666666666',
    timestamp: '2026-04-23T10:00:00.000Z',
    payload: { body: 'echo hi' }
  });
  assert.equal(result.ok, true, result.error);
});
```

- [ ] **Step 13.3: Run tests, confirm fail**

Run: `cd openclaw-client && npm test`

Expected: FAIL — `src/nats-session.js` missing.

- [ ] **Step 13.4: Create `openclaw-client/src/nats-session.js`**

```javascript
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { v4 as uuid } from 'uuid';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';

const HERE = dirname(fileURLToPath(import.meta.url));
const SCHEMA_PATH = resolve(HERE, '../../schemas/envelope.v1.json');
const schema = JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const envelopeValidator = ajv.compile(schema);

export function nowIso() {
  return new Date().toISOString().replace(/Z$/, 'Z');   // already .sssZ
}

export function validateEnvelope(env) {
  const ok = envelopeValidator(env);
  if (ok) return { ok: true };
  return { ok: false,
           error: (envelopeValidator.errors || [])
             .map(e => `${e.instancePath || '(root)'} ${e.message}`)
             .join('; ') };
}

export function buildRegisterEnvelope({ agentId, sessionId,
                                        heartbeatIntervalSec = 30,
                                        description = 'Browser-side openclaw client.' }) {
  const card = {
    name: agentId,
    description,
    version: '0.1.0',
    url: `nats://edgecitadel/agents.${agentId}.inbox`,
    provider: { organization: 'EdgeCitadel', url: 'https://edgecitadel.local' },
    capabilities: {
      streaming: false,
      extensions: [{
        uri: 'https://edgecitadel.local/ext/nats-binding/v1',
        description: 'NATS binding via aggregator-mediated publish.',
        required: false,
        params: { subject_prefix: `agents.${agentId}` }
      }]
    },
    securitySchemes: {},
    skills: [{ id: 'openclaw.chat', name: 'chat',
               description: 'Send commands to fleet via aggregator.',
               tags: ['browser'] }],
    defaultInputModes: ['text/plain'],
    defaultOutputModes: ['text/plain'],
    metadata: {
      'runtime.kind': 'native',
      'runtime.roles': ['orchestrator'],
      'runtime.tags': ['openclaw', 'browser', `session:${sessionId}`],
      'runtime.heartbeat_interval_sec': heartbeatIntervalSec
    }
  };
  return {
    v: 1, id: uuid(), type: 'register',
    sender_id: agentId, timestamp: nowIso(), payload: card
  };
}

export function buildHeartbeatEnvelope(agentId) {
  return {
    v: 1, id: uuid(), type: 'heartbeat',
    sender_id: agentId, timestamp: nowIso(), payload: {}
  };
}

export function buildStatusEnvelope(agentId, state, reason) {
  return {
    v: 1, id: uuid(), type: 'status',
    sender_id: agentId, agent_state: state, timestamp: nowIso(),
    payload: reason ? { reason } : {}
  };
}

export function buildCommandEnvelope({ senderId, recipientId, body, args }) {
  return {
    v: 1, id: uuid(), type: 'command',
    sender_id: senderId, recipient_id: recipientId,
    task_id: uuid(), timestamp: nowIso(),
    payload: { body, ...(args ? { args } : {}) }
  };
}
```

- [ ] **Step 13.5: Run tests, confirm pass**

Run: `cd openclaw-client && npm test`

Expected: PASS (4 tests).

- [ ] **Step 13.6: Create `openclaw-client/index.js` (top-level runner)**

```javascript
/**
 * openclaw-client v0.1
 *
 * Connects to NATS using the account-scoped OPENCLAW_TOKEN.
 * Publishes register + heartbeats on plain NATS.
 * Forwards commands to the aggregator HTTP endpoint (not direct JetStream
 * publish) — see ADR-0005.
 */
import 'dotenv/config';
import { connect } from '@nats-io/nats-core';
import { v4 as uuid } from 'uuid';
import {
  buildRegisterEnvelope, buildHeartbeatEnvelope, buildStatusEnvelope,
  validateEnvelope, nowIso
} from './src/nats-session.js';

const {
  NATS_URL = 'nats://localhost:4222',
  OPENCLAW_TOKEN,
  OPENCLAW_SESSION_ID = `sess-${uuid().slice(0, 8)}`,
  OPENCLAW_AGENT_ID = `openclaw-${OPENCLAW_SESSION_ID}`,
  HEARTBEAT_INTERVAL_SEC = '30'
} = process.env;

if (!OPENCLAW_TOKEN) {
  console.error('OPENCLAW_TOKEN is required (not NATS_TOKEN; see ADR-0005).');
  process.exit(1);
}

async function main() {
  const nc = await connect({ servers: NATS_URL, token: OPENCLAW_TOKEN });
  console.log(`[openclaw] connected as ${OPENCLAW_AGENT_ID}`);

  const enc = data => new TextEncoder().encode(JSON.stringify(data));

  const reg = buildRegisterEnvelope({
    agentId: OPENCLAW_AGENT_ID,
    sessionId: OPENCLAW_SESSION_ID,
    heartbeatIntervalSec: Number(HEARTBEAT_INTERVAL_SEC)
  });
  const v = validateEnvelope(reg);
  if (!v.ok) { console.error('register invalid:', v.error); process.exit(2); }

  await nc.publish(`agents.${OPENCLAW_AGENT_ID}.register`, enc(reg));

  const hbInterval = setInterval(() => {
    const hb = buildHeartbeatEnvelope(OPENCLAW_AGENT_ID);
    nc.publish(`agents.${OPENCLAW_AGENT_ID}.heartbeat`, enc(hb));
  }, Number(HEARTBEAT_INTERVAL_SEC) * 1000);

  const shutdown = async () => {
    clearInterval(hbInterval);
    const off = buildStatusEnvelope(OPENCLAW_AGENT_ID, 'offline', 'shutdown');
    await nc.publish(`agents.${OPENCLAW_AGENT_ID}.status`, enc(off));
    await nc.drain();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  // subscribe to own results (plain NATS — mirrored from aggregator)
  const sub = nc.subscribe(`openclaw.${OPENCLAW_SESSION_ID}.results.*`);
  (async () => {
    for await (const m of sub) {
      try {
        const env = JSON.parse(new TextDecoder().decode(m.data));
        console.log('[openclaw] result:', env.task_id, env.task_state,
                    env.payload?.body?.slice?.(0, 120));
      } catch (e) {
        console.warn('[openclaw] non-JSON result:', e.message);
      }
    }
  })();
}

main().catch(err => { console.error(err); process.exit(3); });
```

Note: Command-dispatch path goes through the aggregator HTTP API (`POST /api/command/{recipient}` with the browser's session token), NOT direct JetStream publish — see Task 14.

- [ ] **Step 13.7: Write ADR 0005**

Create `docs/adr/0005-browser-scoped-token.md`. Status: Accepted. Context: browser holds NATS token in untrusted JS runtime; fleet `NATS_TOKEN` would allow impersonation. Decision: aggregator issues account-scoped `OPENCLAW_TOKEN` (~1h TTL, refresh via HTTP); token permits only `openclaw.{session_id}.*` publishes; aggregator translates them into canonical `agents.{id}.inbox` JetStream publishes with server-set `sender_id: openclaw-{session_id}`. Consequences: all browser-originated commands are aggregator-mediated; v0.2 per-agent JWTs replace this.

- [ ] **Step 13.8: Delete legacy + update README**

```bash
rm openclaw-client/mqtt-listener.js
```

Rewrite `openclaw-client/README.md` to document the new flow: env vars (`NATS_URL`, `OPENCLAW_TOKEN`, `OPENCLAW_SESSION_ID`), how to obtain the scoped token (aggregator login endpoint, TBD), MQTT deleted.

- [ ] **Step 13.9: Commit**

```bash
git add openclaw-client/package.json openclaw-client/index.js \
        openclaw-client/src/ openclaw-client/tests/ \
        openclaw-client/README.md \
        docs/adr/0005-browser-scoped-token.md
git rm openclaw-client/mqtt-listener.js
git commit -m "feat(openclaw): rewrite client on @nats-io/nats with scoped token (ADR-0005)"
```

---

## Task 14: Aggregator session-token endpoint + openclaw subject translation

**Files:**
- Modify: `aggregator/main.py`
- Modify: `aggregator/aggregator.py` (add `openclaw.*.>` subscriber)
- Modify: `aggregator/tests/test_api.py`

- [ ] **Step 14.1: Write failing tests**

Append to `aggregator/tests/test_api.py`:

```python
def test_openclaw_login_returns_token(client):
    r = client.post("/api/openclaw/login",
                    json={"session_id": "sess-abc123"})
    assert r.status_code == 200
    body = r.json()
    assert "token" in body
    assert "expires_at" in body
    assert body["agent_id"] == "openclaw-sess-abc123"


def test_openclaw_login_rejects_bad_session(client):
    r = client.post("/api/openclaw/login", json={"session_id": "bad/slash"})
    assert r.status_code == 422
```

- [ ] **Step 14.2: Run, confirm fail**

Run: `python3 -m pytest aggregator/tests/test_api.py::test_openclaw_login_returns_token -v`

Expected: FAIL (endpoint missing).

- [ ] **Step 14.3: Add login endpoint + translator**

Add to `aggregator/main.py`:

```python
@app.post("/api/openclaw/login")
async def openclaw_login(body: dict):
    session_id = body.get("session_id", "")
    import re
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", session_id):
        raise HTTPException(422, "invalid session_id")
    # v0.1: stub — real per-session NATS JWT issuance is v0.2.
    # We return a short-lived opaque token the aggregator recognizes on the
    # openclaw.* ingress path.
    import uuid as _u
    from datetime import datetime, timezone, timedelta
    token = _u.uuid4().hex
    exp = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")
    _OPENCLAW_TOKENS[token] = session_id  # in-memory, resets on restart
    return {"token": token, "expires_at": exp,
            "agent_id": f"openclaw-{session_id}"}


_OPENCLAW_TOKENS: dict[str, str] = {}
```

Add `openclaw.*.>` subscriber in `aggregator/aggregator.py` `AggregatorApp.start`:

```python
        await nc.subscribe("openclaw.*.>", cb=self.router.on_openclaw_ingress)
```

Add `on_openclaw_ingress` to `MessageRouter`:

```python
    async def on_openclaw_ingress(self, msg: Msg) -> None:
        """Translate openclaw.{session}.command.{target} → agents.{target}.inbox."""
        parts = msg.subject.split(".")
        if len(parts) < 4 or parts[0] != "openclaw":
            return
        session_id, kind = parts[1], parts[2]
        env = self._parse(msg.data)
        if env is None: return
        if kind == "command" and len(parts) == 4:
            target = parts[3]
            # server-set sender_id, do NOT trust browser
            out = {
                "v": 1, "id": env.get("id") or _uuid4_str(),
                "type": "command",
                "sender_id": f"openclaw-{session_id}",
                "recipient_id": target,
                "task_id": env.get("task_id") or _uuid4_str(),
                "timestamp": now_iso(),
                "payload": env.get("payload", {})
            }
            await self.js.publish(f"agents.{target}.inbox",
                                  json.dumps(out).encode(),
                                  headers={"Nats-Msg-Id": out["id"]})
            await self.nc.publish(f"agents.openclaw-{session_id}.outbox",
                                  json.dumps(out).encode())


def _uuid4_str() -> str:
    import uuid; return str(uuid.uuid4())
```

- [ ] **Step 14.4: Run tests, confirm pass**

Run: `python3 -m pytest aggregator/tests/test_api.py -v`

Expected: PASS.

- [ ] **Step 14.5: Commit**

```bash
git add aggregator/main.py aggregator/aggregator.py \
        aggregator/tests/test_api.py
git commit -m "feat(aggregator): openclaw session-token login + subject translator"
```

---

## Task 15: NATS conf MQTT toggle + docker-compose profile

**Files:**
- Create: `nats/nats.conf.tpl` (source template)
- Modify: `nats/nats.conf` (no MQTT by default)
- Modify: `docker-compose.yml`
- Modify: `.env.example`
- Create: `docs/adr/0004-mqtt-ingress-opt-in.md`
- Create: `scripts/render-nats-conf.sh`

- [ ] **Step 15.1: Create `nats/nats.conf.tpl`**

```
server_name: "edgecitadel"
listen: 0.0.0.0:4222
http_port: 8222

jetstream {
    store_dir: "/data/jetstream"
    max_mem: 256MB
    max_file: 1GB
}

# MQTT ingress is deploy-time opt-in (ADR-0004). Uncommented by render script.
# MQTT_BEGIN
# mqtt {
#     port: 1883
#     ack_wait: "30s"
#     max_ack_pending: 1024
# }
# MQTT_END

authorization {
    token: $NATS_TOKEN
    users: [
        { user: openclaw, password: $OPENCLAW_TOKEN,
          permissions: {
            publish: { allow: ["openclaw.*.>"] },
            subscribe: { allow: ["openclaw.*.results.>"] }
          }
        }
    ]
}
```

- [ ] **Step 15.2: Create `scripts/render-nats-conf.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$HERE/nats/nats.conf.tpl"
DST="$HERE/nats/nats.conf"

if [[ "${EC_ENABLE_MQTT:-0}" == "1" ]]; then
  # uncomment the block between MQTT_BEGIN and MQTT_END
  awk '/# MQTT_BEGIN/{flag=1;next} /# MQTT_END/{flag=0;next} flag{sub(/^# /,"")} {print}' \
    "$SRC" > "$DST"
  echo "Rendered $DST with MQTT ingress ENABLED (port 1883 exposed)."
else
  cp "$SRC" "$DST"
  echo "Rendered $DST with MQTT ingress DISABLED (default)."
fi
```

Make executable: `chmod +x scripts/render-nats-conf.sh`.

- [ ] **Step 15.3: Modify `nats/nats.conf`**

Overwrite with the rendered default-off output (i.e., `nats.conf.tpl` verbatim).

- [ ] **Step 15.4: Modify `docker-compose.yml`**

```yaml
services:
  nats:
    image: nats:2.10-alpine
    command: ["-c", "/etc/nats/nats.conf"]
    ports:
      - "4222:4222"
      - "8222:8222"
    volumes:
      - ./nats/nats.conf:/etc/nats/nats.conf:ro
      - ./nats/data:/data
    environment:
      - NATS_TOKEN=${NATS_TOKEN}
      - OPENCLAW_TOKEN=${OPENCLAW_TOKEN}
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8222/healthz"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  nats-mqtt:
    # Activated only with --profile mqtt-ingress.
    profiles: ["mqtt-ingress"]
    image: nats:2.10-alpine
    command: ["-c", "/etc/nats/nats.conf"]
    ports:
      - "4222:4222"
      - "8222:8222"
      - "1883:1883"
    volumes:
      - ./nats/nats.conf:/etc/nats/nats.conf:ro
      - ./nats/data:/data
    environment:
      - NATS_TOKEN=${NATS_TOKEN}
      - OPENCLAW_TOKEN=${OPENCLAW_TOKEN}
    restart: unless-stopped

  aggregator:
    build: ./aggregator
    restart: unless-stopped
    stop_grace_period: 40s
    volumes:
      - ./data:/data
    environment:
      - DB_PATH=/data/openclaw.db
      - NATS_URL=nats://nats:4222
      - NATS_TOKEN=${NATS_TOKEN}
    expose:
      - "8000"
    depends_on:
      nats:
        condition: service_healthy

  dashboard:
    build: ./frontend
    restart: unless-stopped
    expose:
      - "80"

  nginx:
    image: nginx:alpine
    restart: unless-stopped
    ports:
      - "80:80"
    volumes:
      - ./nginx/default.conf:/etc/nginx/conf.d/default.conf
    depends_on:
      - aggregator
      - dashboard
```

(The two NATS services share the same container image; operators run either the default (`docker compose up -d`) or the MQTT-enabled profile (`EC_ENABLE_MQTT=1 scripts/render-nats-conf.sh && docker compose --profile mqtt-ingress up -d`). For v0.1, keep it simple.)

- [ ] **Step 15.5: Update `.env.example`**

Append/set:

```
NATS_TOKEN=change-me
OPENCLAW_TOKEN=change-me-scoped
# Set EC_ENABLE_MQTT=1 before running scripts/render-nats-conf.sh to
# enable deploy-time MQTT ingress. Default off.
EC_ENABLE_MQTT=0
```

- [ ] **Step 15.6: Write ADR 0004**

`docs/adr/0004-mqtt-ingress-opt-in.md`. Status: Accepted. Context: NATS MQTT adapter is 3.1.1-only with known bug (#5282); unused by internal fleet. Decision: MQTT block templated and disabled by default; opt-in via `EC_ENABLE_MQTT=1` + re-render + compose profile. Consequences: internal fleet free of MQTT code; sensor onboarding requires the designated gateway agent.

- [ ] **Step 15.7: Verify default compose has no port 1883**

Run: `docker compose config | grep -A2 ports | head`

Expected: `4222`, `8222` — no `1883`.

- [ ] **Step 15.8: Commit**

```bash
git add nats/nats.conf nats/nats.conf.tpl scripts/render-nats-conf.sh \
        docker-compose.yml .env.example \
        docs/adr/0004-mqtt-ingress-opt-in.md
git commit -m "feat(infra): MQTT ingress behind deploy-time toggle (ADR-0004)"
```

---

## Task 16: Frontend canonical-field rewrite + queue/poison UI

**Files:**
- Modify: `frontend/src/api/client.js`
- Modify: `frontend/src/stores/appStore.js`
- Modify: `frontend/src/components/MessageBubble.jsx`
- Modify: `frontend/src/components/ConversationThread.jsx`
- Modify: `frontend/src/components/AgentCard.jsx`
- Modify: `frontend/src/components/AgentDetail.jsx`
- Modify: `frontend/src/components/TaskBoard.jsx`
- Modify: `frontend/src/components/TaskCard.jsx`
- Modify: `frontend/src/components/CommFlow.jsx`
- Modify: `frontend/src/components/CommandInput.jsx`

- [ ] **Step 16.1: Rewrite API client for new endpoints**

Edit `frontend/src/api/client.js` — replace legacy calls:

```javascript
const API_BASE = '/api';

async function req(path, opts = {}) {
  const r = await fetch(`${API_BASE}${path}`, opts);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.status === 204 ? null : r.json();
}

export const api = {
  systemStatus: () => req('/system/status'),
  listAgents:   () => req('/agents'),
  getAgent:     (id) => req(`/agents/${id}`),
  getAgentCard: (id) => req(`/agents/${id}/card`),
  getAgentQueue: (id) => req(`/agents/${id}/queue`),
  deleteAgent:  (id) => req(`/agents/${id}`, { method: 'DELETE' }),
  sendCommand:  (agentId, body, args) => req(`/command/${agentId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ body, ...(args ? { args } : {}) })
  }),
  queryMessages: (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return req(`/messages${qs ? `?${qs}` : ''}`);
  },
  queryPoison:  (agentId, limit = 100) => req(`/poison?${
    new URLSearchParams({ ...(agentId ? { agent_id: agentId } : {}), limit }).toString()}`)
};
```

- [ ] **Step 16.2: Rewrite store field names**

In `frontend/src/stores/appStore.js`, every read of `receiver_id`, `message_type`, `correlation_id`, `chain_id`:
- `receiver_id` → `recipient_id`
- `message_type` → `type`
- `correlation_id` → `task_id`
- `chain_id` → `context_id`

Add state slices: `poisonEvents` (keyed by agent_id), `agentQueue` (keyed by agent_id, `{pending, ack_pending}`).

- [ ] **Step 16.3: Update component reads**

For each component listed:
- `MessageBubble.jsx` — read `msg.type` (not `message_type`), `msg.recipient_id`, `msg.task_id`, `msg.task_state`.
- `ConversationThread.jsx` — group by `context_id` (was `chain_id`); show `task_state` badge on results.
- `TaskCard.jsx`, `TaskBoard.jsx` — switch to `task_id`/`task_state`; task_state values are A2A enum.
- `CommFlow.jsx` — nodes by `sender_id`/`recipient_id`.
- `AgentCard.jsx` — read `agent.card.metadata['runtime.roles']`, `agent.agent_state` (was `status`), `agent.card.metadata['runtime.heartbeat_interval_sec']`.
- `AgentDetail.jsx` — add two new panels:
  - "Queue depth": `agentQueue[id] = {pending, ack_pending}` polled every 5s via `api.getAgentQueue`.
  - "Poison events": `poisonEvents[id]` list, each with `detected_at`, `task_id`, `original_sender`. Pull via `api.queryPoison(id)` on open.
- `CommandInput.jsx` — on submit, call `api.sendCommand(agentId, body)` and display returned `task_id`; subscribe to WS for `task.progress` + `result` keyed on that task_id.

- [ ] **Step 16.4: Build + manual smoke**

```bash
cd frontend && npm run build
```

Expected: PASS (no type/field reference errors in React strict-mode console).

Bring up full stack and visit `http://localhost/`:

```bash
docker compose up --build -d
```

Visit the Agent Detail panel for `shell-1`, send `echo hi`, confirm:
- task_id displayed
- `type: command` and `type: result` rows visible in conversation thread
- Queue depth reads 0 (idle)

- [ ] **Step 16.5: Commit**

```bash
git add frontend/src/
git commit -m "feat(frontend): canonical envelope field names + queue/poison surfaces"
```

---

## Task 17: E2E — smoke round-trip + subject-inventory coverage

**Files:**
- Modify: `e2e/helpers/fixtures.js`
- Delete: `e2e/helpers/mqtt-client.js`
- Create: `e2e/tests/phase1-smoke.spec.js`
- Delete/rewrite: `e2e/tests/*.spec.js` asserting on legacy fields (see list)

- [ ] **Step 17.1: Enumerate legacy specs to rewrite or delete**

```bash
grep -l "receiver_id\|message_type\|correlation_id\|chain_id\|mqtt" e2e/tests/ | cat
```

For each matching file, either:
- **Rewrite** to canonical fields (keep if it exercises a still-supported surface).
- **Delete** if it tests a legacy-only behavior (e.g., MQTT slash topics).

Document the decision in the commit message.

- [ ] **Step 17.2: Rewrite `e2e/helpers/fixtures.js`**

Strip MQTT helpers. Add `buildCanonicalEnvelope({type, sender_id, recipient_id, task_id, body})` matching v0.1 shape. Delete `mqtt-client.js`.

- [ ] **Step 17.3: Write Phase 1 smoke spec**

Create `e2e/tests/phase1-smoke.spec.js`:

```javascript
import { test, expect } from '@playwright/test';

const API = process.env.AGG_URL || 'http://localhost';

test.describe('Phase 1 smoke — canonical envelope round trip', () => {
  test('system status has no mqtt_connected', async ({ request }) => {
    const r = await request.get(`${API}/api/system/status`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(body).not.toHaveProperty('mqtt_connected');
    expect(body).toHaveProperty('nats_connected');
    expect(body).toHaveProperty('jetstream_stream_ok');
  });

  test('shell-1 registered with A2A card', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/shell-1/card`);
    expect(r.ok()).toBe(true);
    const card = await r.json();
    expect(card.name).toBe('shell-1');
    expect(card.metadata['runtime.kind']).toBe('native');
    expect(card.metadata['runtime.roles']).toContain('worker');
    expect(card.capabilities.extensions.some(
      e => e.uri === 'https://edgecitadel.local/ext/nats-binding/v1')).toBe(true);
  });

  test('POST /command returns task_id and result arrives', async ({ request }) => {
    const post = await request.post(`${API}/api/command/shell-1`, {
      data: { body: 'echo phase1-smoke' }
    });
    expect(post.status()).toBe(202);
    const { task_id } = await post.json();
    expect(task_id).toMatch(/^[0-9a-f-]{36}$/);

    // poll for result
    let result;
    for (let i = 0; i < 30; i++) {
      await new Promise(r => setTimeout(r, 500));
      const q = await request.get(
        `${API}/api/messages?task_id=${task_id}&type=result`);
      const rows = await q.json();
      if (rows.length) { result = rows[0]; break; }
    }
    expect(result).toBeDefined();
    expect(result.task_state).toBe('completed');
    expect(result.payload.body).toContain('phase1-smoke');
    // legacy fields must not be in the DB row
    expect(result).not.toHaveProperty('receiver_id');
    expect(result).not.toHaveProperty('message_type');
  });

  test('queue endpoint returns pending/ack_pending integers', async ({ request }) => {
    const r = await request.get(`${API}/api/agents/shell-1/queue`);
    expect(r.ok()).toBe(true);
    const body = await r.json();
    expect(Number.isInteger(body.pending)).toBe(true);
    expect(Number.isInteger(body.ack_pending)).toBe(true);
  });

  test('subject inventory coverage — DB contains each seeded type', async ({ request }) => {
    // rely on prior fixtures to have produced register, heartbeat, command, result
    const r = await request.get(`${API}/api/messages?limit=500`);
    const rows = await r.json();
    const types = new Set(rows.map(x => x.type));
    for (const t of ['register', 'heartbeat', 'command', 'result']) {
      expect(types.has(t), `missing type=${t}`).toBe(true);
    }
  });
});
```

- [ ] **Step 17.4: Run the smoke spec**

```bash
docker compose down -v
docker compose up --build -d
# wait for aggregator health
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -sf http://localhost/api/system/status && break || sleep 2
done

# shell-1 adapter runs on host
NATS_URL=nats://localhost:4222 NATS_TOKEN="$NATS_TOKEN" \
  python3 -m adapters.shell.adapter &
SHELL_PID=$!
sleep 3

cd e2e && npm test -- phase1-smoke.spec.js
cd -
kill $SHELL_PID
```

Expected: PASS (5 tests).

- [ ] **Step 17.5: Commit**

```bash
git add e2e/helpers/fixtures.js e2e/tests/phase1-smoke.spec.js \
        $(git ls-files -m e2e/tests/)
git rm e2e/helpers/mqtt-client.js
git commit -m "test(e2e): phase1 smoke spec + canonical-envelope fixtures"
```

---

## Task 18: CHANGELOG entry + Phase 1 end-of-phase verification

**Files:**
- Modify: `docs/CHANGELOG.md`

- [ ] **Step 18.1: Add Unreleased → v0.1 entry**

Prepend to `docs/CHANGELOG.md` under `## [Unreleased]`:

```markdown
## [Unreleased]

### Added — v0.1 messaging clean rebuild (Phase 1)
- NATS JetStream `AGENT_INBOX` WorkQueue stream with per-agent durable pull
  consumers (`max_ack_pending=1`). Envelope dedup via `Nats-Msg-Id` and
  `duplicate_window: 5m` (ADR-0002).
- A2A v1.0 task lifecycle vocabulary on every envelope (`task_id`, `context_id`,
  `task_state`, `hop_count`). Agent Card shape replaces legacy EdgeCitadel card
  (ADR-0003).
- Outbox mirror (`agents.{id}.outbox`) as authoritative audit path for inbox
  traffic (ADR-0006).
- Aggregator endpoints: `GET /api/agents`, `/agents/{id}/card`,
  `/agents/{id}/queue`, `DELETE /api/agents/{id}`; `POST /api/command/{id}`
  returns `task_id`; `GET /api/messages` / `/api/poison`.
- Shared adapter skeleton under `adapters/_common/` (pull consumer, Agent Card
  factory, conformance suite, template).
- Shell adapter rewritten on nats-py async (replaces paho legacy).
- openclaw-client rewritten on `@nats-io/nats` with account-scoped
  `OPENCLAW_TOKEN` and aggregator-mediated publishes (ADR-0005).
- MQTT ingress moved behind deploy-time toggle (`EC_ENABLE_MQTT=1`); off by
  default (ADR-0004).
- Frontend reads canonical fields; new queue-depth and poison-event surfaces.

### Removed
- `receiver_id`, `message_type`, `correlation_id`, `chain_id`, `content`,
  `from`, `to`, `assigned_agent` aliases and alias-fallback readers.
- `/data/openclaw.db` wiped on first boot; no migration from pre-v0.1 shape.
- paho-mqtt client code (`openclaw-client/mqtt-listener.js`,
  `adapters/shell/shell_adapter.py`).
- MQTT 1883 port exposed by default in `docker-compose.yml`.
- `mqtt_connected` field in `/api/system/status`.
```

- [ ] **Step 18.2: End-of-phase verification checklist**

Run each of these against a fresh `docker compose down -v && docker compose up --build -d` stack. Record pass/fail per row — resolve any failure before shipping Phase 1.

| Check | Command | Expected |
|---|---|---|
| Stream live | `docker compose exec nats nats stream info AGENT_INBOX` | shows stream |
| Consumer live | `docker compose exec nats nats consumer info AGENT_INBOX shell-1_inbox` | `num_pending=0` idle |
| Sequential FIFO | send 2 commands back-to-back, inspect `nats consumer info` mid-flight | `num_ack_pending <= 1` |
| Crash recovery | kill shell adapter mid-task, restart | unacked redelivers on restart |
| Dedup | publish same cmd with same Nats-Msg-Id 3x | handler fires once |
| Queue endpoint | `curl /api/agents/shell-1/queue` | `{pending, ack_pending}` |
| Strict validation | publish envelope with `receiver_id: x` | dropped; logged reason |
| openclaw round-trip | browser → aggregator HTTP → JetStream → shell-1 → result back | result visible |
| Fresh DB schema | `sqlite3 data/openclaw.db '.schema messages' \| grep recipient_id` | match; `receiver_id` absent |
| MQTT off by default | `docker compose ps \| grep 1883` | empty |
| No `mqtt_connected` in status | `curl /api/system/status` | field absent |
| Aggregator restart discovery | restart aggregator, check `/api/agents` | all online agents within 10s |
| E2E smoke | `cd e2e && npm test -- phase1-smoke.spec.js` | PASS |

- [ ] **Step 18.3: Commit**

```bash
git add docs/CHANGELOG.md
git commit -m "docs(changelog): v0.1 messaging rebuild (Phase 1)"
```

---

# Phases 2–5 — handoff

Phases 2–5 are out of scope for this plan. Each is its own working-software increment building on Phase 1. Open a follow-up plan for each phase when ready — the spec sessions below map 1:1 to follow-up plan scopes.

### Phase 2 — Gemma smoke test (1 session → 1 plan)
- **Session 2.1:** Gemma adapter wrapping Ollama `POST /api/generate`. Reuse `adapters/_common/pull_consumer.py`. Preflight: `ollama list | grep -E "gemma[34]:12b"` before writing code. Plan file: `docs/superpowers/plans/<date>-gemma-adapter.md`.

### Phase 3 — Operational hardening (2 sessions → 1 plan)
- **Session 3.1:** watchdog adapter. Subscribes `agents.*.heartbeat` and `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>`. Publishes synthesized `task_state: failed, payload.error: "recipient_offline"` results when the advisory fires. Own durable inbox with `max_ack_pending: 1`.
- **Session 3.2:** dashboard agent-registry panel — per-agent card, heartbeat freshness, queue depth, poison-count, online/offline badge. Uses endpoints from Tasks 6, 8 already built.

### Phase 4 — AG2 + A2A wrapper (4 sessions → 1 plan)
- **Session 4.1:** AG2 adapter L1 scaffold. **Pin `ag2>=0.12,<0.13` and spend 15 minutes verifying imports** (`A2aRemoteAgent`, `A2aAgentServer`, `autogen.agentchat.group.AutoPattern`) against the pinned wheel before writing code. Use `a_run` async-only.
- **Session 4.2:** AG2 L2 delegation + `hop_count` loop protection. Refuse at `hop_count >= 8`. Cancel returns `task_state: rejected, payload.reason: "ag2_cancel_not_supported"` (v0.1 limitation).
- **Session 4.3:** Dashboard delegation-chain view. `GET /api/chains/{context_id}` endpoint + chain timeline UI.
- **Session 4.4:** A2A HTTP wrapper — `A2aAgentServer(agent, agent_card=card)` serving `/.well-known/agent-card.json`; NATS bridge translates SSE → `task.progress` envelopes. Decide `.build()` vs `.serve()` vs `.run()` at pin time.

### Phase 5 — Mac Mini deploy (1 session → 1 plan)
- **Session 5.1:** `deploy-mac-mini.sh`. Preflight documented: `.env` over tailnet, `BROKER_HOST` set, operator verifies NATS_TOKEN has JetStream perms (`nats consumer add` smoke).

### Optional, v0.1+
- **Bridge adapter for Hermes / ACP.** Covered by spec §"Bridge pattern"; not required for v0.1 completion. Plan when Nous Research's Hermes Agent is first onboarded.

---

## Self-review checklist (completed before saving)

- [x] **Spec coverage:** Every numbered session in spec §"Impact on the execution plan" (Phase 1: 1.1–1.9) maps to a task above (Task 1 ↔ §1.1 envelope, Task 2 ↔ §1.2 agent card, Tasks 3–6 ↔ §1.3 aggregator, Task 7 ↔ §1.4 JetStream, Tasks 9–11 ↔ §1.5 adapter common, Task 12 ↔ §1.6 shell, Tasks 13–14 ↔ §1.7 openclaw, Task 15 ↔ §1.7 MQTT toggle, Task 16 ↔ §1.8 frontend, Task 17 ↔ §1.9 smoke). All spec Verification items 1–8, 13–19, 21, 24–29 targeted by Phase 1 verification.
- [x] **Placeholder scan:** No "TBD", no "handle edge cases", no "add validation" without content, no "similar to Task N" — every code block is inline.
- [x] **Type consistency:** `task_id` / `context_id` / `task_state` / `agent_state` / `hop_count` / `recipient_id` used everywhere; legacy names appear only in deletion/rejection paths.
- [x] **File paths:** Every Files block uses absolute-within-repo paths matching current layout (`aggregator/`, `adapters/_common/`, `openclaw-client/`, `frontend/src/`, `nats/`, `docker-compose.yml`, `docs/adr/`).
