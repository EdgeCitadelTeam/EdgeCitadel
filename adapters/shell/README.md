# Shell Adapter

L1 reference implementation of the [Agent Contract](../../docs/agent-contract.md), rebuilt for v0.1 on the shared `nats-py` async / JetStream pull-consumer skeleton in [`adapters/_common`](../_common).

The legacy paho-mqtt `shell_adapter.py` has been **deleted**. This is the clean rebuild.

## What it does

- Subscribes to `agents.shell-1.inbox` via a JetStream **pull** consumer (durable name `shell-1_inbox`, `max_ack_pending: 1` for FIFO ordering).
- Publishes its Agent Card on `agents.shell-1.register` at startup and a heartbeat envelope on `agents.shell-1.heartbeat` every 30s.
- For each `command` envelope on the inbox, runs `payload.body` via `asyncio.create_subprocess_shell` with a default 30s timeout (override via `payload.args.timeout_sec`).
- Returns a `result` envelope to the sender's inbox with:
  - `payload.body`: combined stdout + stderr, capped at 64 KB.
  - `payload.returncode`: process exit code.
  - `payload.error`: `nonzero_exit` on non-zero rc, `timeout` if the timeout fired, `empty_command` on a blank body, `unsupported_type` for non-`command` envelopes.
  - `task_state`: `completed` (rc == 0), `failed` (timeout or non-zero rc), or `rejected` (non-command type / empty body).
- `delegation`, `cancel`, and other inbox types are rejected with `task_state: rejected`. (The shell adapter does not delegate further.)

## Subjects

| Subject | Purpose |
|---|---|
| `agents.shell-1.register` | Agent Card published on startup |
| `agents.shell-1.heartbeat` | Periodic liveness, every 30s |
| `agents.shell-1.status` | `online`/`offline` lifecycle events |
| `agents.shell-1.inbox` | JetStream-durable inbox (consumed by this adapter) |
| `agents.shell-1.outbox` | Best-effort mirror of every `result` published |
| `agents.shell-1.log` | Adapter log stream (reserved) |

## Run it

From the repository root:

```bash
pip install -r adapters/shell/requirements.txt

NATS_URL=nats://127.0.0.1:4222 \
NATS_TOKEN=edgecitadel-nats-secret-2026 \
PYTHONPATH=. \
python3 -m adapters.shell.adapter
```

`PYTHONPATH=.` is required so the adapter can import `adapters._common` and `aggregator.jetstream_bootstrap` as packages.

### Environment variables

| Var | Default | Purpose |
|---|---|---|
| `NATS_URL` | *(required)* | NATS server URL (e.g. `nats://127.0.0.1:4222`). |
| `NATS_TOKEN` | *(optional)* | Shared token used for NATS auth. |
| `ACK_WAIT_SEC` | `300` | JetStream ack-wait window. The pull consumer extends this with periodic `in_progress()` keepalives while the subprocess runs. |

The agent identity, heartbeat cadence, skills, and tags are declared in `config.yaml`, not env vars. Edit that file to clone the adapter to a new id.

## Cancel / shutdown

`SIGTERM` and `SIGINT` cause the adapter to publish a `status: offline` envelope, stop the pull consumer, drain the NATS connection, and exit cleanly.

## Tests

```bash
pytest adapters/shell/tests/test_shell.py -v
```

The unit tests cover three cases: a successful `echo hello`, a `sleep 99` with `timeout_sec: 1` that must return `task_state: failed` with `error: timeout`, and a `delegation` envelope rejected with `task_state: rejected`.

The timeout test starts a real subprocess that gets killed after 1s; any POSIX shell on `PATH` is sufficient.

## What it does NOT do (by design)

- **P2P delegation.** Out of scope for L1; planners use specialized adapters.
- **MCP tool serving.** That's L3.
- **Per-type payload schema validation.** Only the envelope and Card are schema-validated in v0.1. Per-payload schemas come in v0.2.
