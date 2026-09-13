# Standalone tree topology validation

This directory is a copyable validation unit for EdgeCitadel's **single-level,
destination-owned JetStream broker topology**. It reproduces the successful
2026-09-13 three-process audit and splits its contracts into nine tests. It does
not import EdgeCitadel code or depend on the repository's Python/test setup.

```text
                 Core
                /    \
          Leaf A      Leaf B
          a1, a2         b1
```

These are three **brokers**, not three agents or three physical sites. The inbox
names are synthetic subjects; no agents, harnesses, LLMs, cameras, or production
services are started. The existing generic names are retained.

## Run from a clean machine

Requirements: Python 3.11+, macOS or Linux on amd64/arm64, permission to bind local
TCP ports, and internet access for initial dependency downloads. No Docker,
EdgeCitadel installation, API key, or administrator installation is required.

Copy this entire directory anywhere, then run from the copied directory:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python bootstrap.py
.venv/bin/python run.py
```

`bootstrap.py` downloads NATS Server **2.14.6** from its official GitHub release,
checks a pinned SHA256, and extracts only the expected binary into `.cache/`.
It does not install a system service. `requirements.txt` pins the sole external
Python dependency, `nats-py==2.15.0`; tests use the standard-library `unittest`.

Once dependencies are present, the suite runs offline. You can supply an already
installed binary of exactly that version:

```sh
.venv/bin/python run.py --nats-server /absolute/path/to/nats-server
```

Run from another working directory using an absolute path to `run.py`; no
`PYTHONPATH`, root pytest config, or Git checkout is needed. Each invocation
creates a fresh `artifacts/<timestamp>/` directory. To choose a location:

```sh
.venv/bin/python run.py --output-dir /tmp/tree-validation-new-run
```

The output directory must not already exist. Missing/wrong dependencies,
startup failures, assertion failures, skips, or unexpected test counts return
nonzero. Initial downloads can use a normal package mirror/proxy; runtime
connections are always loopback only.

## Contracts and independent oracles

| Test | Operation | Required evidence |
| --- | --- | --- |
| Local destination | A publishes 10 messages to a2 | Exactly those records in A, none in Core/B |
| Core to Leaf | Core publishes 10 messages to a1 | Exactly those records in A only |
| Leaf to Core | A publishes 10 messages to Core | Exactly those records in Core only |
| Leaf to Leaf | A publishes 10 messages to b1 | Exactly those records in B only |
| Local partition progress | Stop Core, publish locally on both Leaves | Both local writes succeed with exact payloads |
| Unreachable destination | Stop Core, A publishes to B | Specific no-stream-response/timeout exception; neither Leaf stores it |
| Recovery and deduplication | Restore Core after failed publication, then retry twice | Empty destinations during a bounded 0.5s pre-retry observation; one stored record and duplicate ACK with identical stream sequence |
| Leaf persistence | Restart A with the same store, then write again | Exact old records survive and a new write succeeds |
| Original audit sequence | Four directions, partition, recovery/retry, restart | Counts `(Core,A,B)=(10,20,10)`, then `(A,B)=(21,10)`, finally `(10,21,11)`, plus exact payload/subject/message-ID checks |

The expected subjects and records are test inputs, not derived from NATS config
rendering. Every route checks **all three streams**, not just the destination
count. Readiness uses live Leaf connection counts and request/reply barriers;
fixed sleeps are used only for the explicitly bounded negative observation.
Unexpected exceptions are failures, not accepted as evidence of a partition.

## Evidence and process ownership

- `report.json`: versions, binary/source hashes, per-test results, durations,
  observed records, ACKs, and expected failure types.
- `junit.xml`: machine-readable test results, suitable for CI upload.
- Per-test broker logs: startup/connection/termination diagnostics.

Each test creates its own three processes, random client credentials, dynamically
selected loopback ports, and temporary file stores. Cleanup closes clients,
terminates/reaps owned processes, closes logs, and removes temporary configs and
stores. A port collision fails the test rather than reusing an unrelated service.
The first Ctrl-C requests a graceful stop after the current bounded test; a forced
second interruption or SIGKILL may prevent normal cleanup.

Logs and runtime reports are ignored by Git. Logs can contain local filesystem
paths and temporary server identities; review them before sharing. Committed
acceptance summaries contain portable results and hashes, not production data.

## Relationship to the application

This validates the **broker architecture contract**, not the current production
configuration generator. Its standalone configuration mirrors these audited
assumptions from EdgeCitadel main `0586019`:

- one Core and one upstream per Leaf;
- a separate JetStream domain on each Leaf;
- exact destination inbox subjects with one canonical stream owner;
- file-backed streams and message-ID deduplication within a 120-second window.

When `scripts/nats_leaf.py`, `edgecitadel_plugin_runtime/jetstream.py`, or the
pinned NATS version changes, review these assumptions and rerun application
integration tests as well. Decoupling intentionally means application drift will
not automatically make this reference topology fail. It is **not** a replacement
for `scripts/tests/test_nats_leaf.py` or the opt-in Docker topology tests.

It does not establish agentd outbox replay, same-agentd SQLite behavior, task
execution exactly once, workflow recovery, DAG tracing, multi-host site autonomy,
arbitrary-depth trees, WAN behavior, TLS, per-agent authorization, or credential
revocation. The earlier Docker authorization/revocation tests were skipped in the
audit; this unit does not relabel them as passed. Retry here is explicit at the
test client: the broker has no application spool to replay a failed publication.

## Maintenance and CI

All executable code, pins, and run instructions are contained here. Updating the
contract should update the test oracle and this README together. The filename
`check_topology.py` intentionally keeps these owned-process tests out of default
root pytest discovery. Invoke `run.py` explicitly and require its exit status;
archive both JSON/JUnit artifacts. No production-stack restart or UI workflow is
part of this test unit.
