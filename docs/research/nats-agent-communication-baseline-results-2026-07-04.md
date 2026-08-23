# NATS Agent Communication Baseline Results

Date: 2026-07-04

This note records the first native-NATS baseline slice from
`nats-agent-communication-experiment-matrix.md`.

## Environment

- Branch: `docs/nats-agent-research-plan`
- Stack: local `docker compose up --build -d`
- API health: `{"nats_connected": true, "jetstream_stream_ok": true, "version": "0.1.0"}`
- Host adapters started:
  - `shell-1` with `NATS_URL=nats://127.0.0.1:4222`
  - `gemma-1` with `OLLAMA_MODEL=gemma4:latest`
  - `watchdog-1` with short test floor/tolerance for E5
- Ollama: local `ollama serve`, model `gemma4:latest`
- Note: the `nats:2.10-alpine` compose image did not include the `nats` CLI, so stream/consumer inspection used API-visible behavior rather than `nats stream info`.

## E1: Dashboard/API to Agent Command Result

Target: `shell-1`.

Method:

- `POST /api/command/shell-1?sender_id=test-runner`
- Body: deterministic `printf edgecitadel-e1-N`
- Poll `/api/messages?task_id=<task_id>&limit=50` until terminal `result`

Results:

| Trial | Task ID | Latency ms | Result state | Result body | Persisted messages |
|---|---:|---:|---|---|---:|
| 1 | `40f17e1a-c54a-4938-b482-7830e474f9fb` | 88.94 | `completed` | `edgecitadel-e1-1` | 2 |
| 2 | `86758cae-eb77-49ad-a495-091dc5804ef4` | 68.31 | `completed` | `edgecitadel-e1-2` | 2 |
| 3 | `4fc50c0e-1c0c-46f0-a176-a72ca16b7b95` | 69.80 | `completed` | `edgecitadel-e1-3` | 2 |

Takeaway: the local native-NATS command/result path is operational and produces one command plus one result row per task for simple shell work.

## E4: Token/Progress Streaming

Target: `gemma-1`.

Method:

- `POST /api/command/gemma-1?sender_id=test-runner`
- Prompt: short three-sentence response request
- Poll `/api/messages?task_id=<task_id>&limit=500`
- Record first observed `task.progress` and terminal `result`

Results:

| Trial | Task ID | First progress ms | Task latency ms | Progress frames | Result state | Persisted messages |
|---|---:|---:|---:|---:|---|---:|
| 1 | `27ebb443-287f-49a0-81a8-cba39b678e3c` | 6176.14 | 6961.85 | 8 | `completed` | 10 |
| 2 | `442587ee-19bb-4fbe-ab3f-cf5bdb2ca1a9` | 5553.40 | 6211.63 | 7 | `completed` | 9 |

Takeaway: `task.progress` rows are emitted and persisted for a streaming adapter, while the terminal `result` remains the canonical final answer. These timings include local model inference and are not broker-only latency.

## E5: Offline Recipient Failure

Target: temporary `e5-offline-3`.

Method:

- Publish one valid `register` and one `heartbeat` envelope for `e5-offline-3` over NATS.
- Agent Card declares `runtime.heartbeat_interval_sec: 10`, the schema minimum.
- Do not start a consumer for `e5-offline-3`.
- Send `POST /api/command/e5-offline-3?sender_id=test-runner`.
- Run watchdog with short test thresholds:
  - `WATCHDOG_THRESHOLD_FLOOR_SEC=2`
  - `WATCHDOG_TOLERANCE_SEC=1`
  - `WATCHDOG_DEFAULT_INTERVAL_SEC=1`
  - `WATCHDOG_CHECK_CADENCE_SEC=1`

Result:

| Task ID | Recovery ms | Result state | Error | Trigger | Persisted messages |
|---|---:|---|---|---|---:|
| `790b9ad8-9217-4048-b1bc-34bac952e63f` | 21688.56 | `failed` | `recipient_offline` | `heartbeat_staleness` | 2 |

Takeaway: the watchdog heartbeat-staleness path synthesized a canonical failed `result` for an offline recipient without requiring the target agent to consume its inbox.

## E7: Duplicate Publish Deduplication

Target: `shell-1`.

Method:

- Publish the same `command` envelope twice directly to JetStream subject `agents.shell-1.inbox`.
- Use the same envelope `id` as `Nats-Msg-Id` for both publishes.
- Poll `/api/messages?task_id=<task_id>&limit=100` for results.

Result:

| Task ID | Envelope ID | Latency ms | First seq | Second seq | Second duplicate | Result count | Result body |
|---|---|---:|---:|---:|---|---:|---|
| `e2031c64-b5d4-41c1-b31b-93abbebd8430` | `fc711d6c-c8ba-4ea8-b662-c46cda2d5e24` | 533.93 | 47 | 47 | `true` | 1 | `edgecitadel-e7-dedupe-2` |

Takeaway: JetStream duplicate-window behavior worked as expected. The second publish was acknowledged as duplicate and reused the first stream sequence; the agent executed once and emitted one result.

## Blockers and Caveats

- These are local functional baselines, not load benchmarks.
- E4 timing includes model inference and local model warmup effects.
- E5 used a synthetic fixture with the minimum valid 10s heartbeat interval plus shortened floor/tolerance settings; production `shell-1` uses a 30s heartbeat and would take roughly a minute for the same staleness path.
- E7 bypassed the aggregator API to control `Nats-Msg-Id`; the test validates JetStream dedupe, not the API command endpoint.
- The compose NATS image lacks the `nats` CLI, so future benchmark automation should either install/use a local CLI or collect stream stats via NATS monitoring/JetStream APIs.

## Next Experiment Slice

1. Add a repeatable harness for E1/E4/E5/E7 so these measurements are reproducible without inline scripts.
2. Add E6 crash-after-receive-before-ack using a purpose-built test consumer with a harmless side-effect counter.
3. Start MQTT ingress experiments only after the native baseline harness is stable.

## Harness Supersession Note

The manual baseline remains useful as the first observed local run, but repeatability now comes from generated outputs in `docs/research/results/`. Use `scripts/research/run_agent_benchmark.py` with `--workloads native --render-md` to regenerate E1, E4, E5, and E7 summaries against a running stack.
