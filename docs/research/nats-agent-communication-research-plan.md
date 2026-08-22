# NATS for IoT-Deployed Agent Communication Research Plan

Date: 2026-07-04

## Working Thesis

IoT-deployed agent systems have two communication problems that should not be collapsed:

1. **Device ingress**: constrained sensors, firmware, and brownfield devices need a compatibility protocol, usually MQTT.
2. **Agent coordination**: software agents need durable task delivery, peer delegation, liveness, streaming progress, replay, observability, and scoped authority.

The research claim is:

> MQTT remains valuable at the device edge, but native NATS plus JetStream is the better internal fabric for coordinating deployed edge agents. NATS is not a semantic agent language; it is the distributed-systems substrate beneath one.

EdgeCitadel is the reference implementation for this claim. Its current contract already separates MQTT ingress from in-fleet NATS coordination: canonical envelopes, A2A-shaped Agent Cards, JetStream inboxes, outbox mirrors, heartbeat/status subjects, task progress streams, watchdog recovery, and bridge/gateway roles.

## Narrative Spine

The paper should open with a concrete deployment problem rather than with NATS features:

An edge fleet has a Raspberry Pi near sensors, a Mac mini running a local model, a cloud VM hosting a dashboard and broker, and bridge adapters exposing external agent runtimes. A user asks one agent to diagnose a device. The request becomes a distributed workflow: discover peers, select a capability, deliver a command, stream progress, delegate a subtask, survive a device restart, record an audit trail, and reject unauthorized actions.

MQTT helps one part of that system: getting constrained device traffic into the fleet. It does not, by itself, define durable agent work queues, task lifecycle, peer delegation, replay policy, or semantic verification.

Native NATS helps the internal coordination problem because its primitives match the system shape:

- Subjects name agents, tasks, tools, memory, and system events.
- Core pub/sub carries liveness, progress, logs, and broadcast.
- JetStream carries durable inbox work with ack, redelivery, deduplication, and poison-message advisory.
- KV or cached Agent Cards provide discovery and capability metadata.
- Request-reply supports short tool calls without requiring HTTP.
- Queue groups scale equivalent tool workers.
- Subject permissions can encode capability boundaries.
- Leaf nodes can keep local edge traffic local while connecting sites or homes.

The paper should then make the boundary explicit: NATS solves communication mechanics, not agent meaning. The semantic layer above it must define clarification, context alignment, result verification, refusal, provenance, delegation contracts, and memory replay rules.

## Claim Ladder

- **C1: Edge agents are distributed systems.** They need liveness, work delivery, replay, fanout, telemetry, security boundaries, and failure recovery.
- **C2: IoT ingress and agent coordination are different layers.** MQTT is often right for constrained devices; native NATS is cleaner for software agents.
- **C3: NATS maps unusually well to the coordination layer.** Its subjects, JetStream, request-reply, queue groups, authorization, and leaf nodes cover many mechanics agent frameworks otherwise rebuild.
- **C4: Broker benchmarks are not enough.** Agent workloads need peer delegation, streaming, cancellation, offline recovery, replay correctness, and semantic failure measurements.
- **C5: NATS is a substrate, not the whole protocol.** The remaining research problem is a semantic agent layer over a strong communication substrate.

## Research Questions

| ID | Question | Expected contribution |
|---|---|---|
| RQ1 | When should an IoT-deployed agent system use MQTT ingress versus native NATS? | A decision framework separating constrained-device ingress from first-party agent coordination. |
| RQ2 | Does native NATS reduce latency, relay complexity, and failure coupling versus MQTT/HTTP relay designs? | Empirical comparison across command, delegation, streaming, and recovery workloads. |
| RQ3 | Which agent messages require JetStream durability, and which should remain ephemeral Core NATS? | A message-classification rule for inboxes, progress, liveness, logs, broadcast, and audit. |
| RQ4 | Can Agent Cards plus NATS subjects become a practical capability and authorization model? | A subject-permission design for own outbox, permitted peer inboxes, tool subjects, memory, and scoped replies. |
| RQ5 | Where does transport replay become unsafe for agents? | Replay semantics for memory, side effects, idempotency, and non-idempotent tools. |
| RQ6 | What semantic protocol elements remain above NATS? | Requirements for clarification, verification, context checks, refusal, provenance, and delegation contracts. |
| RQ7 | Can valid IoT transport still manipulate an agent's world model through stale, replayed, or contradictory events? | A temporal integrity benchmark for provenance, freshness, trust tier, and confirmation gates above NATS/MQTT. |

## Research Tracks

### Track 1: Architecture Comparison

Compare three deployment patterns:

- **MQTT ingress into NATS**: constrained devices publish MQTT-style telemetry or commands; a gateway normalizes them into canonical agent envelopes.
- **Native NATS agent fabric**: first-party agents publish directly to `agents.{id}.inbox`, mirror to `agents.{id}.outbox`, stream progress, and use JetStream for durable work.
- **External protocol gateway into NATS**: A2A/HTTP/SSE or MCP clients enter through a gateway, but in-fleet delegation remains NATS-native.

Deliverable: a design decision table that says which pattern to use by device capability, trust boundary, workload type, failure tolerance, and operational cost.

### Track 2: Agentic Edge Benchmark

Build an experiment suite around workloads that generic broker benchmarks do not cover:

- dashboard-to-agent command/result;
- agent-to-agent delegation;
- multi-hop delegation with shared `context_id`;
- token/progress streaming;
- long-running task with periodic `in_progress`;
- offline recipient and watchdog-synthesized failure;
- agent crash after receive but before ack;
- duplicate publish under stable `Nats-Msg-Id`;
- cancellation;
- non-idempotent tool redelivery;
- unauthorized subject access.

Deliverable: a benchmark matrix with per-workload metrics and repo implementation status.

### Track 3: IoT Ingress Semantics

Study MQTT as an ingress-only compatibility layer:

- topic-to-subject translation;
- MQTT QoS and retained-message expectations versus Core NATS and JetStream;
- gateway normalization into canonical envelopes;
- telemetry versus command semantics;
- attack-surface increase when MQTT is enabled;
- cases where a dedicated MQTT broker plus bridge is cleaner than NATS' built-in MQTT adapter.

Deliverable: an ingress pattern that keeps firmware compatibility without letting MQTT semantics leak into internal agent coordination.

### Track 4: Subject Namespace and Security

Treat the subject tree as a governance surface:

- `agents.{id}.inbox`: who can send work to an agent;
- `agents.{id}.outbox`: what the sender can auditably claim it sent;
- `agents.{id}.task_progress.{task_id}`: who can observe streaming state;
- `tools.{capability}.request`: how tool pools are exposed;
- `memory.turns.{get,put,delete}`: who can access context memory;
- `$JS.EVENT...`: who can observe broker-level failure advisories.

Deliverable: a least-privilege policy model tied to Agent Card roles and runtime conformance levels.

### Track 5: Semantic Layer Above NATS

Define the protocol requirements that NATS intentionally does not solve:

- clarification request and answer;
- context alignment check;
- result verification request and verdict;
- refusal with machine-readable reason;
- provenance and source attribution;
- delegation contract with expected output shape;
- replay policy for memory versus side effects.

Deliverable: a minimal semantic extension proposal over the current envelope rather than a replacement for the transport contract.

### Track 6: Temporal IoT Semantic Security

Simulate real-life IoT timing disorder and benign adversarial content using synthetic devices only. Scenarios cover out-of-order telemetry, cross-device gaps, replay bursts, split-brain site state, telemetry prompt injection markers, and delayed fake actuator acknowledgements.

Deliverable: S1-S6 benchmark outputs that measure `event_time_skew_ms`, `arrival_order_inversions`, `stale_events_accepted`, `consistency_checks_requested`, `unsafe_recommendations`, `fake_tool_calls_attempted`, and `prompt_injection_followed`. These results separate broker authorization from semantic integrity of the agent's world model.

## Paper Structure

1. **Introduction**
   - Edge agents are distributed systems deployed across devices, gateways, local models, and cloud services.
   - IoT ingress and agent coordination are separate communication layers.
   - Contribution: architecture split, EdgeCitadel case study, agentic edge benchmark design, and semantic-gap analysis.

2. **Background**
   - NATS Core, JetStream, request-reply, queue groups, KV/object store, authorization, leaf nodes, MQTT adapter.
   - IoT protocol constraints: MQTT, CoAP, DDS, OPC UA, constrained wireless links, firmware compatibility.
   - Agent communication: Contract Net, KQML, FIPA/JADE, MCP, A2A, ACP, ANP, and semantic-agent protocol work.

3. **Reference Architecture**
   - MQTT ingress as a compatibility boundary.
   - Native NATS fabric for durable inboxes, progress streams, liveness, discovery, delegation, and passive audit.
   - External A2A/MCP gateways as edge surfaces, not the in-fleet path.

4. **Evaluation Design**
   - Architecture modes: MQTT ingress, native NATS, external gateway.
   - Workloads: command, delegation, streaming, offline recovery, crash/restart, duplicate publish, cancellation, authorization.
   - Metrics: task latency, p95/p99, recovery time, CPU/memory, dropped progress frames, duplicate behavior, implementation size, policy complexity, semantic failure rate.

5. **Results and Analysis**
   - Where native NATS reduces relay overhead and failure coupling.
   - Where MQTT remains justified.
   - Which messages need durability and which should stay ephemeral.
   - What replay does and does not guarantee.

6. **Discussion**
   - NATS as a substrate, not an agent language.
   - Subject namespaces as both routing and governance.
   - Semantic layer requirements above the existing envelope.
   - Temporal integrity, provenance, freshness, and action gates for IoT-derived state.

7. **Future Work**
   - NATS-native semantic protocol.
   - Leaf-node federation across homes, labs, factories, and mobile devices.
   - Dynamic delegation authority.
   - Durable memory and non-idempotent tool semantics.

## Immediate Next Steps

1. Use `nats-agent-communication-experiment-matrix.md` as the experiment backlog.
2. Choose the first runnable slice: native NATS command/result, streaming, offline recipient, and duplicate publish.
3. Add a lightweight experiment harness only after the manual command paths are verified against the current stack.
4. Keep MQTT ingress as a separate slice; do not mix it into the native NATS baseline.
5. Turn the claim ladder into the introduction draft before collecting results, so experiments answer explicit claims.
6. Run the synthetic temporal IoT overlay after E12 so the security discussion covers both subject permissions and semantic world-state integrity.

## Non-Goals

- Do not argue that NATS replaces agent communication languages.
- Do not treat MQTT as the internal first-party agent transport.
- Do not use generic broker throughput alone as the evaluation.
- Do not redesign the current envelope before the experiments identify a concrete semantic gap.
- Do not add runtime features to EdgeCitadel solely for paper completeness without a measurable research question.
