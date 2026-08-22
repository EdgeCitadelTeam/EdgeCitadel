# NATS for Agent Communication: Literature Review and Research Directions

Date: 2026-06-26

This note is a first-pass research base for a paper on NATS as an agent communication substrate, both through IoT-oriented ingress paths and outside IoT using native NATS. The literature is split into direct NATS work, adjacent message-broker and IoT studies, and multi-agent communication protocol work.

## Working Thesis

NATS is a strong candidate for edge and multi-agent communication when the system needs low-latency pub/sub, request-reply, queue groups, subject-level authorization, durable work queues through JetStream, and topology options such as leaf nodes. It is less a semantic agent protocol by itself than a transport and coordination substrate on which agent protocols can run.

The useful paper distinction is:

- Through IoT: agents or devices enter through MQTT-compatible or constrained-device paths, then are normalized into a NATS/JetStream backbone.
- Not through IoT: agents use native NATS subjects, services, JetStream, KV, and request-reply directly, avoiding MQTT semantics and central HTTP relay overhead.

Direct academic work on "NATS for AI agents" remains sparse as of this search. The strongest contribution opportunity is therefore to connect three bodies of work that are usually separate: message-oriented middleware benchmarks, IoT edge protocol studies, and agent communication semantics.

## Search Log

Searches covered the following query families:

- "NATS messaging system IoT paper pub/sub"
- "NATS vs MQTT IoT performance evaluation paper"
- "NATS JetStream edge computing IoT research paper"
- "NATS multi-agent communication agent messaging paper"
- "NATS JetStream site:arxiv.org"
- "Benchmarking Message Brokers for IoT Edge Computing"
- "agent communication protocols MCP A2A ACP arxiv"
- "KQML FIPA ACL multi-agent communication survey"
- "Contract Net Protocol distributed problem solver"
- "JADE FIPA multi-agent systems"
- "MQTT CoAP DDS IoT protocol performance evaluation"

OpenCLI was not available in this environment, so this pass used web search and direct paper/source pages.

## Source Matrix

### Direct NATS and Edge-Agent Sources

| Source | Type | Relevance | Takeaways for the paper |
|---|---|---|---|
| Zhan, Zhang, Haddadi. "Poster: EdgeCitadel - Hybrid NATS-MQTT Orchestration for Edge Multi-Agent Systems." arXiv:2606.14710, submitted 2026-04-20. https://arxiv.org/abs/2606.14710 | Direct paper/poster | Directly matches edge-resident agents, NATS 2.10, MQTT adapter, JetStream persistence, peer delegation. | Use as the closest existing research anchor. It frames hybrid MQTT ingress plus native NATS backend as a single-broker architecture for heterogeneous edge agents. |
| NATS.io. "What's old is new: A NATS-native protocol for AI agents." 2026-05-25. https://nats.io/blog/nats-native-protocol-for-ai-agents/ | Engineering source/protocol proposal | Not academic, but recent and directly on NATS-native AI agents. | Useful for production framing: discovery, conversation, and liveness map cleanly to agent needs. Shows NATS community is explicitly moving toward agent protocols. |
| Paul, Lertpongrujikorn, Nguyen, Salehi. "Benchmarking Message Brokers for IoT Edge Computing: A Comprehensive Performance Study." arXiv:2603.21600, submitted 2026-03-23. https://arxiv.org/abs/2603.21600 | Benchmark paper | Evaluates Mosquitto, EMQX, HiveMQ, RabbitMQ, ActiveMQ Artemis, NATS Server, Redis Pub/Sub, and Zenoh Router under edge-like constraints. | Strong empirical source for the "why NATS at the edge" section. It reports native lightweight brokers achieving sub-millisecond latency and finds multi-threaded systems such as NATS and Zenoh scale efficiently under high connection loads. |
| Jain, Ahuja, Saini. "Evaluation and Performance Analysis of Apache Pulsar and NATS." LNDECT 73, Springer, 2022. https://link.springer.com/chapter/10.1007/978-981-16-3961-6_16 | Benchmark/comparison paper | Compares NATS and Apache Pulsar architecture and benchmarking. | Use to place NATS among cloud-native brokers, especially where Pulsar is the alternative for durable distributed messaging. |
| Maartens, Brink. "Selecting a simple, natively implemented middleware solution for the SALT control system." SPIE, 2018. DOI 10.1117/12.2313106. https://www.researchgate.net/publication/326309531_Selecting_a_simple_natively_implemented_middleware_solution_for_the_SALT_control_system | Case-study/performance paper | Compares NATS, DDS, and a legacy HTTP implementation in a telescope control system. | Important precedent outside IoT and outside AI: NATS was chosen for simpler native integration and acceptable performance versus DDS/HTTP. Helps argue that NATS is attractive when operational simplicity matters. |
| Bhat, Priya. "Modern Messaging Queues - RabbitMQ, NATS and NATS Streaming." IJRTE, 2020. DOI 10.35940/ijrte.B3551.079220. https://www.ijrte.org/wp-content/uploads/papers/v9i2/B3551079220.pdf | Survey/benchmark paper | Summarizes RabbitMQ, NATS, and NATS Streaming features and workloads. | Use cautiously because NATS Streaming is legacy. Still helpful as earlier literature showing NATS in comparative broker studies. |
| Quevedo. "Practical NATS: From Beginner to Pro." Apress, 2018. https://link.springer.com/book/10.1007/978-1-4842-3570-6 | Book | Early NATS technical book. | Background source for NATS design, protocol, client internals, cloud-native control-plane framing, heartbeats, tracing, and request-reply. |

### NATS Official Technical Sources

| Source | Relevance | Use in paper |
|---|---|---|
| NATS docs: MQTT support. https://docs.nats.io/running-a-nats-service/configuration/mqtt | NATS supports MQTT since server 2.2 and maps MQTT topics to NATS subjects. Docs frame MQTT support as a path for existing IoT investments, while preferring native NATS for greenfield deployments when possible. | Core source for "through IoT" architecture and topic/subject translation constraints. |
| NATS docs: JetStream. https://docs.nats.io/nats-concepts/jetstream | JetStream adds persistence, acknowledgments, KV, object store, history/watch, and durable consumers. | Source for durable agent inboxes, replay/audit, KV-based discovery, and long-running task robustness. |
| NATS docs: Request-Reply. https://docs.nats.io/nats-concepts/core-nats/reqreply | Request-reply is built on pub/sub and reply inboxes; supports dynamic responders and observability through normal subscriptions. | Source for agent command/response and tool-invocation patterns without HTTP. |
| NATS docs: Queue Groups. https://docs.nats.io/nats-concepts/core-nats/queue | Queue subscribers give built-in load balancing and fault-tolerant service scaling. | Source for scaling a pool of equivalent agents or tool workers. |
| NATS docs: Leaf Nodes. https://docs.nats.io/running-a-nats-service/configuration/leafnodes | Leaf nodes bridge local and remote NATS systems, useful for IoT and edge scenarios, with local RTT and security-domain separation. | Source for distributed edge topologies across home labs, factories, phones, laptops, and cloud. |
| NATS docs: Authorization. https://docs.nats.io/running-a-nats-service/configuration/securing_nats/authorization | Subject-level publish/subscribe permissions, response permissions, and account isolation. | Source for agent identity, least privilege, subject-scoped capabilities, and secure reply subjects. |

### IoT and Edge Communication Literature

| Source | Type | Relevance | Takeaways for the paper |
|---|---|---|---|
| Chen, Kunz. "Performance Evaluation of IoT Protocols under a Constrained Wireless Access Network." IEEE MoWNeT, 2016. https://people.computing.clemson.edu/~jmarty/projects/lowLatencyNetworking/papers/MQTT-NDN/PerformanceofIOTProtocolsoverConstrainedWireless.pdf | IoT protocol benchmark | Compares MQTT, CoAP, DDS, and custom UDP under constrained bandwidth, latency, and packet loss. | Useful for arguing that constrained links change protocol choice. DDS can improve reliability/latency at bandwidth cost; MQTT remains practical for pub/sub telemetry. |
| Silva, Carvalho, Soares, Sofia. "A Performance Analysis of Internet of Things Networking Protocols: Evaluating MQTT, CoAP, OPC UA." Applied Sciences, 2021. DOI 10.3390/app11114879. https://www.mdpi.com/2076-3417/11/11/4879 | IoT protocol benchmark | Evaluates MQTT, CoAP, and OPC UA in consumer/industrial IoT settings. | Useful as broad IoT benchmark context. Does not cover NATS, so use it to motivate why MQTT ingress remains relevant. |
| Wang, Woisetschlager. "Agentic Performance at the Edge: Insights from Benchmarking." arXiv:2605.10384, submitted 2026-05-11. https://arxiv.org/abs/2605.10384 | Edge-agent benchmark | Studies agentic AI quality under edge constraints: memory, power, latency, model size, and tool workflows. | Connects agent communication to model/runtime constraints. Transport should be evaluated with model-tool latency, not only raw broker latency. |

### Multi-Agent Communication and Protocol Literature

| Source | Type | Relevance | Takeaways for the paper |
|---|---|---|---|
| Smith. "The Contract Net Protocol: High-Level Communication and Control in a Distributed Problem Solver." IEEE Transactions on Computers, 1980. https://www.eecs.ucf.edu/~lboloni/Teaching/EEL6788_2008/papers/The_Contract_Net_Protocol_Dec-1980.pdf | Classic MAS protocol | Defines task distribution by negotiation among nodes in distributed problem solving. | Important historical anchor: agent delegation is not new. NATS can provide the transport for contract-net-like negotiation, but the task semantics must be defined above the broker. |
| Finin, Fritzson, McKay, McEntire. "KQML as an Agent Communication Language." CIKM 1994. https://ebiquity.umbc.edu/paper/html/id/330/KQML-as-an-agent-communication-language | Classic ACL paper | KQML is both a message format and a message-handling protocol for runtime knowledge sharing among agents. | Use to separate transport from communicative acts. NATS subjects route messages; KQML/FIPA-like layers define what those messages mean. |
| Bellifemine, Poggi, Rimassa. "Developing Multi-agent Systems with JADE." ATAL 2000 / Springer, 2001. https://link.springer.com/chapter/10.1007/3-540-44631-1_7 | MAS platform paper | JADE implements a FIPA-compliant multi-agent platform with flexible messaging. | Useful comparison point: classic MAS platforms bundled runtime, registry, message transport, and ACL semantics. NATS is lower-level and more composable. |
| Ehtesham, Singh, Gupta, Kumar. "A survey of agent interoperability protocols: MCP, ACP, A2A, and ANP." arXiv:2505.02279, revised 2025-05-23. https://arxiv.org/abs/2505.02279 | Recent LLM-agent protocol survey | Compares modern protocols across tool access, multimodal messaging, peer delegation, discovery, DIDs, and security models. | Use for modern agent ecosystem framing. NATS can serve as transport for A2A/ACP-style semantics or as an internal substrate behind those external protocols. |
| Yuan et al. "Beyond Message Passing: A Semantic View of Agent Communication Protocols." arXiv:2604.02369, revised 2026-04-13. https://arxiv.org/abs/2604.02369 | Recent agent protocol taxonomy | Organizes agent communication into communication, syntactic, and semantic layers; finds current protocols strong on transport/schema but weak on clarification, context alignment, and verification. | Key theoretical source. It helps position NATS as the communication layer, while the paper can propose missing semantic-layer mechanisms for NATS-backed agents. |

## Synthesis by Research Question

### 1. Why NATS for agent communication?

NATS provides a small set of primitives that map well to multi-agent systems:

- Subjects: stable routing namespace for agents, tasks, tools, memory, and system events.
- Pub/sub: event broadcast, telemetry, status, and passive observability.
- Request-reply: synchronous tool calls, command/response, and no-responder detection.
- Queue groups: horizontal scaling for equivalent responders and local failover.
- JetStream: durable inboxes, replay, at-least-once work queues, acknowledgments, poison-message detection, and audit.
- KV/Object Store: live agent registry, capability discovery, configuration, and larger artifacts.
- Leaf nodes and superclusters: edge/cloud and local/remote topology without replacing the programming model.
- Authorization: subject-scoped least privilege and account isolation.

For an agent system, the main claim is not "NATS is an agent protocol." The stronger claim is "NATS is a compact distributed-systems substrate that removes much of the transport, liveness, fanout, backpressure, and observability plumbing that agent frameworks otherwise rebuild."

### 2. Through IoT: MQTT ingress into NATS

This is the compatibility path. Many constrained devices and deployed sensors already speak MQTT. NATS' MQTT adapter can let those devices publish to a NATS-backed system without a separate broker, while backend agents and services consume native NATS subjects.

Best use cases:

- Existing IoT firmware cannot be changed easily.
- Sensors produce telemetry that gateway agents normalize into canonical envelopes.
- MQTT is an ingress protocol, not the internal fleet substrate.

Risks and constraints:

- MQTT topics and NATS subjects differ; slash-to-dot translation is not perfectly semantic.
- MQTT-centric guarantees and operational assumptions do not map one-to-one to Core NATS or JetStream.
- MQTT adapter exposure expands the attack surface and should be explicit.
- If MQTT 5.0 or deep IoT broker features are needed, a dedicated MQTT broker plus NATS bridge may be cleaner.

### 3. Not through IoT: Native NATS agent fabric

This is the cleaner path for first-party agents. Agents publish and subscribe directly to NATS subjects, use JetStream for durable work, and avoid HTTP relay or MQTT topic semantics.

Best use cases:

- Agents run on servers, laptops, home hubs, phones, or containers that can run a maintained NATS client.
- The system needs peer-to-peer delegation, passive audit, and direct streaming.
- Each agent has identity, subject permissions, and capability metadata.
- Latency and observability matter more than compatibility with existing MQTT devices.

Research angle:

- Compare native NATS, MQTT relay, HTTP/SSE A2A gateway, and message-broker alternatives under the same agent workload.
- Measure not only broker throughput, but task latency, recovery after agent dropout, replay accuracy, security policy complexity, and semantic failure rate.

### 4. Transport is solved better than semantics

Recent agent protocol surveys argue that modern systems increasingly solve transport, streaming, schema, and lifecycle management, but still leave semantic alignment, clarification, context repair, and verification to prompts or application code.

That is directly relevant to a NATS paper. A NATS system can route messages extremely well, but it does not know whether an agent understood a task, whether context is aligned, or whether a delegated result satisfies the original intent. The research opportunity is a protocol layer over NATS subjects that makes these semantics explicit.

## Proposed Paper Structure

1. Introduction
   - Edge AI agents are distributed systems.
   - Existing agent protocols often assume HTTP, cloud relays, or vendor gateways.
   - NATS offers a lightweight communication substrate that can span edge, IoT, and cloud.

2. Background
   - NATS Core, JetStream, KV, request-reply, queue groups, leaf nodes, MQTT adapter.
   - Classic MAS communication: Contract Net, KQML, FIPA/JADE.
   - Modern LLM-agent protocols: MCP, ACP, A2A, ANP.

3. Architecture Patterns
   - Pattern A: MQTT/IoT ingress into NATS.
   - Pattern B: native NATS agent fabric.
   - Pattern C: external A2A/HTTP edge with internal NATS substrate.

4. Evaluation Design
   - Workloads: telemetry fan-in, command/response, peer delegation, streaming, offline/replay.
   - Metrics: end-to-end task latency, tail latency, message loss/replay, CPU/memory, recovery time, policy complexity, semantic failure rate.
   - Baselines: Mosquitto MQTT relay, native NATS, NATS+MQTT adapter, HTTP/SSE gateway, maybe Zenoh or RabbitMQ.

5. Discussion
   - When to use MQTT versus native NATS.
   - What NATS solves and what agent protocols still need.
   - Security and governance implications.

6. Future Work
   - Semantic protocols over NATS.
   - Multi-domain authorization.
   - Edge benchmark suite for agentic workloads.
   - Federated agent discovery.

## Future Research Directions

1. NATS-native semantic agent protocol

Define a protocol over NATS subjects that separates transport, syntax, and semantics:

- Discovery: service ping/info or KV-backed agent cards.
- Conversation: typed stream chunks, task state, cancellation, retries.
- Semantics: clarification requests, context alignment checks, result verification, refusal reasons, and provenance.

This would directly answer the gap identified by recent semantic-agent-protocol work.

2. Agentic edge benchmark suite

Existing broker benchmarks measure latency, throughput, resource use, and connection scaling. Agent systems need additional workloads:

- Multi-hop delegation chains.
- Tool calls with long-running tasks.
- Token streaming under constrained links.
- Agent crash/restart during a task.
- Offline recipient with delayed replay.
- Conflicting context or stale memory.

Metrics should include completion quality and semantic failure, not only transport performance.

3. MQTT ingress versus native NATS controlled experiment

Build the same fleet in two modes:

- MQTT-first: agents publish through MQTT topics, then a gateway/aggregator relays.
- NATS-native: agents publish to NATS subjects and JetStream directly.

Measure latency, CPU, failure recovery, observability, authentication complexity, and implementation size on Raspberry Pi, Mac Mini, cloud VM, and phone/Android clients.

4. Subject-level security model for agents

Map agent capabilities to NATS permissions:

- Agents can publish only to their own outbox and permitted peer inboxes.
- Tool agents can subscribe only to their tool subject namespace.
- Request-reply responders use scoped response permissions.
- Cross-tenant or cross-home fleets use account isolation and leaf-node imports/exports.

Open question: how to express dynamic delegation rights without over-broad wildcard permissions.

5. Durable memory and replay semantics

JetStream and KV can store messages, state, and histories, but agent memory has semantics beyond persistence:

- Which messages are canonical facts?
- Which messages are audit-only?
- Which context should replay after crash?
- How should redelivery interact with non-idempotent tools?

This is a strong research direction because NATS gives the storage primitives but not the agent-level memory contract.

6. Edge federation through leaf nodes

Study leaf-node topologies where homes, labs, factories, or phones run local NATS servers that connect to regional/cloud brokers:

- Local-first routing and queue affinity.
- Behavior during WAN partition.
- Data minimization across security domains.
- Capability discovery across imported/exported subject spaces.

7. External protocol gateway design

Use A2A/ACP/HTTP for external interoperability while preserving native NATS inside the fleet:

- External request enters via A2A gateway.
- Gateway mints a canonical NATS envelope with preserved provenance.
- Internal agents delegate over NATS.
- Results stream back through the external protocol.

Research question: can this preserve enough semantic and security context across protocol boundaries?

8. Formal subject namespace design

Explore how NATS subjects encode agent identity, task identity, capability, tenancy, and environment:

- `agents.{id}.inbox`
- `agents.{id}.outbox`
- `tasks.{id}.progress`
- `tools.{capability}.request`
- `memory.{scope}.{context}.get`

The subject namespace becomes a governance and observability surface, not just a routing convention.

## Gaps in Existing Literature

- Very little peer-reviewed work directly studies NATS for AI agent communication.
- Existing NATS comparisons focus on broker performance, not agent semantics.
- IoT protocol papers rarely include NATS, especially JetStream and leaf-node deployments.
- Agent protocol papers usually focus on HTTP, JSON-RPC, REST, DIDs, or semantic layers rather than broker substrates.
- Benchmarks rarely include failure modes central to agents: task cancellation, partial progress streams, redelivery side effects, memory replay, prompt/context drift, and semantic disagreement.

## Positioning Claim

A strong contribution would be:

"We evaluate NATS not as a replacement for agent communication languages, but as a low-latency, durable, observable, and security-scoped substrate for deploying agent protocols across edge and IoT environments. We show when MQTT ingress is useful, when native NATS is preferable, and which semantic protocol responsibilities remain above the transport layer."

## Notes for EdgeCitadel Context

The local project already embodies this positioning:

- `docs/05-messaging.md` defines native NATS plus JetStream as the canonical fleet messaging substrate.
- MQTT ingress is deploy-time opt-in and reserved for constrained IoT sensors.
- `docs/adr/0010-nats-native-l2-delegation.md` keeps in-fleet delegation NATS-native and reserves HTTP+SSE A2A for external ingress.
- `docs/adr/0001-nats-over-mqtt-broker.md` captures the original choice to replace a standalone MQTT broker with NATS plus JetStream and MQTT compatibility.

This gives the paper a practical architecture to evaluate rather than only a conceptual design.
