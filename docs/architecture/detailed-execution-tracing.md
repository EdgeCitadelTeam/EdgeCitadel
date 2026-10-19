# Detailed execution observations

The canonical trace journal/exporter/collector now retains transport, broker and
infrastructure families alongside execution, permission, model and tool evidence.
Core payload retention remains seven days, subject to the existing source/Core
quotas. No additional content store or change to task transactions is introduced.

## Evidence boundaries

The agentd durable outbox records a new publication attempt UUID per send, separate
from the logical message ID. It records JetStream acknowledgement stream, sequence,
domain and duplicate status. Inbound observations distinguish consumer receipt,
committed acceptance (including caller result acceptance), ACK send and termination.
An ACK send is a client observation, not proof of server receipt. Delivery metadata
includes consumer sequence and delivery count when the client exposes them.
Optional observer failures cannot retry tools, retire pending work or ACK work
that did not commit. A subsequent observation reports accumulated observer drops.

Execution publications request NATS message tracing with `Nats-Trace-Dest` and
`Nats-Trace-Only: false`. Trace destinations contain source, logical message and
publication attempt identity. Replies must match a durable outbox message and its
message-ID header. Server identity and bounded ingress/egress, JetStream and
account-boundary evidence are retained. Telemetry exports, broker replies and
presence traffic do not receive tracing headers. Subscription or permission
policies are not expanded by this implementation.

NATS tracing requires a supporting broker (2.11+) and publication/subscription
permission for `_EC.TRACE.<node>.>`. A header-free round-trip probe checks
that destination before enabling tracing, and checks again after reconnect.
Unsupported broker versions or unavailable telemetry permissions disable tracing
and emit an explicit collection gap without adding headers to task publications. See the official
[NATS header reference](https://docs.nats.io/nats-concepts/jetstream/headers).
Actual paths come from broker observations; missing responses remain missing
coverage. Topology polling is never evidence that a task traversed a link.

Set `EDGECITADEL_NATS_MONITOR_URL` in the agentd service environment to a scoped
monitoring endpoint. Every five seconds it polls `leafz`, `routez` and `gatewayz`,
coalescing unchanged link identity snapshots. Events identify polling provenance
and cadence. An unset endpoint records `not_configured`; failures record
`monitor_unavailable`. Local client authentication/permission errors are separate
broker observations, not agentd grant decisions. Broker-wide authentication logs
and system-account advisories are not collected without an appropriate source;
this implementation does not create accounts, gateways or new permission rules.

## Hermes content and availability

Use the execution-bound server described in the [Hermes guide](../../agent-packages/hermes/README.md).
The ordinary Hermes gateway does not activate these hooks. One observer belongs
to one bound request. Tool call IDs from Chat Completions and Responses API output
link tools to the invoking model span. Missing IDs are not inferred from timing.
Model observations include available request ID, finish status, usage, cache and
reasoning token counts, first-delta latency where exposed, and elapsed duration.
Provider-supplied reasoning summaries may be retained; private reasoning and
SDK-internal network attempts remain explicitly unavailable. Logical calls are
observed individually; there is no invented SDK retry timeline.

Content is optional, redacted text inside the existing event. Its combined fields
are capped at 8 KiB of canonical UTF-8 JSON, within the existing 16 KiB event cap.
Sensitive structured keys, authorization values, common credential formats,
private-key blocks and URLs are redacted before journaling. Binary values are
omitted. Unrepresentable or excessively complex content fails closed with
`content.status=unavailable` and `reason=redaction_failed`; metadata remains.
Oversized content is truncated before metadata. Arbitrary opaque secrets without
recognizable structure cannot be classified reliably by a pattern-based redactor.
Artifacts and content are inert text; the reader never fetches references.

Historical events without content stay readable and do not acquire fabricated
content or availability. `available`, `truncated`, `unavailable`, absent historical
instrumentation, expired payloads and source loss are distinct evidence states.
Retention expiry yields the existing history-expired response, not empty success.

## Read API and dashboard

`GET /api/trace-infrastructure` reads taskless infrastructure, transport, broker and
security events. Optional filters: `source`, `family`, `since`, `until`, `limit`
(1–100), and `cursor`. Times are Core receipt milliseconds. Signed cursors bind
filters and a fixed ingestion upper bound. A page scans at most 500 raw positions;
an empty page can have a continuation. `expired_scanned` counts expired scanned
records whose family can no longer be determined; it is not a family loss count.

Execution event pages expose a separate `receipt_times` map keyed by
`node_id/source_epoch/event_id`; immutable source events are not rewritten.
The inspector reads redacted content and receipt time from the selected retained
snapshot. Messaging/broker nodes live in an optional lane, collapsed initially.
Agent work, model/tools and authorization remain independently selectable.
Lane counts describe observed records, not complete instrumentation. Unsupported
families and run/source collection gaps remain separate from task outcomes.
Infrastructure events have their own API and never enter execution ancestry solely
because their timestamps overlap an execution.

## Deployment and verification

Deploy matching Core schemas/readers before producers and the matching frontend.
Preserve installed Hermes identity configuration when updating package code.
Restarting a source must not delete its journal or Core history. A separate bound
Hermes listener can coexist with an existing gateway on another loopback port.

`e2e/helpers/detailed-trace-broker.py` runs an owned two-broker fixture on jim-eq,
without touching shared streams. `detailed-trace-offline.py` verifies an existing
snapshot with both source daemons stopped and restores them in a `finally` block;
run only during an authorized acceptance window. `e2e/tests/detailed-trace.spec.js`
uses `EDGECITADEL_DETAILED_TRACE_ID`, `APP_URL=http://jim-eq` and `AGG_URL=http://jim-eq`
for the retained-run browser checks. Exact revisions, deployed hashes and limits
are recorded in `local-docs/research/detailed-trace-implementation.md`.
