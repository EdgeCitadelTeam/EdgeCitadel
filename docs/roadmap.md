# Roadmap

This file contains only work that is not implemented. Current behavior belongs
in the product guides, and completed plans remain available in Git history.
Priorities and delivery status are tracked in repository issues.

## Reliability and operations

- Make JetStream-dependent tests fail fast when a broker is unavailable.
- Automate starting the host-side test adapters required by the isolated
  Playwright agent round trips.
- Decide whether registration and heartbeat events require a durable audit
  history in addition to the current agent-state projection.
- Add production evidence for backup restore, upgrade, rollback, and host
  deployment on every supported platform.

## Agent and task workflows

- Decide whether task creation remains derived from message traffic or returns
  as an explicit API and dashboard workflow.
- Add further model backends as separate adapters when there is a maintained
  deployment and test path for each one.
- Define application-level idempotency requirements for handlers with external
  side effects; transport deduplication alone cannot provide exactly-once side
  effects.

## Security and fleet scale

- Replace shared development credentials with a documented production
  authorization and credential-rotation design.
- Validate queue, database, and dashboard behavior beyond the current small
  single-host fleet assumptions before claiming larger-scale support.
- Define artifact/object-storage handling for message bodies that should not be
  carried in the canonical envelope.

## Test-data isolation

The aggregator projects a message's `deployment` from the sender or recipient
agent card. Tests should register a dedicated agent with
`metadata.runtime.deployment: test`. A test that drives a production-registered
agent cannot currently mark its messages as test data without changing the
wire contract; solve that through a dedicated test publisher rather than an
ad-hoc envelope field.
