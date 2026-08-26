# Documentation

This directory contains the maintained EdgeCitadel reference. Git history is
the archive for completed plans and superseded architecture documents.

## Start here

| Need | Document |
|---|---|
| Understand the components | [Architecture](01-architecture.md) |
| Deploy a host | [Server setup](02-server-setup.md) |
| Integrate an agent | [Agent contract](agent-contract.md) and [registration](03-agent-registration.md) |
| Implement messaging | [Messaging](05-messaging.md) |
| Use the HTTP/WebSocket API | [API reference](08-api-reference.md) |
| Operate the system | [Monitoring](09-monitoring.md) |
| Run verification | [Testing](10-testing.md) |

## Product guides

- [Dashboard](04-dashboard.md)
- [P2P delegation](06-p2p-delegation.md)
- [Task management](07-task-management.md)
- [Linux production setup](02-server-setup-linux.md)
- [Lab controller setup](setup-lab-controller.md)
- [Lab node setup](setup-lab-node.md)

## Decisions and planning

- [`adr/`](adr/) records accepted architectural decisions.
- [Roadmap](roadmap.md) contains future work only; implemented behavior belongs
  in the product references above.

Research artifacts are not part of the runtime product documentation.
