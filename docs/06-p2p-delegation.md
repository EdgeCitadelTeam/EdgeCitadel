# P2P Agent-to-Agent Delegation

Agents can spontaneously communicate with other agents without going through the dashboard. When an agent's LLM determines it needs help from another agent, it outputs a delegation marker that the listener executes automatically.

## How It Works

1. Agent receives a command (from dashboard or another agent)
2. The listener calls the `openclaw agent` CLI with the message **plus the roster of online agents**
3. If the LLM's response contains `[DELEGATE:agent_name] message`, the listener:
   - Publishes the delegation to the target agent's inbox
   - Waits for the target to process and reply
   - Feeds the reply back to the LLM for synthesis
4. The LLM produces a final answer incorporating all delegation results

## Delegation Syntax

The LLM outputs this pattern in its response:

```
[DELEGATE:jeeves] Check all temperature sensors and report anomalies
[DELEGATE:percy] Send a push notification about the anomaly to all mobile devices
```

Multiple delegations in a single response are executed **in parallel**.

## Example Flow

```
Dashboard → Rupert: "Check all sensors and notify mobile devices"

  Rupert's LLM outputs:
    [DELEGATE:jeeves] Check all temperature sensors and report readings
    [DELEGATE:percy] Send push notification summary to all mobile devices

  Listener executes both delegations in parallel:
    → agents/jeeves/inbox: "Check all temperature sensors..."
    → agents/percy/inbox: "Send push notification summary..."

  Jeeves replies: "Sensors nominal. Living room 22.5C, Kitchen 24.1C..."
  Percy replies: "Push notification sent to 3 devices"

  Rupert's LLM gets re-invoked with results:
    [DELEGATION RESULTS]
    From jeeves: Sensors nominal. Living room 22.5C, Kitchen 24.1C...
    From percy: Push notification sent to 3 devices

  Rupert synthesizes final answer:
    "All sensors are nominal. Jeeves reports temperatures within range.
     Percy confirmed push notifications sent to 3 mobile devices."

Dashboard ← Rupert: final synthesized answer
```

## Multi-Hop Delegation

Delegations can chain across agents (Rupert → Jeeves → Percy), tracked by `chain_id` and `chain_depth`:

```
Rupert (depth 0) → delegates to Jeeves (depth 1) → delegates to Percy (depth 2)
```

Each hop increments `chain_depth`. The system enforces a hard limit to prevent runaway chains.

## Guardrails

| Guard | Default | Description |
|---|---|---|
| `MAX_DELEGATION_DEPTH` | 3 | Max delegation rounds per conversation |
| `MAX_CHAIN_DEPTH` | 5 | Hard limit on total chain depth across agents |
| `DELEGATION_TIMEOUT` | 90s | Time to wait for a delegation reply |
| `MAX_CONCURRENT_DELEGATIONS` | 5 | Max parallel delegations at once |
| Self-delegation | blocked | Agent cannot delegate to itself |
| Unknown target | skipped | Target must be in the online agent roster |
| Loop detection | hash-based | Content hashing detects A→B→A→B loops |

## Delegation Message Format

When a delegation is published to `agents/{target}/inbox`:

```json
{
  "from": "rupert",
  "to": "jeeves",
  "sender_id": "rupert",
  "receiver_id": "jeeves",
  "type": "delegation",
  "message_type": "command",
  "content": "Check the temperature sensors",
  "message": "Check the temperature sensors",
  "correlationId": "deleg-1710000000000-abc123",
  "correlation_id": "deleg-1710000000000-abc123",
  "chain_id": "chain-1710000000000-xyz789",
  "chain_depth": 1,
  "delegation": true,
  "timestamp": "2026-03-12T10:00:00.000Z"
}
```

## Agent Roster

Each agent periodically fetches the list of online agents from the aggregator API (`/api/agents`). The roster is included in the LLM prompt so the LLM knows which agents are available and what their roles are:

```
[AVAILABLE AGENTS]
- jeeves: Jeeves (IoT Controller)
- percy: Percy (Mobile Agent)
```

Roster refresh interval: 60 seconds (configurable via `ROSTER_REFRESH_SEC`).

## Dashboard Visibility

All delegation messages flow through the aggregator and appear on the dashboard:

- **Outbox publish**: The delegating agent publishes to its own outbox, so the dashboard sees the delegation request
- **Inbox delivery**: The delegation arrives on the target agent's inbox, creating a message record
- **Reply**: The target's reply appears on its outbox and the sender's inbox
- **Task auto-creation**: Delegations with correlation IDs auto-create tasks in the task board

## Configuration

Set these in `agent.env` or as environment variables:

```bash
MAX_DELEGATION_DEPTH=3       # Max rounds of delegation
DELEGATION_TIMEOUT=90        # Seconds to wait for reply
ROSTER_REFRESH_SEC=60        # Seconds between roster refreshes
CITADEL_API_URL=http://host  # API URL for roster fetching
```
