# Dashboard

The React dashboard provides real-time visibility into all agent activity. It connects via WebSocket for live updates and REST API for historical data.

## Tabs

Switch tabs with the sidebar buttons or keyboard shortcuts `1`-`4`.

### Chat (1)

The primary view for agent communication.

**Message List:**
- Displays all inter-agent and dashboard-to-agent messages
- Groups command-reply pairs by correlation ID
- Auto-scrolls to latest; "Jump to Latest" button when scrolled up
- "Load older messages" pagination at the top
- Pending command indicators show a spinner until the agent replies

**Filters:**
- **Type dropdown**: All types, command, result, alert, info, broadcast, task_assign
- **Search**: Full-text search across payload, sender, and receiver
- **Clear filters** button to reset

**Message Bubble:**
- Color-coded by type (command=blue, result=green, alert=red, broadcast=indigo)
- Shows sender → receiver flow
- Timestamp (relative, e.g., "2 min ago")
- Correlation ID badge (click to highlight related messages)
- Content truncated at 400 characters

**Command Input (bottom bar):**
- Target agent dropdown (populated from online agents)
- Text input with Enter-to-send
- Send button (disabled when no target or empty text)
- Shows success/error toast notification

### Flow (2)

Force-directed graph visualizing agent communication topology.

- **Time range**: 1h, 6h, 24h, 7d buttons
- **Nodes**: agents (colored by hash), central "NATS" hub (indigo)
- **Edges**: message flows with directional arrows
- **Scaling**: node size by message volume, edge width by message count
- **Interaction**: click a node to see connection count and message volume
- Auto-refreshes on new realtime messages (debounced)

### Logs (3)

System and agent log viewer.

**Filters:**
- **Level buttons**: INFO, WARN, ERROR, DEBUG, NATS (toggle to filter)
- **Source**: filter by log source
- **Search**: full-text search

**Desktop view**: Sticky-header table with columns: Level, Time, Agent, Source, Message. Click a row to expand metadata JSON.

**Mobile view**: Card layout with level badge, time, agent, and message.

- ERROR rows highlighted in red
- WARN rows highlighted in yellow
- Monospace font
- Auto-refreshes every 5 seconds (200 log limit)

### Tasks (4)

Kanban board for task management.

**Columns** (5): Pending (gray), Assigned (blue), Running (yellow), Completed (green), Failed (red)

**Create Task** button opens a modal:
- Title, description, assigned agent (dropdown), priority (low/normal/high/critical)

**Task Card:**
- Title, description (2-line clamp)
- Assigned agent badge (colored)
- Priority indicator (left border color)
- Created timestamp
- Click to open detail modal

**Task Detail Modal:**
- Full title, status, priority, assigned agent
- Timestamps: created, started, completed
- Result JSON (if completed)
- Error message (if failed)
- **Message Trace**: all messages linked by correlation_id = task_id

## Header Bar

- **Connection badge**: green "Connected" or red "Disconnected"
- **System stats** (desktop): agents online, total messages, active tasks, errors today
- **Test data toggle**: show/hide test agent data (flask icon)
- **Dark/Light mode** toggle

## Agent Sidebar

- Lists all agents sorted: online first, then offline
- Agent count badge
- Click agent to filter all views to that agent
- Click again to deselect
- **Agent detail view** (click avatar/icon):
  - Profile: name, status, role, device type, model, IP, last seen
  - Capabilities pills
  - CPU and memory progress bars
  - Stats: message count, task count
  - Send command input
  - Recent messages list
  - Task history with status badges

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `1` | Switch to Chat tab |
| `2` | Switch to Flow tab |
| `3` | Switch to Logs tab |
| `4` | Switch to Tasks tab |
| `Enter` | Send command (when input focused) |

Shortcuts are disabled when focus is in an input, textarea, or select element.

## Real-time Updates

The dashboard connects to `/ws/stream` via WebSocket:

- **message**: new agent message → appears in chat
- **agent_registered**: new agent → toast notification, sidebar update
- **agent_status_change**: online/offline → sidebar badge update
- **task_update**: task status change → kanban board update
- **log** (ERROR level): error toast notification

Auto-reconnects with exponential backoff (max 30s). Pings every 15s for keepalive.

## Toast Notifications

- Position: top-right corner
- Duration: 4 seconds (6 for errors)
- Types: success (green), error (red), warning (yellow), info (default)
- Triggered by: agent registration, command sent, errors

## Test Data Toggle

The flask icon in the header toggles visibility of test agents (created by E2E tests). When disabled, the frontend API interceptor adds `exclude_test: true` to all requests, and the backend filters out agents/messages/logs matching test patterns.
