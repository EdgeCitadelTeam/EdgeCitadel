# Task Management

## Overview

EdgeCitadel includes a Kanban-style task board for tracking work assigned to agents. Tasks can be created manually from the dashboard or auto-created when commands with correlation IDs flow through the system.

## Task Lifecycle

```
Pending → Assigned → Running → Completed
                          └───→ Failed
```

## Creating Tasks

### From the Dashboard UI

1. Click the **Tasks** tab (or press `4`)
2. Click **+ Create Task**
3. Fill in:
   - **Title**: short description
   - **Description**: detailed instructions
   - **Assigned Agent**: select from dropdown
   - **Priority**: Low, Normal, High, or Critical
4. Click **Create**

The aggregator publishes a `tasks.{id}.assign` message to NATS, which the assigned agent receives.

### Via API

```bash
curl -X POST http://localhost/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Check temperature sensors",
    "description": "Read all sensors and report anomalies",
    "assigned_agent": "jeeves",
    "priority": "high"
  }'
```

### Auto-Created from Commands

When a command with a `correlation_id` is sent to an agent, the aggregator automatically creates a task:

- Dashboard sends command → task created (status: running)
- Agent replies with result → task auto-completed
- No manual task management needed for simple command-reply flows

## Task Board (Kanban)

Five columns with color coding:

| Column | Color | Description |
|---|---|---|
| Pending | Gray | Created but not yet assigned |
| Assigned | Blue | Assigned to an agent |
| Running | Yellow | Agent is actively working |
| Completed | Green | Finished with result |
| Failed | Red | Failed with error |

### Task Card

Each card shows:
- Title (truncated)
- Description (2-line clamp)
- Assigned agent badge (colored)
- Priority border: critical=red, high=orange, normal=blue, low=gray
- Created timestamp (relative)

### Task Detail Modal

Click a task card to see:
- Full title, status, priority, assigned agent
- Created, started, completed timestamps
- Result JSON (if completed)
- Error message (if failed)
- **Message Trace**: all messages linked by `correlation_id = task_id`

## Task Lifecycle via NATS

Agents can update task status by publishing to task subjects:

```bash
# Assign
agents/{name}/inbox → tasks.{id}.assign

# Progress
tasks.{id}.progress → { progress: 50, message: "Halfway done" }

# Complete
tasks.{id}.complete → { result: { ... } }

# Failed
tasks.{id}.failed → { error: "Sensor offline" }
```

## Updating Tasks via API

```bash
# Update status to running
curl -X PATCH http://localhost/api/tasks/{id} \
  -H "Content-Type: application/json" \
  -d '{"status": "running"}'

# Complete with result
curl -X PATCH http://localhost/api/tasks/{id} \
  -H "Content-Type: application/json" \
  -d '{"status": "completed", "result": "All sensors nominal"}'

# Mark as failed
curl -X PATCH http://localhost/api/tasks/{id} \
  -H "Content-Type: application/json" \
  -d '{"status": "failed", "error_message": "Sensor timeout"}'
```

Timestamps (`started_at`, `completed_at`) are auto-set based on status transitions.

## Task Trace

View all messages related to a task:

```bash
curl http://localhost/api/tasks/{id}/trace

# Returns all messages where correlation_id matches the task_id
```

This is visualized in the task detail modal as a message timeline.

## Querying Tasks

```bash
# All tasks
curl http://localhost/api/tasks

# Filter by agent
curl http://localhost/api/tasks?agent=jeeves

# Filter by status
curl http://localhost/api/tasks?status=running

# Exclude test data
curl http://localhost/api/tasks?exclude_test=true
```
