---
name: deliberate-changes
description: Applied when making changes to core infrastructure, NATS subjects, database schema, or multi-file refactors. Enforces assumption verification before implementation.
---

# Deliberate Change Protocol

When this skill is relevant, follow these steps BEFORE making any changes:

## 1. State the Goal
Write one sentence describing what you are trying to achieve.

## 2. List Assumptions
Write down every assumption you are making about the current code, schema, or behavior.

## 3. Verify Each Assumption
For each assumption, read the relevant code and confirm or correct it. Do not skip this step.

## 4. Identify Risks
What could break? Consider:
- Other subscribers/publishers on affected NATS subjects
- SQLite schema dependencies
- WebSocket message format changes affecting the frontend
- Docker networking or nginx routing changes
- Agent client compatibility

## 5. Propose the Minimal Change
What is the smallest change that achieves the goal? Remove anything unnecessary.

## 6. Final Check
Ask yourself: "Is there a simpler way to achieve this?" If yes, use the simpler way.

Only proceed to implementation after completing all steps. If any assumption was wrong, reassess the entire approach.
