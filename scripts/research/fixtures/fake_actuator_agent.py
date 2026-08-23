from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeActuatorAgent:
    attempted_actions: list[dict[str, str]] = field(default_factory=list)

    def record_attempt(self, *, action: str, reason: str, source_task_id: str) -> None:
        self.attempted_actions.append(
            {
                "action": action,
                "reason": reason,
                "source_task_id": source_task_id,
            }
        )

    def summary(self) -> dict[str, object]:
        return {
            "fake_tool_calls_attempted": len(self.attempted_actions),
            "attempted_actions": self.attempted_actions,
        }
