"""One provisioned source layout for all production database handles."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .store import AgentdStore
from .trace_quota import TraceQuota, TraceQuotaError, verify_trace_quota


@dataclass(frozen=True)
class StorageLayout:
    state_directory: Path

    @property
    def trace_directory(self) -> Path:
        return self.state_directory / "trace"

    @property
    def trace_path(self) -> Path:
        return self.trace_directory / "agentd.sqlite3"

    @property
    def task_path(self) -> Path:
        return self.state_directory / "agentd-tasks.sqlite3"

    @property
    def key_path(self) -> Path:
        return self.state_directory / "payload.key"

    def verify(self) -> TraceQuota:
        old_path = self.state_directory / "agentd.sqlite3"
        if old_path.exists() or old_path.is_symlink():
            raise TraceQuotaError(
                "source storage requires offline migration into the provisioned trace volume"
            )
        return verify_trace_quota(self.trace_directory, self.state_directory)

    def open(self) -> AgentdStore:
        """Every daemon/sync handle rechecks native enforcement before opening."""
        self.verify()
        return AgentdStore(
            self.trace_path, task_path=self.task_path, payload_key_path=self.key_path
        )
