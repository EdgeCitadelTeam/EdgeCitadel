"""One provisioned source layout for all production database handles."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import platform

from .store import AgentdStore
from .trace_quota import TraceQuota, TraceQuotaError, verify_trace_quota


def verify_storage(trace_directory: Path, state_directory: Path) -> TraceQuota | dict:
    if platform.system() == "Darwin":
        from .storage_macos import verify_macos_storage

        return verify_macos_storage(trace_directory, state_directory)
    return verify_trace_quota(trace_directory, state_directory)


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

    def mount(self) -> None:
        if platform.system() == "Darwin":
            from .storage_macos import mount_macos_storage

            mount_macos_storage(self.state_directory)

    def verify(self) -> TraceQuota | dict:
        old_path = self.state_directory / "agentd.sqlite3"
        if old_path.exists() or old_path.is_symlink():
            raise TraceQuotaError(
                "source storage requires offline migration into the provisioned trace volume"
            )
        return verify_storage(self.trace_directory, self.state_directory)

    def open(self) -> AgentdStore:
        """Every daemon/sync handle rechecks native enforcement before opening."""
        self.verify()
        store = AgentdStore(
            self.trace_path, task_path=self.task_path, payload_key_path=self.key_path
        )
        store.storage_status = self.status
        return store

    def status(self) -> dict:
        value = self.verify()
        if isinstance(value, dict):
            return value
        if value is None:  # Explicit component-test verifier.
            return {"backend": "fixture", "mount_verified": False}
        return {
            "backend": "linux_user_quota",
            "mount_verified": True,
            "trust_boundary": "dedicated_uid",
            "physical_limit_bytes": value.hard_bytes,
            "admission_limit_bytes": value.hard_bytes,
            "inode_limit": value.hard_inodes,
            "migration_status": "complete",
        }
