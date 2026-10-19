import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from edgecitadel_agentd import trace_quota as quota


@pytest.fixture
def qualified(tmp_path, monkeypatch):
    trace, task = tmp_path / "trace", tmp_path / "task"
    trace.mkdir(mode=0o700)
    task.mkdir(mode=0o700)
    original = os.fstat
    task_inode = task.stat().st_ino

    def metadata(fd):
        result = original(fd)
        if result.st_ino == task_inode:
            return SimpleNamespace(
                st_uid=result.st_uid, st_mode=result.st_mode, st_dev=result.st_dev + 1
            )
        return result

    state = quota._QuotaState(version=1, flags=3)
    usage = quota._Quota(
        hard=256 * 1024, inode_hard=128, space=4096, inodes=1, valid=15
    )
    monkeypatch.setattr(quota, "_writer_uid", os.geteuid)
    monkeypatch.setattr(quota, "_filesystem_type", lambda fd: "ext4")
    monkeypatch.setattr(quota, "_read_quota", lambda fd, uid: (state, usage))
    monkeypatch.setattr(os, "fstat", metadata)
    return trace, task, state, usage


def test_qualified_usage_and_regular_files(qualified):
    trace, task, _, _ = qualified
    (trace / "agentd.sqlite3").write_bytes(b"owned")
    value = quota.verify_trace_quota(trace, task)
    assert value.uid == os.geteuid()
    assert value.hard_bytes == 256 * 1024 * 1024
    assert value.allocated_bytes == 4096


@pytest.mark.parametrize(
    "change,reason",
    [
        (lambda state, usage: setattr(state, "flags", 1), "both be active"),
        (lambda state, usage: setattr(usage, "hard", 0), "hard limits"),
        (lambda state, usage: setattr(usage, "hard", 512 * 1024), "hard limits"),
        (lambda state, usage: setattr(usage, "inode_hard", 0), "hard limits"),
        (lambda state, usage: setattr(usage, "valid", 1), "incomplete"),
        (
            lambda state, usage: setattr(usage, "space", 256 * 1024 * 1024 + 1),
            "already over",
        ),
    ],
)
def test_accounting_and_limits_never_substitute_for_enforcement(
    qualified, change, reason
):
    trace, task, state, usage = qualified
    change(state, usage)
    with pytest.raises(quota.TraceQuotaError, match=reason):
        quota.verify_trace_quota(trace, task)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "directory_link"])
def test_external_or_alias_entries_refuse(qualified, kind):
    trace, task, _, _ = qualified
    source = task / "outside"
    source.write_bytes(b"unowned by trace")
    target = trace / "alias"
    if kind == "hardlink":
        os.link(source, target)
    else:
        target.symlink_to(task if kind == "directory_link" else source)
    with pytest.raises(quota.TraceQuotaError, match="single-link"):
        quota.verify_trace_quota(trace, task)


def test_absent_directory_is_not_created(qualified):
    trace, task, _, _ = qualified
    missing = trace / "absent"
    with pytest.raises(quota.TraceQuotaError, match="could not be verified"):
        quota.verify_trace_quota(missing, task)
    assert not missing.exists()


def test_same_filesystem_and_public_directory_refuse(qualified):
    trace, task, _, _ = qualified
    with pytest.raises(quota.TraceQuotaError, match="separate filesystems"):
        quota.verify_trace_quota(trace, trace)
    trace.chmod(0o755)
    with pytest.raises(quota.TraceQuotaError, match="private"):
        quota.verify_trace_quota(trace, task)


def test_unsupported_filesystem_and_failed_kernel_query_refuse(qualified, monkeypatch):
    trace, task, _, _ = qualified
    monkeypatch.setattr(quota, "_filesystem_type", lambda fd: "tmpfs")
    with pytest.raises(quota.TraceQuotaError, match="qualified ext4"):
        quota.verify_trace_quota(trace, task)
    monkeypatch.setattr(quota, "_filesystem_type", lambda fd: "ext4")

    def unavailable(fd, uid):
        raise PermissionError("owned kernel refusal")

    monkeypatch.setattr(quota, "_read_quota", unavailable)
    with pytest.raises(quota.TraceQuotaError, match="could not be verified") as result:
        quota.verify_trace_quota(trace, task)
    assert isinstance(result.value.__cause__, PermissionError)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("Uid", "0 0 0 0", "non-root"),
        ("Uid", "1000 1000 0 1000", "non-root"),
        ("CapPrm", "01000000", "capabilities"),
        ("CapEff", "01000000", "capabilities"),
        ("CapInh", "01000000", "capabilities"),
        ("CapAmb", "01000000", "capabilities"),
        ("NoNewPrivs", "0", "no_new_privs"),
    ],
)
def test_privilege_refusal(monkeypatch, field, value, reason):
    fields = {
        "Uid": "1000 1000 1000 1000",
        "CapPrm": "0",
        "CapEff": "0",
        "CapInh": "0",
        "CapAmb": "0",
        "NoNewPrivs": "1",
    }
    fields[field] = value
    monkeypatch.setattr(quota.platform, "system", lambda: "Linux")
    monkeypatch.setattr(quota.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        Path,
        "read_text",
        lambda self: "\n".join(f"{k}: {v}" for k, v in fields.items()),
    )
    with pytest.raises(quota.TraceQuotaError, match=reason):
        quota._writer_uid()
