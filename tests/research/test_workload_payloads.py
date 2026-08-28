from scripts.research.benchmark_core import (
    cancel_envelope,
    command_envelope,
    delegation_envelope,
)
from scripts.research.run_agent_benchmark import build_parser, parse_workloads


def test_benchmark_default_output_uses_runtime_data_directory():
    arguments = build_parser().parse_args([])

    assert arguments.out_dir == "data/research/results"


def test_parse_workloads_expands_native_slice():
    assert parse_workloads("native") == ["E1", "E4", "E5", "E7"]


def test_parse_workloads_accepts_csv():
    assert parse_workloads("E1,E7,E12") == ["E1", "E7", "E12"]


def test_command_envelope_uses_required_fields():
    env = command_envelope(
        sender_id="bench-runner",
        recipient_id="shell-1",
        body="printf hi",
    )
    assert env["v"] == 1
    assert env["type"] == "command"
    assert env["sender_id"] == "bench-runner"
    assert env["recipient_id"] == "shell-1"
    assert env["payload"]["body"] == "printf hi"
    assert "task_id" in env


def test_delegation_envelope_includes_context_and_hop_count():
    env = delegation_envelope(
        sender_id="bench-delegator",
        recipient_id="bench-worker",
        body="child work",
        context_id="00000000-0000-4000-8000-000000000001",
        hop_count=1,
    )
    assert env["type"] == "delegation"
    assert env["context_id"] == "00000000-0000-4000-8000-000000000001"
    assert env["hop_count"] == 1


def test_cancel_envelope_targets_existing_task():
    env = cancel_envelope(
        sender_id="bench-runner",
        recipient_id="bench-cancel",
        task_id="00000000-0000-4000-8000-000000000002",
    )
    assert env["type"] == "cancel"
    assert env["task_id"] == "00000000-0000-4000-8000-000000000002"
