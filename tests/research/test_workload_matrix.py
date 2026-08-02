"""Fixed paper-matrix contract tests."""

from __future__ import annotations

from collections.abc import Mapping
from time import perf_counter_ns

import pytest

from adapters._common.task_types import PublicationReceipt
from scripts.research.modes.base import ObservedEnvelope
from scripts.research.workload_matrix import (
    CrashPoint,
    MatrixCell,
    required_matrix_cells,
    run_cell,
    workload_timeout_seconds,
)


def test_required_matrix_has_exactly_the_predeclared_primary_and_ablation_cells() -> (
    None
):
    cells = required_matrix_cells()
    primary = [cell for cell in cells if cell.variant == "primary"]
    ablations = [cell for cell in cells if cell.variant == "ablation"]

    assert len(cells) == 46
    assert {cell.workload for cell in cells} == {
        "W1",
        "W2",
        "W3",
        "W4",
        "W5",
        "W6a",
        "W6b",
        "W6c",
        "W7",
        "W8",
    }
    assert {cell.mode for cell in cells} == {
        "central-relay",
        "core-only",
        "edgecitadel",
        "all-durable",
    }
    assert len(primary) == 40
    assert {(cell.workload, cell.mode, cell.ablation) for cell in primary} == {
        (workload, mode, "full-contract")
        for workload in (
            "W1",
            "W2",
            "W3",
            "W4",
            "W5",
            "W6a",
            "W6b",
            "W6c",
            "W7",
            "W8",
        )
        for mode in (
            "central-relay",
            "core-only",
            "edgecitadel",
            "all-durable",
        )
    }
    assert {(cell.workload, cell.mode, cell.ablation) for cell in ablations} == {
        (workload, "edgecitadel", ablation)
        for workload in ("W6a", "W6b", "W8")
        for ablation in ("none", "broker-only")
    }
    assert {cell.timeout_seconds for cell in cells if cell.workload == "W6b"} == {330}
    assert {cell.timeout_seconds for cell in cells if cell.workload == "W7"} == {35}
    assert all(
        cell.timeout_seconds == 30
        for cell in cells
        if cell.workload not in {"W6b", "W7"}
    )
    assert workload_timeout_seconds("W6b") == 330
    assert workload_timeout_seconds("W7") == 35
    assert workload_timeout_seconds("W1") == 30
    with pytest.raises(ValueError, match="invalid workload"):
        workload_timeout_seconds("W9")


def test_crash_points_are_complete_and_have_stable_paper_labels() -> None:
    assert tuple(CrashPoint) == (
        CrashPoint.AFTER_RECEIVE,
        CrashPoint.AFTER_SIDE_EFFECT,
        CrashPoint.AFTER_PREPARE,
        CrashPoint.AFTER_PUBLISH,
        CrashPoint.AFTER_MARK,
        CrashPoint.DURING_EXCEPTION,
    )
    assert {point.value for point in CrashPoint} == {
        "after-receive-before-handler",
        "after-side-effect-before-ledger-prepare",
        "after-ledger-prepare-before-result-publish",
        "after-result-publish-before-publish-mark",
        "after-publish-mark-before-inbound-commit",
        "during-handler-exception-conversion",
    }


class _TerminalTransport:
    def __init__(self) -> None:
        self.submissions: list[dict[str, object]] = []
        self.observed_task_ids: list[str] = []
        self.terminal_observer_started = 0

    async def start_terminal_observer(self) -> None:
        self.terminal_observer_started += 1

    async def submit_task(self, envelope: dict[str, object]) -> PublicationReceipt:
        self.submissions.append(envelope)
        return PublicationReceipt(
            envelope_id=str(envelope["id"]),
            accepted=True,
            transport="edgecitadel",
            stream="AGENT_INBOX",
            stream_sequence=1,
            duplicate=False,
            accepted_ns=perf_counter_ns(),
            application_bytes=256,
            wire_bytes=512,
        )

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        self.observed_task_ids.append(task_id)
        request = self.submissions[0]
        nonce = request["payload"]["body"]
        return ObservedEnvelope(
            envelope={
                "id": "10000000-0000-4000-8000-000000000001",
                "task_id": task_id,
                "payload": {"body": f"edgecitadel:{nonce}"},
            },
            observed_ns=perf_counter_ns(),
            observation_index=1,
            stream_sequence=2,
            delivery_count=1,
            replayed=False,
            delivery=None,
        )

    async def inspect_state(self):
        return {"transport": "captured"}


class _Faults:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def disconnect_progress_observer(self) -> None:
        self._calls.append("disconnect")

    async def reconnect_progress_observer(self) -> None:
        self._calls.append("reconnect")

    async def stop_worker(self, agent_id: str) -> None:
        self._calls.append(f"stop:{agent_id}")

    async def start_worker(self, agent_id: str) -> None:
        self._calls.append(f"start:{agent_id}")

    async def restart_coordinator(self) -> None:
        self._calls.append("restart-coordinator")


class _ProgressTransport(_TerminalTransport):
    def __init__(self) -> None:
        super().__init__()
        self.progress_observer_started = 0
        self.fault_calls: list[str] = []
        self.faults = _Faults(self.fault_calls)

    async def start_progress_observer(self) -> None:
        self.progress_observer_started += 1


class _ProgressObserver:
    def __init__(self) -> None:
        self.waits: list[int] = []

    async def wait_for_generated(self, count: int) -> None:
        self.waits.append(count)

    def progress_counts(self) -> dict[str, int]:
        return {"generated": 20, "live": 10, "replayed": 0, "missing": 10}


class _DelegationObserver:
    child_task_id = "20000000-0000-4000-8000-000000000001"

    def __init__(self) -> None:
        self.parents: list[str] = []

    async def wait_for_child(self, parent_task_id: str) -> dict[str, object]:
        self.parents.append(parent_task_id)
        return {
            "task_id": self.child_task_id,
            "context_id": parent_task_id,
            "hop_count": 1,
            "parent_task_id": parent_task_id,
        }


class _DelegationTransport(_TerminalTransport):
    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        self.observed_task_ids.append(task_id)
        request = self.submissions[0]
        nonce = request["payload"]["body"]
        return ObservedEnvelope(
            envelope={
                "id": "30000000-0000-4000-8000-000000000001",
                "task_id": task_id,
                "payload": {
                    "body": f"edgecitadel:{nonce}",
                    "parent_task_id": request["task_id"],
                },
            },
            observed_ns=perf_counter_ns(),
            observation_index=1,
            stream_sequence=2,
            delivery_count=1,
            replayed=False,
            delivery=None,
        )


class _OfflineTransport(_TerminalTransport):
    def __init__(self, *, terminal: bool) -> None:
        super().__init__()
        self.fault_calls: list[str] = []
        self.faults = _Faults(self.fault_calls)
        self._terminal = terminal

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        if not self._terminal:
            self.observed_task_ids.append(task_id)
            return None
        return await super().observe_terminal(task_id, timeout_s)


class _WireRetryTransport(_TerminalTransport):
    async def submit_task(self, envelope: dict[str, object]) -> PublicationReceipt:
        self.submissions.append(envelope)
        return PublicationReceipt(
            envelope_id=str(envelope["id"]),
            accepted=True,
            transport="edgecitadel",
            stream="AGENT_INBOX",
            stream_sequence=len(self.submissions),
            duplicate=len(self.submissions) == 2,
            accepted_ns=perf_counter_ns(),
            application_bytes=256,
            wire_bytes=512,
        )


class _CollisionObserver:
    def __init__(self) -> None:
        self.task_ids: list[str] = []

    async def wait_for_collisions(self, task_id: str) -> dict[str, int]:
        self.task_ids.append(task_id)
        return {"rejections": 2, "executions": 0, "cached_output_exposure": 0}


class _SemanticRetryObserver:
    def __init__(self) -> None:
        self.task_ids: list[str] = []

    async def wait_for_retry_window(self, task_id: str) -> dict[str, int]:
        self.task_ids.append(task_id)
        return {
            "broker_duplicate_window_seconds": 300,
            "retry_elapsed_seconds": 301,
            "ledger_retention_seconds": 3600,
        }


class _CrashObserver:
    def __init__(self, *, core_only: bool = False) -> None:
        self.points: list[CrashPoint] = []
        self.core_only = core_only

    async def run_crash_subtrial(
        self,
        point: CrashPoint,
        envelope: dict[str, object],
        timeout_s: float,
    ) -> dict[str, object]:
        self.points.append(point)
        assert timeout_s == 30.0
        assert envelope["type"] == "command"
        return {
            "applicability": (
                "transport-inapplicable"
                if self.core_only and point is CrashPoint.AFTER_MARK
                else "applicable"
            ),
            "accepted": 1,
            "delivered": 1,
            "executions": 1,
            "side_effects": 1,
            "logical_terminals": 1,
            "distinct_terminal_ids": 1,
            "publication_attempts": 1,
            "wire_deliveries": 1,
            "poison": 0,
            "timed_out": False,
        }


class _ActuatorObserver:
    def __init__(self) -> None:
        self.task_ids: list[str] = []
        self.submission_ids: list[str] = []

    async def record_submission(self, envelope: Mapping[str, object]) -> None:
        envelope_id = envelope.get("id")
        assert isinstance(envelope_id, str)
        self.submission_ids.append(envelope_id)

    async def wait_for_actuator_outcome(self, task_id: str) -> dict[str, object]:
        self.task_ids.append(task_id)
        return {
            "handler_attempts": 2,
            "delivered": 1,
            "side_effects": 2,
            "prepared_outcomes": 1,
            "logical_terminals": 1,
            "distinct_terminal_ids": 1,
            "publication_attempts": 2,
            "wire_deliveries": 1,
            "poison": 0,
            "timed_out": False,
            "crash_point": CrashPoint.AFTER_SIDE_EFFECT.value,
        }


@pytest.mark.asyncio
async def test_w1_submits_once_and_records_one_matching_terminal_without_invented_fixture_counts() -> (
    None
):
    transport = _TerminalTransport()
    cell = MatrixCell("W1", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        (),
        None,
    )

    assert transport.terminal_observer_started == 1
    assert len(transport.submissions) == 1
    assert transport.observed_task_ids == [transport.submissions[0]["task_id"]]
    assert observation.initiated == 1
    assert observation.accepted == 1
    assert observation.logical_terminals == 1
    assert observation.distinct_terminal_ids == 1
    assert observation.executions is None
    assert observation.side_effects is None
    assert observation.timed_out is False
    assert observation.final_transport == {"transport": "captured"}


@pytest.mark.asyncio
async def test_w3_records_external_progress_counts_across_the_fixed_disconnect_schedule() -> (
    None
):
    transport = _ProgressTransport()
    observer = _ProgressObserver()
    cell = MatrixCell("W3", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"progress": observer},
        None,
    )

    assert transport.progress_observer_started == 1
    assert observer.waits == [5, 15, 20]
    assert transport.fault_calls == ["disconnect", "reconnect"]
    assert observation.progress_generated == 20
    assert observation.progress_live_delivered == 10
    assert observation.progress_replay_delivered == 0
    assert observation.progress_missing == 10
    assert observation.logical_terminals == 1


@pytest.mark.asyncio
async def test_w2_requires_child_identity_context_hop_and_parent_terminal_linkage() -> (
    None
):
    transport = _DelegationTransport()
    observer = _DelegationObserver()
    cell = MatrixCell("W2", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"delegation": observer},
        None,
    )

    parent_task_id = transport.submissions[0]["task_id"]
    assert observer.parents == [parent_task_id]
    assert transport.observed_task_ids == [observer.child_task_id]
    assert observation.initiated == 1
    assert observation.accepted == 1
    assert observation.delivered == 1
    assert observation.logical_terminals == 1


@pytest.mark.asyncio
async def test_w4_faults_only_the_transport_worker_and_records_accepted_core_loss() -> (
    None
):
    transport = _OfflineTransport(terminal=False)
    cell = MatrixCell("W4", "core-only", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        (),
        None,
    )

    assert transport.fault_calls == ["stop:worker-1", "start:worker-1"]
    assert len(transport.submissions) == 1
    assert observation.initiated == 1
    assert observation.accepted == 1
    assert observation.logical_terminals == 0
    assert observation.timed_out is True


@pytest.mark.asyncio
async def test_w6a_reuses_the_same_serialized_envelope_and_wire_identity() -> None:
    transport = _WireRetryTransport()
    cell = MatrixCell("W6a", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        (),
        None,
    )

    assert len(transport.submissions) == 2
    assert transport.submissions[0] is transport.submissions[1]
    assert transport.submissions[0]["id"] == transport.submissions[1]["id"]
    assert transport.submissions[0]["task_id"] == transport.submissions[1]["task_id"]
    assert observation.initiated == 1
    assert observation.accepted == 2
    assert observation.publication_attempts == 2
    assert observation.logical_terminals == 1
    assert observation.workload_evidence["wire_retry"]["envelope_ids"] == [
        transport.submissions[0]["id"],
        transport.submissions[1]["id"],
    ]


@pytest.mark.asyncio
async def test_w6b_uses_a_new_wire_id_with_the_same_logical_request() -> None:
    transport = _WireRetryTransport()
    observer = _SemanticRetryObserver()
    cell = MatrixCell("W6b", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"semantic_retry": observer},
        None,
    )

    assert len(transport.submissions) == 2
    assert transport.submissions[0] is not transport.submissions[1]
    assert transport.submissions[0]["id"] != transport.submissions[1]["id"]
    assert transport.submissions[0]["task_id"] == transport.submissions[1]["task_id"]
    assert transport.submissions[0]["payload"] == transport.submissions[1]["payload"]
    assert observer.task_ids == [transport.submissions[0]["task_id"]]
    assert observation.initiated == 1
    assert observation.accepted == 2
    assert observation.publication_attempts == 2
    assert observation.workload_evidence["semantic_retry"]["task_id"] == (
        transport.submissions[0]["task_id"]
    )


@pytest.mark.asyncio
async def test_w7_restarts_the_transport_coordinator_only_after_acceptance() -> None:
    transport = _OfflineTransport(terminal=False)
    cell = MatrixCell("W7", "core-only", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        (),
        None,
    )

    assert transport.fault_calls == [
        "stop:worker-1",
        "restart-coordinator",
        "start:worker-1",
    ]
    assert observation.accepted == 1
    assert observation.logical_terminals == 0
    assert observation.timed_out is True


@pytest.mark.asyncio
async def test_w6c_submits_sender_and_payload_collisions_without_cached_output_exposure() -> (
    None
):
    transport = _WireRetryTransport()
    observer = _CollisionObserver()
    cell = MatrixCell("W6c", "edgecitadel", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"collision": observer},
        None,
    )

    assert len(transport.submissions) == 3
    first, sender_mutation, payload_mutation = transport.submissions
    assert sender_mutation["id"] != first["id"]
    assert sender_mutation["task_id"] == first["task_id"]
    assert sender_mutation["sender_id"] != first["sender_id"]
    assert payload_mutation["id"] != first["id"]
    assert payload_mutation["task_id"] == first["task_id"]
    assert payload_mutation["payload"] != first["payload"]
    assert observer.task_ids == [first["task_id"]]
    assert observation.executions == 0
    assert observation.logical_terminals == 0
    assert observation.timed_out is True
    assert observation.workload_evidence["collision"] == {
        "rejections": 2,
        "executions": 0,
        "cached_output_exposure": 0,
    }


@pytest.mark.asyncio
async def test_w5_runs_every_crash_boundary_and_preserves_core_only_inapplicability() -> (
    None
):
    transport = _TerminalTransport()
    observer = _CrashObserver(core_only=True)
    cell = MatrixCell("W5", "core-only", "primary", "full-contract", 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"crash": observer},
        None,
    )

    assert observer.points == list(CrashPoint)
    assert observation.initiated == len(CrashPoint)
    assert observation.executions == len(CrashPoint)
    assert observation.side_effects == len(CrashPoint)
    assert observation.logical_terminals == len(CrashPoint)
    assert observation.inapplicable_crash_points == (CrashPoint.AFTER_MARK.value,)
    assert len(observation.workload_evidence["crash_subtrials"]) == len(CrashPoint)


@pytest.mark.asyncio
@pytest.mark.parametrize("ablation", ("full-contract", "none", "broker-only"))
async def test_w8_records_actuator_attempts_effects_prepared_outcomes_and_terminals(
    ablation: str,
) -> None:
    transport = _TerminalTransport()
    observer = _ActuatorObserver()
    cell = MatrixCell("W8", "edgecitadel", "primary", ablation, 30)

    observation = await run_cell(
        cell,
        transport,
        {"sender_id": "requester-1", "worker_id": "worker-1"},
        {"actuator": observer},
        None,
    )

    assert observer.task_ids == [transport.submissions[0]["task_id"]]
    assert observer.submission_ids == [transport.submissions[0]["id"]]
    assert observation.handler_attempts == 2
    assert observation.executions == 2
    assert observation.side_effects == 2
    assert observation.prepared_outcomes == 1
    assert observation.logical_terminals == 1
    assert observation.timed_out is False
