"""The A2A baseline's operator: the protocol's own controls, answered to one alert.

The alert names the faulty agent and the bad artifact (the given root mode, as
every EdgeCitadel strategy gets). A2A carries no lineage, so the operator can:

- ``a2a_cancel_agent``: ``ListTasks`` on the faulty agent's server and
  ``CancelTask`` on every queued or running task, repeated every
  ``SWEEP_S`` until the run ends (the agent is taken out of service);
- ``a2a_cancel_all``: the same on every agent server;
- ``a2a_block_input``: tell every server to reject messages carrying the bad
  artifact (the one operator call outside the protocol; see ``a2a_agent``).

Every action is logged in the monitor's decision-log schema
(``core/containment-decisions.jsonl``), so ``metrics.decision_metrics`` scores
it with the same code: the first sweep is the ``decision``, later sweeps that
act are ``resolve`` records; an action is confirmed when its reply arrives with
the task cancelled (a task that finished between list and cancel is ``noop``:
nothing was left to stop).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from a2a.types import a2a_pb2 as pb
from a2a.utils.errors import A2AError, TaskNotCancelableError

from .a2a_common import LoopThread, Peers, now_ms
from .names import A2A_CODES

log = logging.getLogger(__name__)
NATIVE_STRATEGIES = A2A_CODES
SWEEP_S = 0.1
CONFIRMED = "confirmed"


class NativeController:
    def __init__(
        self,
        loop: LoopThread,
        peers: Peers,
        *,
        strategy: str | None,
        agent_nodes: Mapping[str, str],
        faulty_agent: str | None,
        bad_item: str | None,
        decisions_path: Path,
        operator_token: str,
        incident_id: str,
    ) -> None:
        if strategy is not None and strategy not in NATIVE_STRATEGIES:
            raise ValueError(f"not an A2A-native strategy: {strategy}")
        self.loop = loop
        self.peers = peers
        self.strategy = strategy
        self.agent_nodes = dict(agent_nodes)
        self.faulty_agent = faulty_agent
        self.bad_item = bad_item
        self.decisions_path = decisions_path
        self.operator_token = operator_token
        self.incident_id = incident_id
        self._stop = asyncio.Event()
        self._response: Any = None
        self._decided = False
        decisions_path.parent.mkdir(parents=True, exist_ok=True)
        decisions_path.touch()

    # -- the runner's view ---------------------------------------------------------
    def alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        t_received = now_ms()
        if self.strategy is None:
            return {"accepted": False, "reason": "no containment in this run"}
        self._response = self.loop.submit(self._respond(dict(alert), t_received))
        return {"accepted": True, "strategy": self.strategy, "t_received": t_received}

    def status(self) -> dict[str, Any]:
        mechanisms = {
            "native_a2a": True,
            "lineage": False,
            "persistent": self.strategy != "a2a_block_input",
        }
        return {
            "strategy": self.strategy,
            "mechanisms": {self.strategy: mechanisms} if self.strategy else {},
            "last_decision": self._decided,
            # No phase 1 here: the runner records the monitor's phase1_mode for
            # every strategy run (since 33b4309), and native controls have none.
            "phase1_mode": None,
        }

    def stop(self) -> None:
        self.loop.loop.call_soon_threadsafe(self._stop.set)
        if self._response is not None:
            try:
                self._response.result(10)
            except Exception as error:  # noqa: BLE001 - the log has what ran
                log.warning("native controller ended with %r", error)

    # -- responses -----------------------------------------------------------------
    async def _respond(self, alert: dict[str, Any], t_received: int) -> None:
        subject = {k: alert[k] for k in ("attempt_id", "task_id") if k in alert}
        header = {
            "type": "decision",
            "incident_id": self.incident_id,
            "alert": alert,
            "t_received": t_received,
            "strategy": self.strategy,
            "root_basis": "given",
            "roots": [subject.get("attempt_id") or subject.get("task_id")],
            "ranking": [],
            "status": "applied",
        }
        if self.strategy == "a2a_block_input":
            actions = await self._block_everywhere()
            self._write({**header, "phase1": actions, "phase2": [], "released": []})
            self._decided = True
            return
        targets = (
            [self.faulty_agent]
            if self.strategy == "a2a_cancel_agent"
            else sorted(self.agent_nodes)
        )
        first = True
        while True:
            actions = await self._sweep([t for t in targets if t])
            if first:
                self._write({**header, "phase1": actions, "phase2": [], "released": []})
                self._decided, first = True, False
            elif actions:
                self._write(
                    {
                        "type": "resolve",
                        "incident_id": self.incident_id,
                        "phase1": actions,
                        "phase2": [],
                        "released": [],
                    }
                )
            try:
                await asyncio.wait_for(self._stop.wait(), SWEEP_S)
                return
            except TimeoutError:
                continue

    async def _sweep(self, agents: list[str]) -> list[dict[str, Any]]:
        listed = await asyncio.gather(
            *(self.peers.live_tasks(a) for a in agents), return_exceptions=True
        )
        work = []
        for agent, tasks in zip(agents, listed, strict=True):
            if isinstance(tasks, BaseException):
                continue  # the server is gone (run ending); nothing to cancel there
            work += [(agent, task.id) for task in tasks]
        return list(await asyncio.gather(*(self._cancel(a, t) for a, t in work)))

    async def _cancel(self, agent: str, task_id: str) -> dict[str, Any]:
        record = self._record("cancel", "task", task_id, agent)
        record["t_sent"] = now_ms()
        try:
            task = await self.peers.cancel(agent, task_id)
            record["t_confirmed"] = now_ms()
            cancelled = task.status.state == pb.TaskState.TASK_STATE_CANCELED
            record["state"] = CONFIRMED if cancelled else "noop"
        except TaskNotCancelableError:
            record["t_confirmed"], record["state"] = now_ms(), "noop"
        except A2AError as error:
            record["state"], record["error"] = "failed", f"{type(error).__name__}"
        record["t_applied"] = record.get("t_confirmed")
        return record

    async def _block_everywhere(self) -> list[dict[str, Any]]:
        async def block(agent: str) -> dict[str, Any]:
            record = self._record(
                "block", "artifact", f"{agent}/{self.bad_item}", agent
            )
            record["t_sent"] = now_ms()
            try:
                reply = await self.peers.post(
                    self.peers.urls[agent].rstrip("/") + "/operator/block",
                    {"item": self.bad_item},
                    {"x-operator-token": self.operator_token},
                )
                ok = reply.status_code == 200
                record["state"] = CONFIRMED if ok else "failed"
                if ok:
                    record["t_confirmed"] = now_ms()
                else:
                    record["error"] = f"http:{reply.status_code}"
            except Exception as error:  # noqa: BLE001 - recorded as a failed action
                record["state"], record["error"] = "failed", type(error).__name__
            record["t_applied"] = record.get("t_confirmed")
            return record

        if not self.bad_item:
            return []
        return list(await asyncio.gather(*(block(a) for a in sorted(self.agent_nodes))))

    def _record(self, op: str, kind: str, object_id: str, agent: str) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "phase": 1,
            "action": {
                "op": op,
                "kind": kind,
                "object_id": object_id,
                "node_id": self.agent_nodes[agent],
                "agent_id": agent,
            },
            "state": "planned",
            "t_decision": now_ms(),
            "t_sent": None,
            "t_confirmed": None,
        }

    def _write(self, record: Mapping[str, Any]) -> None:
        with self.decisions_path.open("a") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
