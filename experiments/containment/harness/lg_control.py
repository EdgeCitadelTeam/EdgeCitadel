"""The LangGraph baseline's operator: LangGraph Server's own controls, answered
to one alert (given root: the faulty task and the bad artifact).

- ``lg_cancel_run``: cancel the workflow's run (``action="interrupt"``): every
  running node stops, the checkpoints stay, nothing is resumed;
- ``lg_block_resume``: cancel the run, add the bad artifact to the state's
  ``blocked_items`` (``update_state``), and resume the thread: the supervisor
  rejects every request carrying it. When the cancel cut a worker step short,
  the edit is written as the supervisor, so that step runs again (LangGraph
  drops the finished nodes' writes of a step it re-plans); when it landed
  between steps, as a worker, so the supervisor runs next and nothing is sent
  twice;
- ``lg_time_travel``: cancel the run, fork from the latest supervisor
  checkpoint before the faulty task completed (paging back through the whole
  history), with the task in ``skip_tasks``, and run from the fork: everything
  after that checkpoint is executed again (its effects reach the devices
  again). With no such checkpoint the fork is recorded as failed.

Actions are logged in the monitor's decision-log schema
(``harness.native``); the first response is the decision.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from langgraph_sdk import get_client

from .names import LG_CODES
from .native import CONFIRMED, LoopThread, action_record, append_jsonl, now_ms

log = logging.getLogger(__name__)
ASSISTANT = "workload"
HISTORY_PAGE = 200
STEP_NODES = frozenset({"worker", "actuate"})


class LangGraphController:
    def __init__(
        self,
        loop: LoopThread,
        url: str,
        *,
        strategy: str | None,
        node: str,
        faulty_task: str | None,
        faulty_agent: str | None,
        bad_item: str | None,
        decisions_path: Path,
        incident_id: str,
        run_config: Mapping[str, Any],
    ) -> None:
        if strategy is not None and strategy not in LG_CODES:
            raise ValueError(f"not a LangGraph-native strategy: {strategy}")
        self.loop = loop
        self.url = url
        self.strategy = strategy
        self.node = node
        self.faulty_task = faulty_task
        self.faulty_agent = faulty_agent or "supervisor"
        self.bad_item = bad_item
        self.decisions_path = decisions_path
        self.incident_id = incident_id
        self.run_config = dict(run_config)
        self.thread_id: str | None = None
        self.run_id: str | None = None
        self._response: Any = None
        self._decided = False
        decisions_path.parent.mkdir(parents=True, exist_ok=True)
        decisions_path.touch()

    # -- the stack's view ------------------------------------------------------------
    async def start(self, requests: list[dict[str, Any]]) -> str:
        client = get_client(url=self.url)
        self.thread_id = (await client.threads.create())["thread_id"]
        run = await client.runs.create(
            self.thread_id,
            ASSISTANT,
            input={"requests": requests},
            config=self.run_config,
        )
        self.run_id = run["run_id"]
        return self.run_id

    async def running(self) -> bool:
        if self.thread_id is None or self.run_id is None:
            return False
        run = await get_client(url=self.url).runs.get(self.thread_id, self.run_id)
        return run["status"] in {"pending", "running"}

    def alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        t_received = now_ms()
        if self.strategy is None:
            return {"accepted": False, "reason": "no containment in this run"}
        self._response = self.loop.submit(self._respond(dict(alert), t_received))
        return {"accepted": True, "strategy": self.strategy, "t_received": t_received}

    def status(self) -> dict[str, Any]:
        mechanisms = {"native_langgraph": True, "lineage": False}
        return {
            "strategy": self.strategy,
            "mechanisms": {self.strategy: mechanisms} if self.strategy else {},
            "last_decision": self._decided,
            "thread_id": self.thread_id,
            "run_id": self.run_id,
            # No phase 1 here: the runner records the monitor's phase1_mode for
            # every strategy run (since 33b4309), and native controls have none.
            "phase1_mode": None,
        }

    def stop(self) -> None:
        if self._response is not None:
            try:
                self._response.result(30)
            except Exception as error:  # noqa: BLE001 - the log has what ran
                log.warning("LangGraph controller ended with %r", error)

    # -- responses -----------------------------------------------------------------
    async def _respond(self, alert: dict[str, Any], t_received: int) -> None:
        client = get_client(url=self.url)
        subject = alert.get("attempt_id") or alert.get("task_id")
        actions = [await self._cancel(client)]
        if self.strategy == "lg_block_resume" and self.bad_item:
            latest = await client.threads.get_state(self.thread_id)
            actions += await self._edit_and_resume(
                client,
                {"blocked_items": [self.bad_item]},
                latest,
                "block",
                self.bad_item,
            )
        elif self.strategy == "lg_time_travel" and self.faulty_task:
            fork = await self._fork_point(client)
            if fork is None:
                edit = self._record("fork", "state", self.faulty_task)
                edit["state"], edit["error"] = "failed", "no_fork_point"
                actions.append(edit)
            else:
                actions += await self._edit_and_resume(
                    client,
                    {"skip_tasks": [self.faulty_task]},
                    fork,
                    "fork",
                    self.faulty_task,
                )
        append_jsonl(
            self.decisions_path,
            {
                "type": "decision",
                "incident_id": self.incident_id,
                "alert": alert,
                "t_received": t_received,
                "strategy": self.strategy,
                "root_basis": "given",
                "roots": [subject],
                "ranking": [],
                "status": "applied",
                "phase1": actions,
                "phase2": [],
                "released": [],
            },
        )
        self._decided = True

    def _record(self, op: str, kind: str, object_id: str) -> dict[str, Any]:
        return action_record(
            self.incident_id, op, kind, object_id, self.node, self.faulty_agent
        )

    async def _cancel(self, client: Any) -> dict[str, Any]:
        record = self._record("cancel", "run", str(self.run_id))
        record["t_sent"] = now_ms()
        try:
            await client.runs.cancel(
                self.thread_id, self.run_id, action="interrupt", wait=True
            )
            run = await client.runs.get(self.thread_id, self.run_id)
            record["t_confirmed"] = record["t_applied"] = now_ms()
            # A run that had already ended left nothing to stop.
            record["state"] = CONFIRMED if run["status"] == "interrupted" else "noop"
            record["run_status"] = run["status"]
        except Exception as error:  # noqa: BLE001 - recorded as a failed action
            record["state"], record["error"] = "failed", type(error).__name__
        return record

    async def _fork_point(self, client: Any) -> dict[str, Any] | None:
        """The latest supervisor checkpoint before the faulty task completed."""
        before = None
        while True:
            page = await client.threads.get_history(
                self.thread_id, limit=HISTORY_PAGE, before=before
            )
            for entry in page:  # newest first
                values = entry.get("values") or {}
                after_supervisor = entry["metadata"].get("source") == "loop" and any(
                    n in STEP_NODES for n in entry.get("next") or []
                )
                if after_supervisor and self.faulty_task not in (
                    values.get("completed") or []
                ):
                    return entry
            if len(page) < HISTORY_PAGE:
                return None
            before = page[-1]["checkpoint"]

    async def _edit_and_resume(
        self,
        client: Any,
        values: dict[str, Any],
        target: dict[str, Any],
        op: str,
        object_id: str,
    ) -> list[dict[str, Any]]:
        """Edit the state at ``target`` (a thread state: the latest, or the fork
        point) and run the thread from the edit."""
        edit = self._record(op, "state", object_id)
        checkpoint = target.get("checkpoint")
        edit["fork"] = (checkpoint or {}).get("checkpoint_id")
        pending = list(target.get("next") or [])
        if not pending:
            edit["state"] = "noop"  # the run had ended: nothing left to resume
            return [edit]
        if "__start__" in pending:
            edit["state"], edit["error"] = "failed", "before_first_step"
            return [edit]
        as_node = "supervisor" if STEP_NODES & set(pending) else "worker"
        edit["as_node"] = as_node
        edit["t_sent"] = now_ms()
        try:
            update = await client.threads.update_state(
                self.thread_id, values, as_node=as_node, checkpoint=checkpoint
            )
            edit["t_confirmed"] = edit["t_applied"] = now_ms()
            edit["state"] = CONFIRMED
        except Exception as error:  # noqa: BLE001 - recorded as a failed action
            edit["state"], edit["error"] = "failed", type(error).__name__
            return [edit]
        resume = self._record("resume", "run", str(self.thread_id))
        resume["t_sent"] = now_ms()
        try:
            run = await client.runs.create(
                self.thread_id,
                ASSISTANT,
                input=None,
                checkpoint=update["checkpoint"],
                config=self.run_config,
            )
            self.run_id = run["run_id"]
            resume["t_confirmed"] = resume["t_applied"] = now_ms()
            resume["state"] = CONFIRMED
        except Exception as error:  # noqa: BLE001 - recorded as a failed action
            resume["state"], resume["error"] = "failed", type(error).__name__
        return [edit, resume]
