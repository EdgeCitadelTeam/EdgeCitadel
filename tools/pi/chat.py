#!/usr/bin/env python3
"""Terminal client for the resident jim-eq Pi agent; no model or key runs here."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from uuid import uuid4

AGENT = "jim-eq-pi"
HELP = "/agents  /new  /history  /wait  /retry  /quit  /help"


def display(value):
    # Remote text must not be interpreted as terminal controls (including OSC).
    return "".join(
        c
        for c in str(value)
        if c in "\n\t" or not unicodedata.category(c).startswith("C")
    )


class Client:
    def __init__(self, path, base="http://127.0.0.1"):
        self.path = path
        self.base = base.rstrip("/")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.state = (
            json.loads(path.read_text())
            if path.exists()
            else {"session": str(uuid4()), "pending": None, "history": []}
        )

    def save(self):
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as stream:
            os.chmod(temporary, 0o600)
            json.dump(self.state, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        directory = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def api(self, route, payload=None):
        request = urllib.request.Request(
            self.base + "/api" + route,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with self.opener.open(request, timeout=15) as response:
            return json.load(response)

    def agents(self):
        for agent in self.api("/agents"):
            if agent["agent_id"] not in {AGENT, "endurance-pi", "jim-eq-hermes"}:
                continue
            skills = ", ".join(s["id"] for s in agent.get("card", {}).get("skills", []))
            print(display(f"{agent['agent_id']}: {agent['agent_state']} ({skills})"))

    def submit(self, body):
        if self.state["pending"]:
            raise ValueError(
                "An earlier task is unresolved. Use /wait or /retry first."
            )
        if not body.strip() or len(body) > 16000:
            raise ValueError("Enter a nonempty message of at most 16000 characters.")
        self.state["pending"] = {
            "payload": {
                "body": body,
                "skill_id": "pi.coordinate",
                "args": {
                    "session_id": self.state["session"],
                    "request_id": str(uuid4()),
                },
            },
            "task_id": None,
            "child_task_id": None,
        }
        self.save()  # Persist intent before any possibly accepted network request.
        self.retry()

    def retry(self):
        pending = self.state["pending"]
        if not pending:
            print("No pending task.")
            return
        if pending["task_id"]:
            print("Task already accepted; use /wait. No resubmission.")
            return
        receipt = self.api("/command/" + AGENT, pending["payload"])
        task = receipt.get("task_id")
        if not isinstance(task, str) or not task:
            raise ValueError(
                "No task ID in receipt. Submission remains unconfirmed; use /retry."
            )
        pending["task_id"] = task
        self.save()
        print(display(f"Task: {task}"))

    def messages(self, task):
        query = urllib.parse.urlencode({"task_id": task, "limit": 200})
        return sorted(
            self.api("/messages?" + query),
            key=lambda event: event.get("observation_index", 0),
        )

    def wait(self, interval=2, timeout=1500):
        pending = self.state["pending"]
        if not pending:
            print("No pending task.")
            return True
        if not pending["task_id"]:
            print(
                "Submission is unconfirmed. /retry sends the SAME request ID and body."
            )
            return False
        print(
            display(
                f"Watching {pending['task_id']} (Ctrl-C detaches; remote work continues)"
            )
        )
        seen = set()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            events = self.messages(pending["task_id"])
            results = [
                e for e in events if e["type"] == "result" and e["sender_id"] == AGENT
            ]
            # Always consume a known root result even if the child progress endpoint fails.
            if results:
                result = results[-1]
                self.state["history"].append(
                    {
                        "user": pending["payload"]["body"],
                        "task_id": pending["task_id"],
                        "task_state": result.get("task_state", "unknown"),
                        "payload": result.get("payload", {}),
                    }
                )
                self.state["history"] = self.state["history"][-100:]
                self.state["pending"] = None
                self.save()
                self.show_result(self.state["history"][-1])
                return result.get("task_state") == "completed"
            for event in events:
                if event["type"] == "task.progress" and event["sender_id"] == AGENT:
                    child = event.get("payload", {}).get("child_task_id")
                    if child and child != pending["child_task_id"]:
                        pending["child_task_id"] = child
                        self.save()
            if pending["child_task_id"]:
                events += self.messages(pending["child_task_id"])
            for event in events:
                if event["type"] != "task.progress" or event["id"] in seen:
                    continue
                seen.add(event["id"])
                print(
                    display(
                        f"[{event['sender_id']}] {event.get('payload', {}).get('message', '')}"
                    )
                )
            time.sleep(interval)
        print("Stopped waiting. Remote status remains unconfirmed; use /wait later.")
        return False

    def show_result(self, turn):
        payload = turn["payload"]
        print(
            display(
                f"\n{AGENT} [{turn['task_state']}] via {payload.get('executor', 'no executor')}:"
            )
        )
        print(display(payload.get("body", "No response body.")))
        if payload.get("child_task_id"):
            print(display("Child task: " + payload["child_task_id"]))
        print(display(f"Trace: {self.base}/#execution?task={turn['task_id']}\n"))

    def new(self):
        if self.state["pending"]:
            raise ValueError(
                "Resolve the pending task before starting a new conversation."
            )
        self.state = {"session": str(uuid4()), "pending": None, "history": []}
        self.save()
        print("New conversation: " + self.state["session"])

    def chat(self):
        print("Pi · connected through EdgeCitadel to jim-eq-pi (Jev router)")
        print("Conversation: " + self.state["session"])
        print(HELP)
        try:
            self.agents()
            if self.state["pending"]:
                self.wait()
            elif self.state["history"]:
                print("Last response (use /history for earlier turns):")
                self.show_result(self.state["history"][-1])
        except (OSError, ValueError) as error:
            self.report_error(error)
        while True:
            try:
                body = input("you> ").strip()
            except EOFError:
                return
            if not body:
                continue
            try:
                if body in {"/quit", "/exit"}:
                    return
                if body == "/help":
                    print(HELP)
                elif body == "/agents":
                    self.agents()
                elif body == "/new":
                    self.new()
                elif body == "/history":
                    for turn in self.state["history"]:
                        print(display("you: " + turn["user"]))
                        self.show_result(turn)
                elif body in {"/wait", "/retry"}:
                    if body == "/retry":
                        self.retry()
                    self.wait()
                elif body.startswith("/"):
                    print(HELP)
                else:
                    self.submit(body)
                    self.wait()
            except (OSError, ValueError) as error:
                self.report_error(error)

    def report_error(self, error):
        print(display(f"Client error: {error}"), file=sys.stderr)
        if self.state["pending"]:
            print(
                "Task intent is saved. /wait checks acceptance; /retry recovers a missing receipt."
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "message", nargs="*", help="Submit one message and wait; omit to chat."
    )
    parser.add_argument(
        "--status", action="store_true", help="List the agents and exit."
    )
    parser.add_argument(
        "--new", action="store_true", help="Start a fresh conversation."
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path.home() / ".local/state/edgecitadel-pi/chat.json",
        help="Separate conversation state for another terminal or test.",
    )
    args = parser.parse_args()
    os.umask(0o077)
    path = args.state_file.expanduser()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(
                1,
                "This conversation is open in another terminal. Use --state-file PATH.\n",
            )
        client = Client(path)
        client.save()
        try:
            if args.status:
                client.agents()
                return 0
            if args.new:
                client.new()
            if args.message:
                client.submit(" ".join(args.message))
                return 0 if client.wait() else 1
            client.chat()
            return 0
        except KeyboardInterrupt:
            print(
                "\nDetached. The resident agent continues; run pi again to reconnect."
            )
            return 130
        except (OSError, ValueError) as error:
            client.report_error(error)
            return 1


if __name__ == "__main__":
    sys.exit(main())
