"""session.mjs effect gate against a scripted stand-in for the Pi SDK.

The real SDK (0.87.1) is not installed here. The stand-in implements only the
surface session.mjs uses, including the extension contract the gate relies on:
``DefaultResourceLoader({extensionFactories})`` calls each factory with ``pi``,
and a ``pi.on("tool_call")`` handler returning ``{block: true}`` stops the tool.
The test therefore checks session.mjs's classification, private IPC and
fail-closed behaviour, not the SDK itself; that contract still needs live
verification on Endurance.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

SESSION = Path(__file__).parents[2] / "agent-packages/pi-root/pi_runtime/session.mjs"
NODE = shutil.which("node")

FAKE_SDK = r"""
export const SettingsManager = { inMemory: () => ({}) };
export const SessionManager = { continueRecent: (cwd, dir) => ({ getSessionFile: () => dir + '/s.jsonl' }) };
export class ModelRuntime {
  static async create() { return new ModelRuntime(); }
  registerProvider() {}
  getModels() { return [{ id: 'fake-model' }]; }
}
export class DefaultResourceLoader {
  constructor(options) {
    this.factories = process.env.FAKE_PI_NO_EXTENSIONS ? [] : (options.extensionFactories ?? []);
  }
  async reload() {}
}
export async function createAgentSession({ resourceLoader }) {
  const handlers = [];
  const pi = { on: (name, handler) => { if (name === 'tool_call') handlers.push(handler); } };
  for (const factory of resourceLoader.factories) factory(pi);
  const subscribers = [];
  const log = [];
  const session = {
    messages: [],
    subscribe: fn => subscribers.push(fn),
    dispose() {},
    abort() {},
    sessionManager: { getSessionFile: () => 'transcript.jsonl' },
    async prompt() {
      for (const [index, call] of JSON.parse(process.env.FAKE_PI_TOOLS).entries()) {
        const event = { type: 'tool_call', toolCallId: 'call-' + index, toolName: call.toolName, input: call.input };
        let outcome = 'executed';
        for (const handler of handlers) {
          const result = await handler(event, {});
          if (result?.block) outcome = 'blocked: ' + result.reason;
        }
        log.push({ tool: call.toolName, outcome });
      }
      session.messages = [{ role: 'assistant', stopReason: 'stop' }];
    },
    getLastAssistantText: () => JSON.stringify(log),
  };
  return { session };
}
"""
FAKE_AI = r"""
export const Type = { Object: x => x, String: () => ({}) };
export function createAssistantMessageEventStream() { return { push() {}, end() {} }; }
"""


def package(root: Path, name: str, source: str) -> None:
    directory = root / "node_modules/@earendil-works" / name
    directory.mkdir(parents=True)
    (directory / "package.json").write_text(
        json.dumps(
            {"name": f"@earendil-works/{name}", "type": "module", "main": "index.js"}
        )
    )
    (directory / "index.js").write_text(source)


@pytest.fixture
def worker(tmp_path):
    if NODE is None:
        pytest.skip("node is not installed")
    sdk = tmp_path / "sdk"
    sdk.mkdir()
    (sdk / "package.json").write_text("{}")
    package(sdk, "pi-coding-agent", FAKE_SDK)
    package(sdk, "pi-ai", FAKE_AI)
    workspace = tmp_path / "workspace"

    def run(tools, decide, **extra_env):
        process = subprocess.Popen(
            [NODE, str(SESSION)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env={
                "PATH": os.environ.get("PATH", ""),
                "PI_SDK_DIR": str(sdk),
                "PI_ROLE": "worker",
                "PI_WORKSPACE": str(workspace),
                "EDGECITADEL_PLUGIN_STATE_DIR": str(tmp_path / "state"),
                "FAKE_PI_TOOLS": json.dumps(tools),
                **extra_env,
            },
        )
        asked = []
        try:
            assert json.loads(process.stdout.readline())["type"] == "ready"
            process.stdin.write(
                json.dumps({"type": "prompt", "body": "work", "session_key": "k" * 64})
                + "\n"
            )
            process.stdin.flush()
            while True:
                message = json.loads(process.stdout.readline())
                if message["type"] == "call":
                    assert message["operation"] == "effect"
                    asked.append(
                        {
                            key: message[key]
                            for key in ("effect_class", "target", "tool_call_id")
                        }
                    )
                    process.stdin.write(
                        json.dumps(
                            {"type": "reply", "id": message["id"], **decide(message)}
                        )
                        + "\n"
                    )
                    process.stdin.flush()
                elif message["type"] in {"result", "error"}:
                    return message, asked
        finally:
            process.stdin.close()
            process.wait(timeout=10)

    return run, workspace


def test_effects_are_classified_and_denied_tools_never_run(worker):
    run, workspace = worker
    tools = [
        {"toolName": "read", "input": {"path": "notes.md"}},
        {"toolName": "write", "input": {"path": "out/report.md", "content": "x"}},
        {"toolName": "edit", "input": {"path": "/etc/hosts"}},
        {"toolName": "bash", "input": {"command": "curl example.org"}},
        {"toolName": "write", "input": {"path": "out/second.md"}},
    ]

    def decide(message):
        if message["target"] == "workspace/out/report.md":
            return {"result": {"allowed": True, "code": None}}
        if message["effect_class"] == "process.exec":
            return {"error": "authorization transport failed"}
        return {"result": {"allowed": False, "code": "outside_effect_scope"}}

    result, asked = run(tools, decide)
    assert result["type"] == "result"
    outcomes = {
        entry["tool"] + str(i): entry["outcome"]
        for i, entry in enumerate(json.loads(result["body"]))
    }
    assert outcomes["read0"] == "executed"  # reads are not external effects
    assert outcomes["write1"] == "executed"
    assert (
        outcomes["edit2"].startswith("blocked")
        and "outside_effect_scope" in outcomes["edit2"]
    )
    assert outcomes["bash3"].startswith("blocked")  # no answer: fail closed
    assert outcomes["write4"].startswith("blocked")
    assert asked == [
        {
            "effect_class": "fs.write",
            "target": "workspace/out/report.md",
            "tool_call_id": "call-1",
        },
        {"effect_class": "fs.write", "target": "/etc/hosts", "tool_call_id": "call-2"},
        {"effect_class": "process.exec", "target": "bash", "tool_call_id": "call-3"},
        {
            "effect_class": "fs.write",
            "target": "workspace/out/second.md",
            "tool_call_id": "call-4",
        },
    ]
    assert workspace.is_dir()


def test_worker_refuses_to_run_when_the_gate_cannot_be_installed(worker):
    run, _ = worker
    result, asked = run(
        [{"toolName": "bash", "input": {}}],
        lambda _message: {"result": {"allowed": True}},
        FAKE_PI_NO_EXTENSIONS="1",
    )
    assert result["type"] == "error"
    assert "effect gate" in result["body"]
    assert asked == []
