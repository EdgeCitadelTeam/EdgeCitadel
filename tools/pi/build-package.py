"""Build standalone Managed Agent packages from the shared Pi runtime."""

import argparse
import json
import shutil
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("role", choices=["root", "worker"])
parser.add_argument("destination", type=Path)
args = parser.parse_args()
source = Path(__file__).resolve().parents[2] / "agent-packages/pi-root"
root = args.destination
if root.resolve() != source.resolve():
    shutil.copytree(
        source,
        root,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("plugin.lock.json", "__pycache__"),
    )
role = args.role
name = "pi-root" if role == "root" else "pi-worker"
agent = "jim-eq-pi" if role == "root" else "endurance-pi"
skill = "pi.coordinate" if role == "root" else "pi.execute"
(root / "pi_runtime").mkdir(parents=True, exist_ok=True)
(root / "plugin.yaml").write_text(f"""apiVersion: edgecitadel.io/v1alpha2
kind: ManagedAgent
metadata:
  name: {name}
  displayName: {"Jim-eq Pi + Jev" if role == "root" else "Endurance Pi"}
  description: Persistent Pi harness connected through actual EdgeCitadel task transport.
  version: 0.1.1
  publisher: edgecitadel
compatibility:
  supervisorApi: ">=0.1.0,<0.2.0"
  protocols: [edgecitadel.managed-agent.v1]
runtime:
  kind: agent_runtime
  command: [python, -m, pi_runtime]
  environmentVariables: [PI_ROLE, PI_NODE, PI_SDK_DIR, PI_WORKSPACE]
  healthTimeoutSeconds: 60
  restartPolicy: on-failure
skills:
  directory: skills
agents:
  - id: {agent}
    skillNames: [pi-task]
    listensBroadcast: false
permissions:
  knowledge: []
  messaging:
    outboundAgents: {("[aggregator, endurance-pi, jim-eq-hermes]" if role == "root" else "[aggregator, jim-eq-pi]")}
  network:
    outbound: {('["https://api.typesafe.ai", "http://127.0.0.1"]' if role == "root" else '["http://127.0.0.1:11434"]')}
  devices: []
  # Pi worker tools, gated by effect.authorize. The root holds them only to
  # delegate them: a child's effects never exceed its delegator's.
  effects:
    - {{effect_class: fs.write, target: "workspace/*"}}
    - {{effect_class: process.exec, target: bash}}
security:
  sandbox: restricted
  secrets: {("[TYPESAFE_API_KEY]" if role == "root" else "[]")}
extensions: {{}}
""")
(root / "pi_runtime/config.yaml").write_text(f"""agent_id: {agent}
name: {agent}
description: {"Persistent Pi coordinator using Jev routing, with real EdgeCitadel delegation." if role == "root" else "Persistent Pi worker on NetSys-Endurance with local Qwen and real file/shell tools."}
version: 0.1.1
runtime:
  kind: native
  roles: [{("orchestrator" if role == "root" else "worker")}]
  conformance: L1
  heartbeat_interval_sec: 10
  tags: [pi, persistent, {role}]
capabilities:
  streaming: true
skills:
  - id: {skill}
    name: pi-task
    description: {"Route a natural-language task to an authorized executor." if role == "root" else "Execute a natural-language task with local tools and model."}
    tags: [pi, {role}]
defaultInputModes: [text/plain]
defaultOutputModes: [text/plain]
""")
s = root / "skills/pi-task"
(s / "schemas").mkdir(parents=True, exist_ok=True)
(s / "SKILL.md").write_text(f"""---
name: pi-task
description: {"Coordinate tasks through Jev and EdgeCitadel." if role == "root" else "Execute tasks using the persistent Endurance Pi harness."}
---

Accept natural-language requests. Reuse args.session_id for a conversation.
Return actual execution results and task identifiers. Never report unconfirmed
remote execution as successful, or automatically repeat interrupted work.
""")
(s / "binding.yaml").write_text(f"""apiVersion: edgecitadel.io/v1alpha1
kind: AgentSkillBinding
skillId: {skill}
version: 0.1.1
execution:
  kind: runtime-handler
  name: pi-task
schemas:
  input: schemas/input.json
  output: schemas/output.json
requires:
  knowledge: []
  network: []
  devices: []
extensions: {{}}
""")
for filename in ["input", "output"]:
    (s / f"schemas/{filename}.json").write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"body": {"type": "string"}, "args": {"type": "object"}},
                "required": ["body"],
                "additionalProperties": True,
            },
            indent=2,
        )
        + "\n"
    )
print(root)
