"""Explicit test provenance for owned jim-eq connectors."""


def test_card(agent_id):
    return {
        "name": agent_id,
        "description": "Disposable E2E fixture",
        "version": "0.1.0",
        "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
        "provider": {"organization": "EdgeCitadel"},
        "capabilities": {},
        "securitySchemes": {},
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [],
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 15,
            "runtime.deployment": "test",
        },
    }
