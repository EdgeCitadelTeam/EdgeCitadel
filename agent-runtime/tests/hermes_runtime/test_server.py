import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from edgecitadel_hermes_plugin.delegation import bind_agent_execution
from edgecitadel_hermes_plugin.server import register_delegation


def test_registered_tool_dispatches_only_inside_bound_execution():
    server = Mock()
    server.tools = [
        {
            "name": "edgecitadel_delegate",
            "description": "delegate",
            "inputSchema": {"type": "object"},
        }
    ]
    server.handle.return_value = {"result": {"structuredContent": {"task_id": "child"}}}
    registry = Mock()
    registry.get_toolset_for_tool.return_value = None
    toolsets = {}
    register_delegation(server, registry, toolsets)
    handler = registry.register.call_args.kwargs["handler"]
    arguments = {"recipient_id": "worker", "request": "work"}
    assert json.loads(handler(arguments))["error"] == "execution_binding_unavailable"
    server.handle.assert_not_called()

    class Agent:
        def run_conversation(self, **kwargs):
            return handler(arguments)

    agent = Agent()
    bind_agent_execution(agent, SimpleNamespace(binding_id="binding", task_id="parent"))
    assert json.loads(agent.run_conversation()) == {"task_id": "child"}
    params = server.handle.call_args.args[0]["params"]
    assert params["arguments"] == arguments
    assert params["_meta"]["edgecitadel_execution"]["binding_id"] == "binding"
    assert toolsets["edgecitadel-scoped"]["tools"] == ["edgecitadel_delegate"]
    server.handle.return_value = {"result": {"isError": True, "content": "secret"}}
    assert json.loads(agent.run_conversation()) == {"error": "delegation_rejected"}


def test_registration_rejects_existing_unscoped_tool():
    registry = Mock()
    registry.get_toolset_for_tool.return_value = "mcp-existing"
    with pytest.raises(RuntimeError, match="already registered"):
        register_delegation(Mock(), registry, {})
    registry.register.assert_not_called()
