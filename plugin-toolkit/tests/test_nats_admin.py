from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nats.js.errors import NotFoundError

from edgecitadel_supervisor.nats_admin import ensure_inbox_subjects, inbox_subject


def test_inbox_subject_rejects_noncanonical_agent_id() -> None:
    assert inbox_subject("edge_agent-1") == "agents.edge_agent-1.inbox"
    with pytest.raises(ValueError, match="invalid agent id"):
        inbox_subject("edge.agent")


@pytest.mark.asyncio
async def test_ensure_inbox_subjects_creates_exact_destination_subjects() -> None:
    js = SimpleNamespace(
        stream_info=AsyncMock(side_effect=NotFoundError()),
        add_stream=AsyncMock(return_value="created"),
        update_stream=AsyncMock(),
    )

    result = await ensure_inbox_subjects(js, ["agent-b", "agent-a"])

    assert result == "created"
    config = js.add_stream.await_args.args[0]
    assert config.subjects == ["agents.agent-a.inbox", "agents.agent-b.inbox"]
    assert "agents.*.inbox" not in config.subjects
    js.update_stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_inbox_subjects_migrates_wildcard_without_losing_consumers() -> (
    None
):
    existing = SimpleNamespace(
        config=SimpleNamespace(subjects=["agents.*.inbox", "agents.existing.inbox"])
    )
    consumers = [
        SimpleNamespace(
            config=SimpleNamespace(filter_subject="agents.consumer_owned.inbox")
        ),
        SimpleNamespace(config=SimpleNamespace(filter_subject=">")),
    ]
    js = SimpleNamespace(
        stream_info=AsyncMock(return_value=existing),
        consumers_info=AsyncMock(return_value=consumers),
        add_stream=AsyncMock(),
        update_stream=AsyncMock(return_value="updated"),
    )

    result = await ensure_inbox_subjects(js, ["new-agent"])

    assert result == "updated"
    config = js.update_stream.await_args.args[0]
    assert config.subjects == [
        "agents.consumer_owned.inbox",
        "agents.existing.inbox",
        "agents.new-agent.inbox",
    ]
    js.consumers_info.assert_awaited_once_with("AGENT_INBOX")
    js.add_stream.assert_not_awaited()
