import asyncio

import pytest
from edgecitadel_hermes_plugin.trace_hooks import HermesToolObserver


@pytest.mark.asyncio
async def test_overlapping_runs_isolate_call_ids_and_omit_content():
    class Sink:
        def __init__(self):
            self.events = []

        async def observe(self, observation, *, observation_id):
            self.events.append((observation_id, observation))

    sinks = [Sink(), Sink()]
    observers = [HermesToolObserver(sink, asyncio.get_running_loop()) for sink in sinks]
    for observer in observers:
        await asyncio.to_thread(
            observer.started,
            "same-call-id",
            "owned-tool",
            {"secret": "private-arguments"},
        )
    for observer in reversed(observers):
        await asyncio.to_thread(
            observer.completed, "same-call-id", "owned-tool", {}, "private-result"
        )
        await asyncio.to_thread(
            observer.completed, "same-call-id", "owned-tool", {}, "private-result"
        )
    assert sinks[0].events[0][1]["span_id"] != sinks[1].events[0][1]["span_id"]
    for sink in sinks:
        assert len(sink.events) == 2
        assert sink.events[0][0] != sink.events[1][0]
        assert sink.events[0][1]["span_id"] == sink.events[1][1]["span_id"]
        assert sink.events[1][1]["duration_ms"] >= 0
        assert "private-" not in str(sink.events)


@pytest.mark.asyncio
async def test_event_loop_callback_does_not_deadlock():
    observer = HermesToolObserver(None, asyncio.get_running_loop())
    observer.started("owned-call", "owned-tool", {})
    observer.completed("owned-call", "owned-tool", {}, {})
    assert observer.dropped_callbacks == 2


@pytest.mark.asyncio
async def test_callback_sink_failure_never_escapes_into_hermes():
    class FailedSink:
        async def observe(self, observation, *, observation_id):
            raise RuntimeError("private-error-sentinel")

    observer = HermesToolObserver(FailedSink(), asyncio.get_running_loop())
    await asyncio.to_thread(observer.started, "owned-call", "owned-tool", {})
    await asyncio.to_thread(observer.completed, "owned-call", "owned-tool", {}, {})
    assert observer.dropped_callbacks == 2
    assert not observer._calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "usage,expected",
    [
        (None, (None, None)),
        ({"prompt_tokens": 0}, (0, None)),
        ({"input_tokens": 3, "output_tokens": 2}, (3, 2)),
    ],
)
async def test_model_instance_wrapping_preserves_response_and_unknown_usage(
    usage, expected
):
    from types import SimpleNamespace

    from edgecitadel_hermes_plugin.trace_hooks import HermesModelObserver

    events = []
    effects = []
    response = SimpleNamespace(usage=usage, secret="private-response")

    class Sink:
        async def observe(self, observation, *, observation_id):
            events.append((observation_id, observation))

    class Agent:
        model = "owned-model"

        def _interruptible_api_call(self, kwargs):
            effects.append("called")
            return response

        def _interruptible_streaming_api_call(self, kwargs):
            return self._interruptible_api_call(kwargs)

    agent = Agent()
    observer = HermesModelObserver(Sink(), asyncio.get_running_loop())
    observer.attach(agent)
    observer.attach(agent)
    assert (
        await asyncio.to_thread(
            agent._interruptible_streaming_api_call, {"messages": "private-prompt"}
        )
        is response
    )
    assert effects == ["called"]
    assert [e[1]["phase"] for e in events] == ["started", "finished"]
    assert events[0][1]["span_id"] == events[1][1]["span_id"]
    assert events[0][0] != events[1][0]
    attrs = events[-1][1]["attributes"]
    assert (attrs["input_tokens"], attrs["output_tokens"]) == expected
    assert attrs["usage_unavailable_reason"] == (
        None if all(v is not None for v in expected) else "not_reported"
    )
    assert "private-" not in str(events)


@pytest.mark.asyncio
async def test_model_error_propagates_without_retry_or_error_text_export():
    from edgecitadel_hermes_plugin.trace_hooks import HermesModelObserver

    events = []
    effects = []

    class Sink:
        async def observe(self, observation, *, observation_id):
            events.append(observation)

    class Agent:
        model = "owned-model"

        def _interruptible_api_call(self, kwargs):
            effects.append("called")
            raise ValueError("private-provider-error")

    agent = Agent()
    HermesModelObserver(Sink(), asyncio.get_running_loop()).attach(agent)
    with pytest.raises(ValueError, match="private-provider-error"):
        await asyncio.to_thread(agent._interruptible_api_call, {})
    assert effects == ["called"]
    assert [e["phase"] for e in events] == ["started", "failed"]
    assert "private-provider-error" not in str(events)
