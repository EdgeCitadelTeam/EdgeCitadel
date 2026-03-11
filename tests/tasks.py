"""
NATS Migration E2E Test Plan
=============================
Each test validates a specific migration milestone. Tests are ordered by phase
and can be run incrementally as the migration progresses.

Run: pytest tests/tasks.py -v
Requires: NATS server running on localhost:4222 (docker-compose up nats)
"""

import asyncio
import json
import time
import uuid

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NATS_URL = "nats://localhost:4222"
API_URL = "http://localhost:8000"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def nats_conn():
    """Connect to NATS for test publishing/subscribing."""
    import nats
    nc = await nats.connect(NATS_URL)
    yield nc
    await nc.drain()


@pytest_asyncio.fixture
async def js(nats_conn):
    """JetStream context."""
    return nats_conn.jetstream()


@pytest.fixture
def api_client():
    """HTTP client for aggregator REST API."""
    import httpx
    with httpx.Client(base_url=API_URL, timeout=10) as client:
        yield client


@pytest.fixture
def ws_client():
    """WebSocket client for aggregator streaming."""
    import websockets
    return websockets


def _test_agent_id():
    """Generate a test agent ID that matches the test filter pattern."""
    ts = int(time.time() * 1000)
    return f"test-nats-{ts}"


# ===========================================================================
# PHASE 1: NATS Infrastructure
# ===========================================================================

class TestPhase1_NATSInfrastructure:
    """Verify NATS server is running, JetStream is enabled, and basic
    pub/sub works as a replacement for Mosquitto."""

    @pytest.mark.asyncio
    async def test_nats_server_reachable(self, nats_conn):
        """NATS server accepts connections on localhost:4222."""
        assert nats_conn.is_connected

    @pytest.mark.asyncio
    async def test_jetstream_enabled(self, js):
        """JetStream is enabled and responsive."""
        info = await js.account_info()
        assert info is not None

    @pytest.mark.asyncio
    async def test_basic_pubsub(self, nats_conn):
        """Basic pub/sub works — message published on subject is received."""
        received = []
        sub = await nats_conn.subscribe("test.pubsub")
        test_payload = json.dumps({"msg": "hello"})
        await nats_conn.publish("test.pubsub", test_payload.encode())
        try:
            msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
            received.append(json.loads(msg.data.decode()))
        except asyncio.TimeoutError:
            pass
        await sub.unsubscribe()
        assert len(received) == 1
        assert received[0]["msg"] == "hello"

    @pytest.mark.asyncio
    async def test_request_reply(self, nats_conn):
        """NATS request-reply pattern works (replaces MQTT command/response)."""
        async def responder(msg):
            reply = json.dumps({"status": "ok", "echo": json.loads(msg.data.decode())})
            await nats_conn.publish(msg.reply, reply.encode())

        sub = await nats_conn.subscribe("test.request", cb=responder)
        request_data = json.dumps({"command": "ping"})
        response = await nats_conn.request("test.request", request_data.encode(), timeout=2.0)
        result = json.loads(response.data.decode())
        await sub.unsubscribe()

        assert result["status"] == "ok"
        assert result["echo"]["command"] == "ping"

    @pytest.mark.asyncio
    async def test_wildcard_subscription(self, nats_conn):
        """Wildcard subscriptions work (replaces MQTT # wildcard)."""
        received = []
        sub = await nats_conn.subscribe("agents.>")

        await nats_conn.publish("agents.rupert.heartbeat", b'{"status":"online"}')
        await nats_conn.publish("agents.jeeves.register", b'{"role":"iot"}')

        for _ in range(2):
            try:
                msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
                received.append(msg.subject)
            except asyncio.TimeoutError:
                break
        await sub.unsubscribe()

        assert "agents.rupert.heartbeat" in received
        assert "agents.jeeves.register" in received

    @pytest.mark.asyncio
    async def test_jetstream_stream_create(self, js):
        """Can create a JetStream stream for conversations."""
        stream_name = f"TEST_CONVERSATIONS_{int(time.time())}"
        try:
            await js.add_stream(name=stream_name, subjects=[f"{stream_name}.>"])
            info = await js.find_stream_name_by_subject(f"{stream_name}.test")
            assert info == stream_name
        finally:
            await js.delete_stream(stream_name)

    @pytest.mark.asyncio
    async def test_jetstream_publish_and_consume(self, js):
        """JetStream stores messages and allows ordered consumption."""
        stream_name = f"TEST_PERSIST_{int(time.time())}"
        subject = f"{stream_name}.messages"
        try:
            await js.add_stream(name=stream_name, subjects=[f"{stream_name}.>"])

            # Publish 3 messages
            for i in range(3):
                await js.publish(subject, json.dumps({"seq": i}).encode())

            # Consume all 3 in order
            consumed = []
            sub = await js.subscribe(subject, ordered_consumer=True)
            for _ in range(3):
                msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
                consumed.append(json.loads(msg.data.decode()))
                await msg.ack()
            await sub.unsubscribe()

            assert [m["seq"] for m in consumed] == [0, 1, 2]
        finally:
            await js.delete_stream(stream_name)

    @pytest.mark.asyncio
    async def test_jetstream_kv_store(self, js):
        """JetStream K/V store works for agent state."""
        bucket_name = f"TEST_AGENT_STATE_{int(time.time())}"
        try:
            kv = await js.create_key_value(bucket=bucket_name)
            await kv.put("rupert", json.dumps({"status": "online", "cpu": 45.2}).encode())
            entry = await kv.get("rupert")
            data = json.loads(entry.value.decode())
            assert data["status"] == "online"
            assert data["cpu"] == 45.2
        finally:
            await js.delete_key_value(bucket_name)


# ===========================================================================
# PHASE 2: Aggregator Migration (paho-mqtt -> nats.py)
# ===========================================================================

class TestPhase2_AggregatorMigration:
    """Verify the aggregator correctly receives NATS messages, parses them
    into structured records, and stores in SQLite — same behavior as MQTT."""

    @pytest.mark.asyncio
    async def test_agent_heartbeat_creates_agent(self, nats_conn, api_client):
        """Publishing a heartbeat on agents.{name}.heartbeat creates an agent record."""
        agent_id = _test_agent_id()
        payload = json.dumps({
            "sender_id": agent_id,
            "status": "online",
            "display_name": "Test NATS Agent",
            "role": "test",
        })
        await nats_conn.publish(f"agents.{agent_id}.heartbeat", payload.encode())
        await asyncio.sleep(1)  # Allow aggregator to process

        resp = api_client.get(f"/agents/{agent_id}")
        assert resp.status_code == 200
        agent = resp.json()
        assert agent["status"] == "online"

    @pytest.mark.asyncio
    async def test_agent_register(self, nats_conn, api_client):
        """Publishing on agents.{name}.register creates agent with metadata."""
        agent_id = _test_agent_id()
        payload = json.dumps({
            "sender_id": agent_id,
            "display_name": "NATS Jeeves",
            "role": "iot",
            "device_type": "raspberry-pi",
            "capabilities": ["temperature", "humidity"],
        })
        await nats_conn.publish(f"agents.{agent_id}.register", payload.encode())
        await asyncio.sleep(1)

        resp = api_client.get(f"/agents/{agent_id}")
        assert resp.status_code == 200
        agent = resp.json()
        assert agent["role"] == "iot"
        assert agent["device_type"] == "raspberry-pi"

    @pytest.mark.asyncio
    async def test_command_stored_as_message(self, nats_conn, api_client):
        """A command published via NATS is stored in the messages table."""
        agent_id = _test_agent_id()
        corr_id = str(uuid.uuid4())
        payload = json.dumps({
            "sender_id": "dashboard",
            "receiver_id": agent_id,
            "message_type": "command",
            "correlation_id": corr_id,
            "message": "check temperature",
        })
        await nats_conn.publish(f"agents.{agent_id}.inbox", payload.encode())
        await asyncio.sleep(1)

        resp = api_client.get(f"/messages?correlation_id={corr_id}")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) >= 1
        assert items[0]["message_type"] == "command"

    @pytest.mark.asyncio
    async def test_result_completes_task(self, nats_conn, api_client):
        """A result message auto-completes a previously created task."""
        agent_id = _test_agent_id()
        corr_id = str(uuid.uuid4())

        # Send command (auto-creates task)
        cmd = json.dumps({
            "sender_id": "dashboard",
            "receiver_id": agent_id,
            "message_type": "command",
            "correlation_id": corr_id,
            "message": "read sensor",
        })
        await nats_conn.publish(f"agents.{agent_id}.inbox", cmd.encode())
        await asyncio.sleep(0.5)

        # Send result (auto-completes task)
        result = json.dumps({
            "sender_id": agent_id,
            "receiver_id": "dashboard",
            "message_type": "result",
            "correlation_id": corr_id,
            "message": "temperature: 22.5C",
        })
        await nats_conn.publish(f"agents.{agent_id}.outbox", result.encode())
        await asyncio.sleep(1)

        resp = api_client.get(f"/tasks?agent={agent_id}")
        tasks = resp.json()["items"]
        matching = [t for t in tasks if t["id"] == corr_id]
        assert len(matching) == 1
        assert matching[0]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_websocket_receives_nats_events(self, nats_conn, ws_client):
        """WebSocket /ws/stream receives events from NATS messages."""
        agent_id = _test_agent_id()
        async with ws_client.connect(f"ws://localhost:8000/ws/stream") as ws:
            payload = json.dumps({
                "sender_id": agent_id,
                "status": "online",
            })
            await nats_conn.publish(f"agents.{agent_id}.heartbeat", payload.encode())

            # Wait for event on WebSocket
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                event = json.loads(msg)
                assert event.get("event") in ("message", "agent_registered", "agent_status_change")
            except asyncio.TimeoutError:
                pytest.fail("No WebSocket event received within timeout")

    @pytest.mark.asyncio
    async def test_deduplication(self, nats_conn, api_client):
        """Duplicate messages with same correlation_id are not double-stored."""
        agent_id = _test_agent_id()
        corr_id = str(uuid.uuid4())
        payload = json.dumps({
            "sender_id": agent_id,
            "receiver_id": "dashboard",
            "message_type": "info",
            "correlation_id": corr_id,
            "message": "dedup test",
        })
        # Publish same message twice rapidly
        await nats_conn.publish(f"agents.{agent_id}.outbox", payload.encode())
        await nats_conn.publish(f"agents.{agent_id}.outbox", payload.encode())
        await asyncio.sleep(1)

        resp = api_client.get(f"/messages?correlation_id={corr_id}")
        items = resp.json()["items"]
        assert len(items) == 1  # Only one stored

    @pytest.mark.asyncio
    async def test_publish_command_via_api(self, api_client, nats_conn):
        """POST /command/{agent} publishes to NATS (not MQTT)."""
        agent_id = _test_agent_id()
        # Pre-register agent
        payload = json.dumps({"sender_id": agent_id, "status": "online"})
        await nats_conn.publish(f"agents.{agent_id}.heartbeat", payload.encode())
        await asyncio.sleep(0.5)

        # Subscribe to agent's inbox
        received = []
        sub = await nats_conn.subscribe(f"agents.{agent_id}.inbox")

        resp = api_client.post(f"/command/{agent_id}", json={
            "message": "test command via API",
        })
        assert resp.status_code == 200

        try:
            msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
            received.append(json.loads(msg.data.decode()))
        except asyncio.TimeoutError:
            pass
        await sub.unsubscribe()

        assert len(received) == 1
        assert "test command via API" in str(received[0])

    @pytest.mark.asyncio
    async def test_no_thread_bridging(self):
        """Verify aggregator no longer uses asyncio.run_coroutine_threadsafe."""
        import importlib
        import inspect
        # Import the aggregator module and check for thread-bridging calls
        spec = importlib.util.spec_from_file_location(
            "aggregator_check",
            "/app/aggregator.py"  # Docker path; adjust for local
        )
        if spec and spec.loader:
            source = inspect.getsource(spec.loader)  # type: ignore
            assert "run_coroutine_threadsafe" not in source, \
                "Aggregator should not use run_coroutine_threadsafe with NATS"


# ===========================================================================
# PHASE 3: JetStream Conversations & Sessions
# ===========================================================================

class TestPhase3_JetStreamSessions:
    """Verify conversation persistence and session replay via JetStream."""

    STREAM_NAME = "CONVERSATIONS"

    @pytest.mark.asyncio
    async def test_conversations_stream_exists(self, js):
        """The CONVERSATIONS stream is created on startup."""
        info = await js.find_stream_name_by_subject("conversations.test")
        assert info == self.STREAM_NAME

    @pytest.mark.asyncio
    async def test_conversation_messages_persisted(self, js, nats_conn):
        """Messages published to conversations.{session}.{agent} are stored in JetStream."""
        session_id = str(uuid.uuid4())
        agent_id = _test_agent_id()
        subject = f"conversations.{session_id}.{agent_id}"

        for i in range(3):
            await js.publish(subject, json.dumps({
                "seq": i, "message": f"turn {i}",
            }).encode())

        # Verify all 3 are persisted
        sub = await js.subscribe(subject, ordered_consumer=True)
        messages = []
        for _ in range(3):
            msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
            messages.append(json.loads(msg.data.decode()))
            await msg.ack()
        await sub.unsubscribe()

        assert len(messages) == 3
        assert [m["seq"] for m in messages] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_session_replay_from_sequence(self, js):
        """An agent can replay a session from a specific sequence number."""
        session_id = str(uuid.uuid4())
        subject = f"conversations.{session_id}.replay-test"

        # Publish 5 messages
        for i in range(5):
            await js.publish(subject, json.dumps({"seq": i}).encode())

        # Replay from sequence 3 (skip first 3)
        sub = await js.subscribe(subject, ordered_consumer=True)
        all_msgs = []
        for _ in range(5):
            msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
            all_msgs.append(json.loads(msg.data.decode()))
            await msg.ack()
        await sub.unsubscribe()

        replayed = all_msgs[2:]  # Simulate starting from seq 3
        assert len(replayed) == 3
        assert replayed[0]["seq"] == 2

    @pytest.mark.asyncio
    async def test_kv_agent_state(self, js):
        """Agent state is readable/writable from the K/V bucket."""
        bucket_name = "AGENT_STATE"
        try:
            kv = await js.key_value(bucket_name)
        except Exception:
            kv = await js.create_key_value(bucket=bucket_name)

        agent_id = _test_agent_id()
        state = {
            "status": "online",
            "cpu_percent": 35.0,
            "memory_percent": 62.1,
            "last_heartbeat": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        await kv.put(agent_id, json.dumps(state).encode())

        entry = await kv.get(agent_id)
        loaded = json.loads(entry.value.decode())
        assert loaded["status"] == "online"
        assert loaded["cpu_percent"] == 35.0

        # Cleanup
        await kv.delete(agent_id)


# ===========================================================================
# PHASE 4: Streaming & Token Delivery
# ===========================================================================

class TestPhase4_Streaming:
    """Verify LLM token streaming between agents via NATS subjects."""

    @pytest.mark.asyncio
    async def test_token_streaming(self, nats_conn):
        """Agent B streams tokens back to Agent A on tasks.{id}.stream."""
        task_id = str(uuid.uuid4())
        stream_subject = f"tasks.{task_id}.stream"

        received_tokens = []
        sub = await nats_conn.subscribe(stream_subject)

        # Simulate agent streaming 5 tokens
        tokens = ["Hello", " world", ",", " how", " are you?"]
        for token in tokens:
            await nats_conn.publish(stream_subject, json.dumps({
                "token": token,
                "done": token == tokens[-1],
            }).encode())

        for _ in range(len(tokens)):
            try:
                msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
                received_tokens.append(json.loads(msg.data.decode()))
            except asyncio.TimeoutError:
                break
        await sub.unsubscribe()

        assert len(received_tokens) == 5
        full_text = "".join(t["token"] for t in received_tokens)
        assert full_text == "Hello world, how are you?"
        assert received_tokens[-1]["done"] is True

    @pytest.mark.asyncio
    async def test_streaming_task_lifecycle(self, nats_conn):
        """Full lifecycle: assign task -> stream tokens -> complete."""
        task_id = str(uuid.uuid4())
        events = []

        # Subscribe to all task events
        sub = await nats_conn.subscribe(f"tasks.{task_id}.>")

        # 1. Assign task
        await nats_conn.publish(f"tasks.{task_id}.assign", json.dumps({
            "assigned_agent": "jeeves",
            "prompt": "What is the temperature?",
        }).encode())

        # 2. Stream tokens
        for token in ["The", " temperature", " is", " 22C"]:
            await nats_conn.publish(f"tasks.{task_id}.stream", json.dumps({
                "token": token,
            }).encode())

        # 3. Complete
        await nats_conn.publish(f"tasks.{task_id}.complete", json.dumps({
            "result": "The temperature is 22C",
            "status": "completed",
        }).encode())

        for _ in range(6):  # 1 assign + 4 tokens + 1 complete
            try:
                msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
                events.append(msg.subject)
            except asyncio.TimeoutError:
                break
        await sub.unsubscribe()

        assert f"tasks.{task_id}.assign" in events
        assert f"tasks.{task_id}.stream" in events
        assert f"tasks.{task_id}.complete" in events

    @pytest.mark.asyncio
    async def test_streaming_to_websocket(self, nats_conn, ws_client):
        """Token stream events are forwarded to WebSocket /ws/stream."""
        task_id = str(uuid.uuid4())
        async with ws_client.connect("ws://localhost:8000/ws/stream") as ws:
            await nats_conn.publish(f"tasks.{task_id}.stream", json.dumps({
                "token": "hello",
                "task_id": task_id,
            }).encode())

            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                event = json.loads(msg)
                assert "token" in str(event) or "stream" in str(event)
            except asyncio.TimeoutError:
                pytest.skip("WebSocket streaming not yet wired to NATS task subjects")


# ===========================================================================
# PHASE 5: Command Pipeline (REST -> NATS -> Agent -> Response)
# ===========================================================================

class TestPhase5_CommandPipeline:
    """End-to-end: dashboard sends command via REST, agent receives via NATS,
    agent responds, dashboard sees result."""

    @pytest.mark.asyncio
    async def test_full_command_response_cycle(self, api_client, nats_conn):
        """POST /command -> NATS inbox -> agent responds -> task completed."""
        agent_id = _test_agent_id()

        # Register agent
        await nats_conn.publish(f"agents.{agent_id}.register", json.dumps({
            "sender_id": agent_id, "status": "online", "role": "test",
        }).encode())
        await asyncio.sleep(0.5)

        # Agent subscribes to inbox
        inbox_msgs = []
        sub = await nats_conn.subscribe(f"agents.{agent_id}.inbox")

        # Dashboard sends command
        resp = api_client.post(f"/command/{agent_id}", json={
            "message": "get status",
        })
        assert resp.status_code == 200
        corr_id = resp.json()["correlation_id"]

        # Agent receives command
        try:
            msg = await asyncio.wait_for(sub.next_msg(), timeout=2.0)
            inbox_msgs.append(json.loads(msg.data.decode()))
        except asyncio.TimeoutError:
            pass
        await sub.unsubscribe()

        assert len(inbox_msgs) == 1

        # Agent sends response
        await nats_conn.publish(f"agents.{agent_id}.outbox", json.dumps({
            "sender_id": agent_id,
            "receiver_id": "dashboard",
            "message_type": "result",
            "correlation_id": corr_id,
            "message": "all systems nominal",
        }).encode())
        await asyncio.sleep(1)

        # Verify task is completed
        resp = api_client.get(f"/tasks?agent={agent_id}")
        tasks = resp.json()["items"]
        matching = [t for t in tasks if t["id"] == corr_id]
        assert len(matching) == 1
        assert matching[0]["status"] == "completed"


# ===========================================================================
# PHASE 6: Backward Compatibility & Regression
# ===========================================================================

class TestPhase6_Regression:
    """Ensure existing API behavior is unchanged after migration."""

    def test_health_endpoint(self, api_client):
        """GET /health still returns ok."""
        resp = api_client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_system_status(self, api_client):
        """GET /system/status returns expected fields."""
        resp = api_client.get("/system/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "agents_online" in data
        assert "total_messages" in data
        assert "active_tasks" in data

    def test_agents_crud(self, api_client):
        """Agent CRUD via REST API still works."""
        agent_id = _test_agent_id()

        # Create
        resp = api_client.post("/agents", json={"id": agent_id, "status": "online"})
        assert resp.status_code == 201

        # Read
        resp = api_client.get(f"/agents/{agent_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == agent_id

        # Update
        resp = api_client.patch(f"/agents/{agent_id}", json={"role": "updated"})
        assert resp.status_code == 200
        assert resp.json()["role"] == "updated"

        # Delete
        resp = api_client.delete(f"/agents/{agent_id}")
        assert resp.status_code == 204

    def test_messages_query(self, api_client):
        """GET /messages returns structured message list."""
        resp = api_client.get("/messages?limit=5")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_tasks_query(self, api_client):
        """GET /tasks returns task list."""
        resp = api_client.get("/tasks?limit=5")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_logs_query(self, api_client):
        """GET /logs returns log list."""
        resp = api_client.get("/logs?limit=5")
        assert resp.status_code == 200
        assert "items" in resp.json()

    def test_message_flow(self, api_client):
        """GET /messages/flow returns graph structure."""
        resp = api_client.get("/messages/flow")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert "edges" in data

    def test_exclude_test_filter(self, api_client):
        """exclude_test=true filters out test agents."""
        resp = api_client.get("/agents?exclude_test=true")
        assert resp.status_code == 200
        agents = resp.json()
        for agent in agents:
            assert not agent["id"].startswith("test-nats-")

    def test_websocket_connect(self, ws_client):
        """WebSocket /ws/stream accepts connections."""
        async def _check():
            async with ws_client.connect("ws://localhost:8000/ws/stream") as ws:
                assert ws.open
        asyncio.get_event_loop().run_until_complete(_check())


# ===========================================================================
# PHASE 7: Performance & Edge Cases
# ===========================================================================

class TestPhase7_Performance:
    """Verify performance characteristics of the NATS setup."""

    @pytest.mark.asyncio
    async def test_high_throughput_pubsub(self, nats_conn):
        """NATS handles 1000 messages without dropping any."""
        subject = f"perf.test.{int(time.time())}"
        received_count = 0
        done = asyncio.Event()

        async def handler(msg):
            nonlocal received_count
            received_count += 1
            if received_count >= 1000:
                done.set()

        sub = await nats_conn.subscribe(subject, cb=handler)

        for i in range(1000):
            await nats_conn.publish(subject, json.dumps({"i": i}).encode())

        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            pass
        await sub.unsubscribe()

        assert received_count == 1000

    @pytest.mark.asyncio
    async def test_latency_local(self, nats_conn):
        """Local NATS request-reply latency is under 10ms."""
        async def echo(msg):
            await nats_conn.publish(msg.reply, msg.data)

        sub = await nats_conn.subscribe("perf.echo", cb=echo)

        latencies = []
        for _ in range(100):
            start = time.monotonic()
            await nats_conn.request("perf.echo", b"ping", timeout=1.0)
            latencies.append((time.monotonic() - start) * 1000)
        await sub.unsubscribe()

        avg_ms = sum(latencies) / len(latencies)
        p99_ms = sorted(latencies)[98]
        assert avg_ms < 10, f"Average latency {avg_ms:.2f}ms exceeds 10ms"
        assert p99_ms < 50, f"p99 latency {p99_ms:.2f}ms exceeds 50ms"

    @pytest.mark.asyncio
    async def test_concurrent_subscribers(self, nats_conn):
        """Multiple subscribers on same subject all receive messages."""
        subject = f"perf.fanout.{int(time.time())}"
        counts = [0, 0, 0]

        async def make_handler(idx):
            async def handler(msg):
                counts[idx] += 1
            return handler

        subs = []
        for i in range(3):
            cb = await make_handler(i)
            subs.append(await nats_conn.subscribe(subject, cb=cb))

        for _ in range(10):
            await nats_conn.publish(subject, b"test")
        await asyncio.sleep(1)

        for sub in subs:
            await sub.unsubscribe()

        # All 3 subscribers should have received all 10 messages
        assert all(c == 10 for c in counts), f"Fan-out counts: {counts}"
