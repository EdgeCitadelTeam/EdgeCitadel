/**
 * Dashboard Command Pipeline E2E tests
 *
 * Tests the CRITICAL path: Dashboard sends command via API → aggregator stores
 * in message history → agent receives via NATS → agent replies → reply stored.
 *
 * This test suite was created to catch the bug where dashboard-sent commands
 * were silently dropped by the aggregator (sender "dashboard" in SKIP_AGENT_IDS
 * caused the message to be discarded before storage).
 */
const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Dashboard Command Pipeline', () => {
  let agentName;
  let agentData;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    agentData = makeAgent({ display_name: `Pipeline-${uniqueId()}` });
    agentName = agentData.name;
    await mqttClient.registerAgent(agentName, agentData);
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentName)).data; }
      catch { return null; }
    }, { label: 'agent ready' });
  });

  test('Dashboard command stored in message history with correct sender/receiver', async ({ apiClient }) => {
    const cmdText = `pipeline-cmd-${uniqueId()}`;
    const corrId = `corr-${Date.now()}`;

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      correlation_id: corrId,
      payload: { message: cmdText },
    });

    // Verify the command appears in message history
    const msg = await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentName, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => {
        const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
        return p.message === cmdText;
      });
    }, { label: 'command in message history', timeout: 10_000 });

    expect(msg).toBeTruthy();
    expect(msg.sender_id).toBe('dashboard');
    expect(msg.receiver_id).toBe(agentName);
    expect(msg.message_type).toBe('command');
    expect(msg.correlation_id).toBe(corrId);
  });

  test('Dashboard command reaches agent NATS inbox', async ({ mqttClient, apiClient }) => {
    await mqttClient.subscribe(`agents.${agentName}.inbox`);

    const cmdText = `inbox-delivery-${uniqueId()}`;
    const msgPromise = mqttClient.waitForMessage(
      `agents.${agentName}.inbox`,
      (msg) => (msg.content || msg.message || msg.payload?.message || '') === cmdText,
      10_000,
    );

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      payload: { message: cmdText },
    });

    const received = await msgPromise;
    expect(received).toBeTruthy();
  });

  test('Dashboard command auto-creates task with correlation_id', async ({ apiClient }) => {
    const cmdText = `task-auto-${uniqueId()}`;
    const corrId = `corr-task-${Date.now()}`;

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      correlation_id: corrId,
      payload: { message: cmdText },
    });

    // Verify a task was auto-created from the command
    const task = await pollUntil(async () => {
      const res = await apiClient.listTasks({ agent: agentName });
      const items = res.data.items || res.data;
      return items.find(t => t.id === corrId || t.title?.includes(cmdText));
    }, { label: 'auto-created task', timeout: 10_000 });

    expect(task).toBeTruthy();
    expect(task.status).toBe('running');
  });

  test('Agent reply stored in message history', async ({ mqttClient, apiClient }) => {
    const corrId = `corr-reply-${Date.now()}`;
    const replyText = `reply-from-agent-${uniqueId()}`;

    // Simulate agent reply (as mqtt-listener.js would do)
    await mqttClient.publish(`agents.${agentName}.outbox`, {
      from: agentName,
      to: 'dashboard',
      sender_id: agentName,
      receiver_id: 'dashboard',
      type: 'response',
      message_type: 'result',
      content: replyText,
      message: replyText,
      correlationId: corrId,
      correlation_id: corrId,
      timestamp: new Date().toISOString(),
    });

    // Verify reply is stored
    const msg = await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentName, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => {
        const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
        return p.content === replyText || p.message === replyText;
      });
    }, { label: 'reply in message history', timeout: 10_000 });

    expect(msg).toBeTruthy();
    expect(msg.sender_id).toBe(agentName);
    expect(msg.message_type).toBe('result');
    expect(msg.correlation_id).toBe(corrId);
  });

  test('Full round-trip: command → stored → reply → stored → task completed', async ({ mqttClient, apiClient }) => {
    const cmdText = `roundtrip-${uniqueId()}`;
    const corrId = `corr-rt-${Date.now()}`;

    // 1. Dashboard sends command
    const cmdRes = await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      correlation_id: corrId,
      payload: { message: cmdText },
    });
    expect(cmdRes.status).toBe(200);

    // 2. Verify command stored in history
    const storedCmd = await pollUntil(async () => {
      const res = await apiClient.listMessages({ correlation_id: corrId, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => m.message_type === 'command');
    }, { label: 'command stored', timeout: 10_000 });
    expect(storedCmd).toBeTruthy();
    expect(storedCmd.sender_id).toBe('dashboard');

    // 3. Agent replies (simulating mqtt-listener.js reply())
    const replyText = `Processed: ${cmdText}`;
    await mqttClient.publish(`agents.${agentName}.outbox`, {
      from: agentName,
      to: 'dashboard',
      sender_id: agentName,
      receiver_id: 'dashboard',
      type: 'response',
      message_type: 'result',
      content: replyText,
      message: replyText,
      correlationId: corrId,
      correlation_id: corrId,
      timestamp: new Date().toISOString(),
    });

    // 4. Verify reply stored in history
    const storedReply = await pollUntil(async () => {
      const res = await apiClient.listMessages({ correlation_id: corrId, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => m.message_type === 'result');
    }, { label: 'reply stored', timeout: 10_000 });
    expect(storedReply).toBeTruthy();
    expect(storedReply.sender_id).toBe(agentName);

    // 5. Verify task auto-completed
    const task = await pollUntil(async () => {
      const res = await apiClient.listTasks({ agent: agentName });
      const items = res.data.items || res.data;
      return items.find(t => t.id === corrId && t.status === 'completed');
    }, { label: 'task completed', timeout: 10_000 });
    expect(task).toBeTruthy();
    expect(task.status).toBe('completed');
  });

  test('Dashboard command does not create phantom dashboard agent', async ({ apiClient }) => {
    // Get baseline agent count
    const before = await apiClient.listAgents();
    const beforeIds = new Set((before.data.items || before.data).map(a => a.id));

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      payload: { message: `phantom-check-${uniqueId()}` },
    });
    await sleep(2000);

    // Only the target agent should exist, no new "dashboard"/"system"/"broadcast" agents
    const after = await apiClient.listAgents();
    const afterIds = (after.data.items || after.data).map(a => a.id);
    const newAgents = afterIds.filter(id => !beforeIds.has(id));
    const phantomIds = newAgents.filter(id => ['dashboard', 'system', 'mqtt-broker', 'broadcast'].includes(id));
    expect(phantomIds).toEqual([]);
  });

  test('Multiple commands to same agent all stored', async ({ apiClient }) => {
    const messages = [];
    for (let i = 0; i < 3; i++) {
      const cmdText = `multi-cmd-${i}-${uniqueId()}`;
      messages.push(cmdText);
      await apiClient.sendCommand(agentName, {
        message_type: 'command',
        sender_id: 'dashboard',
        receiver_id: agentName,
        correlation_id: `corr-multi-${i}-${Date.now()}`,
        payload: { message: cmdText },
      });
      await sleep(300);
    }

    // Verify all 3 commands are stored
    await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentName, limit: 50 });
      const items = res.data.items || res.data;
      const found = messages.filter(cmdText =>
        items.some(m => {
          const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
          return p.message === cmdText;
        })
      );
      return found.length === 3 ? true : null;
    }, { label: 'all 3 commands stored', timeout: 10_000 });
  });

  test('Command via UI stored in message history', async ({ apiClient, page, mqttClient }) => {
    await page.goto('/');
    await sleep(2000);

    // Select agent in sidebar
    await expect(page.locator(`text=${agentData.display_name}`).first()).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${agentData.display_name}`).first().click();
    await sleep(500);

    const cmdText = `ui-history-${uniqueId()}`;
    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    await commandInput.fill(cmdText);
    await commandInput.press('Enter');

    // Wait for toast confirmation
    await expect(page.locator('[role="status"]').first()).toBeVisible({ timeout: 10_000 });

    // Verify the command appears in API message history
    const msg = await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentName, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => {
        const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
        return p.message === cmdText;
      });
    }, { label: 'UI command in message history', timeout: 15_000 });

    expect(msg).toBeTruthy();
    expect(msg.sender_id).toBe('dashboard');
    expect(msg.receiver_id).toBe(agentName);
  });

  test('WebSocket broadcasts dashboard command', async ({ apiClient, wsClient }) => {
    const cmdText = `ws-broadcast-${uniqueId()}`;

    const eventPromise = wsClient.waitForEvent(
      (event) => {
        if (event.event !== 'message') return false;
        const data = event.data;
        if (!data) return false;
        const payload = typeof data.payload === 'string' ? JSON.parse(data.payload) : data.payload;
        return payload?.message === cmdText;
      },
      10_000,
    );

    await apiClient.sendCommand(agentName, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentName,
      payload: { message: cmdText },
    });

    const event = await eventPromise;
    expect(event).toBeTruthy();
    expect(event.data.sender_id).toBeDefined();
    expect(event.data.receiver_id).toBe(agentName);
  });
});
