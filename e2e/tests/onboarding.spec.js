/**
 * Onboarding E2E tests — validates the exact NATS protocol that
 * add-agent.sh + join.sh (mqtt-listener.js) implement.
 *
 * Any change to join.sh payloads, subjects, or the aggregator's
 * command routing must pass these tests.
 */
const { test, expect } = require('../helpers/fixtures');
const { uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

/**
 * Publish a registration message in the exact format mqtt-listener.js uses.
 * (See join.sh → nc.publish(`agents.${ID}.register`, ...))
 */
async function publishJoinRegister(mqttClient, agentId, opts = {}) {
  const payload = {
    agent_id: agentId,
    display_name: opts.display_name || agentId.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    role: opts.role || 'Agent',
    device_type: opts.device_type || 'server',
    capabilities: opts.capabilities || ['chat', 'task_execution', 'mqtt_listener'],
    ip_address: opts.ip_address || '10.0.0.42',
    status: 'online',
    timestamp: new Date().toISOString(),
  };
  await mqttClient.publish(`agents.${agentId}.register`, payload);
}

/**
 * Publish a heartbeat in the exact format mqtt-listener.js uses.
 * (See join.sh → function heartbeat())
 */
async function publishJoinHeartbeat(mqttClient, agentId, opts = {}) {
  const payload = {
    agent_id: agentId,
    display_name: opts.display_name || agentId.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
    role: opts.role || 'Agent',
    device_type: opts.device_type || 'server',
    status: 'online',
    cpu_percent: opts.cpu_percent ?? 23.5,
    memory_percent: opts.memory_percent ?? 41.2,
    ip_address: opts.ip_address || '10.0.0.42',
    capabilities: opts.capabilities || ['chat', 'task_execution', 'mqtt_listener'],
    timestamp: new Date().toISOString(),
  };
  await mqttClient.publish(`agents.${agentId}.heartbeat`, payload);
}

/**
 * Publish a reply in the exact format mqtt-listener.js reply() uses.
 */
async function publishJoinReply(mqttClient, fromAgent, toAgent, content, corrId) {
  const msg = {
    from: fromAgent,
    to: toAgent,
    sender_id: fromAgent,
    receiver_id: toAgent,
    type: 'response',
    message_type: 'result',
    content,
    message: content,
    correlationId: corrId || '',
    correlation_id: corrId || '',
    timestamp: new Date().toISOString(),
  };
  await mqttClient.publish(`agents.${fromAgent}.outbox`, msg);
}


test.describe('Agent Onboarding (add-agent + join)', () => {
  let agentId;

  test.beforeEach(() => {
    agentId = uniqueId('remote');
  });

  test('Registration with join.sh payload creates agent in DB', async ({ mqttClient, apiClient }) => {
    await publishJoinRegister(mqttClient, agentId, {
      role: 'Field Agent',
      device_type: 'cloud_vm',
    });

    const agent = await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent registered' });

    expect(agent.id).toBe(agentId);
    expect(agent.status).toBe('online');
    expect(agent.role).toBe('Field Agent');
    expect(agent.device_type).toBe('cloud_vm');
    expect(agent.ip_address).toBe('10.0.0.42');
  });

  test('Heartbeat with join.sh payload updates metrics', async ({ mqttClient, apiClient }) => {
    await publishJoinRegister(mqttClient, agentId);
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent ready' });

    await publishJoinHeartbeat(mqttClient, agentId, {
      cpu_percent: 87.3,
      memory_percent: 64.1,
    });
    await sleep(1000);

    const agent = (await apiClient.getAgent(agentId)).data;
    expect(agent.status).toBe('online');
    expect(agent.cpu_percent).toBeCloseTo(87.3, 0);
    expect(agent.memory_percent).toBeCloseTo(64.1, 0);
  });

  test('Dashboard command arrives on agents.{id}.inbox', async ({ mqttClient, apiClient }) => {
    // Register agent first
    await publishJoinRegister(mqttClient, agentId);
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent ready' });

    // Subscribe to the agent's inbox (simulating mqtt-listener.js)
    await mqttClient.subscribe(`agents.${agentId}.inbox`);

    const commandText = `test-command-${uniqueId()}`;

    const msgPromise = mqttClient.waitForMessage(
      `agents.${agentId}.inbox`,
      (msg) => {
        const content = msg.content || msg.message || msg.payload?.message || '';
        return content === commandText;
      },
      10_000,
    );

    // Send command from dashboard API
    await apiClient.sendCommand(agentId, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentId,
      correlation_id: `corr-${Date.now()}`,
      payload: { message: commandText },
    });

    const received = await msgPromise;
    const content = received.content || received.message || received.payload?.message || '';
    expect(content).toBe(commandText);
  });

  test('Agent reply appears in message history', async ({ mqttClient, apiClient }) => {
    // Register agent
    await publishJoinRegister(mqttClient, agentId);
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent ready' });

    const correlationId = `corr-reply-${Date.now()}`;
    const replyText = `Hello from ${agentId}`;

    // Simulate agent replying (exact mqtt-listener.js reply() format)
    await publishJoinReply(mqttClient, agentId, 'dashboard', replyText, correlationId);

    // Verify reply appears in messages API
    const result = await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentId, limit: 20 });
      const items = res.data.items || res.data;
      return items.find(m => {
        const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
        return (p.content === replyText || p.message === replyText);
      });
    }, { label: 'reply in messages', timeout: 10_000 });

    expect(result).toBeTruthy();
  });

  test('Full round-trip: register → command → reply', async ({ mqttClient, apiClient }) => {
    // 1. Agent registers (simulating join.sh startup)
    await publishJoinRegister(mqttClient, agentId, {
      role: 'Remote Worker',
      device_type: 'laptop',
    });
    await publishJoinHeartbeat(mqttClient, agentId);

    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent online' });

    // 2. Subscribe to inbox (simulating mqtt-listener.js)
    await mqttClient.subscribe(`agents.${agentId}.inbox`);

    const cmdText = `roundtrip-${uniqueId()}`;
    const cmdPromise = mqttClient.waitForMessage(
      `agents.${agentId}.inbox`,
      (msg) => (msg.content || msg.message || msg.payload?.message || '') === cmdText,
      10_000,
    );

    // 3. Dashboard sends command
    const cmdRes = await apiClient.sendCommand(agentId, {
      message_type: 'command',
      sender_id: 'dashboard',
      receiver_id: agentId,
      correlation_id: `corr-rt-${Date.now()}`,
      payload: { message: cmdText },
    });
    expect(cmdRes.status).toBe(200);

    // 4. Agent receives command
    const cmd = await cmdPromise;
    const corrId = cmd.correlationId || cmd.correlation_id || '';

    // 5. Agent replies (simulating mqtt-listener.js reply())
    const replyText = `Processed: ${cmdText}`;
    await publishJoinReply(mqttClient, agentId, 'dashboard', replyText, corrId);

    // 6. Reply stored in DB
    const stored = await pollUntil(async () => {
      const res = await apiClient.listMessages({ agent: agentId, limit: 50 });
      const items = res.data.items || res.data;
      return items.find(m => {
        const p = typeof m.payload === 'string' ? JSON.parse(m.payload) : m.payload;
        return (p.content === replyText || p.message === replyText);
      });
    }, { label: 'reply stored', timeout: 10_000 });
    expect(stored).toBeTruthy();
    expect(stored.message_type).toBe('result');
  });

  test('Agent appears in UI after registration', async ({ mqttClient, page }) => {
    const displayName = `Onboard-${uniqueId()}`;
    await publishJoinRegister(mqttClient, agentId, { display_name: displayName });
    await sleep(1000);

    await page.goto('/');
    await expect(page.locator(`text=${displayName}`).first()).toBeVisible({ timeout: 15_000 });
  });

  test('Command via UI reaches agent inbox', async ({ mqttClient, apiClient, page }) => {
    const displayName = `UICmd-${uniqueId()}`;
    await publishJoinRegister(mqttClient, agentId, { display_name: displayName });
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent ready' });

    await mqttClient.subscribe(`agents.${agentId}.inbox`);

    await page.goto('/');
    await sleep(2000);

    // Select agent in sidebar
    await expect(page.locator(`text=${displayName}`).first()).toBeVisible({ timeout: 15_000 });
    await page.locator(`text=${displayName}`).first().click();
    await sleep(500);

    const cmdText = `ui-cmd-${uniqueId()}`;
    const msgPromise = mqttClient.waitForMessage(
      `agents.${agentId}.inbox`,
      (msg) => (msg.content || msg.message || msg.payload?.message || '') === cmdText,
      10_000,
    );

    const commandInput = page.locator('input[placeholder*="command"], input[placeholder*="message"], textarea').last();
    await commandInput.fill(cmdText);
    await commandInput.press('Enter');

    const received = await msgPromise;
    expect(received).toBeTruthy();
  });

  test('Re-register (reconnect) updates agent back to online', async ({ mqttClient, apiClient }) => {
    // Register, then go offline, then re-register
    await publishJoinRegister(mqttClient, agentId);
    await pollUntil(async () => {
      try { return (await apiClient.getAgent(agentId)).data; }
      catch { return null; }
    }, { label: 'agent registered' });

    // Simulate offline status message (mqtt-listener.js will message)
    await mqttClient.publish(`agents.${agentId}.status`, {
      status: 'offline',
      timestamp: new Date().toISOString(),
    });

    await pollUntil(async () => {
      const res = await apiClient.getAgent(agentId);
      return res.data.status === 'offline' ? res.data : null;
    }, { label: 'agent offline', timeout: 10_000 });

    // Re-register (simulating service restart / reconnect)
    await publishJoinRegister(mqttClient, agentId);
    await sleep(1000);

    const updated = (await apiClient.getAgent(agentId)).data;
    expect(updated.status).toBe('online');
  });
});
