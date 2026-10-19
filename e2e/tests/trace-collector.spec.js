const { test, expect } = require('@playwright/test');

test('collector status follows explicit startup opt-in', async ({ request }) => {
  const enabled = process.env.EDGECITADEL_TRACE_COLLECTOR === '1';
  await expect.poll(async () => {
    const response = await request.get(`${process.env.AGG_URL}/api/system/status`);
    expect(response.ok()).toBe(true);
    const status = await response.json();
    expect(status.nats_connected).toBe(true);
    if (enabled) {
      expect(status.telemetry.broker_backlog.state).toBe('available');
      expect(status.telemetry.broker_backlog.pending_delivery).toBeGreaterThanOrEqual(0);
      expect(status.telemetry.broker_backlog.awaiting_ack).toBeGreaterThanOrEqual(0);
      expect(status.telemetry.broker_backlog.sampled_at_ms).toBeGreaterThan(0);
      expect(['observed', 'clock_skew', 'unavailable']).toContain(status.telemetry.metrics.event_collection_age_state);
      if (status.telemetry.metrics.event_collection_age_state === 'observed') {
        expect(status.telemetry.metrics.last_event_collection_age_ms).toBeGreaterThanOrEqual(0);
      } else {
        expect(status.telemetry.metrics.last_event_collection_age_ms).toBeNull();
      }
      expect(status.telemetry.metrics.lifetime).toBe('service_instance');
      expect(status.telemetry.metrics.ack_successes).toBeGreaterThanOrEqual(0);
      expect(Object.keys(status.telemetry.metrics.commit_observations).sort()).toEqual(
        ['accepted', 'conflict', 'duplicate', 'quarantined', 'rejected']);
    }
    return { enabled: status.telemetry.enabled, state: status.telemetry.state };
  }, { timeout: 10000 }).toEqual({ enabled, state: enabled ? 'running' : 'disabled' });
});

test('administrator can stop collection while commands continue, then retry', async ({ request }) => {
  test.skip(process.env.EDGECITADEL_TRACE_COLLECTOR !== '1', 'collector opt-in required');
  const url = `${process.env.AGG_URL}/api/system/telemetry/control`;
  const headers = { 'X-EdgeCitadel-Admin-Token': process.env.EDGECITADEL_ADMIN_TOKEN };
  const before = (await (await request.get(`${process.env.AGG_URL}/api/system/status`)).json()).telemetry;
  expect((await request.post(url, { data: { action: 'stop' } })).status()).toBe(401);
  let didStop = false;
  try {
    const stopped = await request.post(url, { headers, data: { action: 'stop' } });
    expect(stopped.status()).toBe(200);
    didStop = true;
    expect((await stopped.json()).state).toBe('stopped');
    const command = await request.post(`${process.env.AGG_URL}/api/command/shell-1`, {
      data: { body: 'telemetry-stopped' },
    });
    expect(command.ok()).toBe(true);
    const { task_id } = await command.json();
    await expect.poll(async () => {
      const response = await request.get(`${process.env.AGG_URL}/api/messages?task_id=${task_id}&type=result`);
      const rows = await response.json();
      return rows[0]?.task_state;
    }, { timeout: 10000 }).toBe('completed');
    const status = await (await request.get(`${process.env.AGG_URL}/api/system/status`)).json();
    expect(status.nats_connected).toBe(true);
    expect(status.telemetry.state).toBe('stopped');
    expect(status.telemetry.broker_backlog).toEqual({ state: 'unavailable' });
  } finally {
    if (didStop) expect((await request.post(url, { headers, data: { action: 'retry' } })).status()).toBe(200);
  }
  await expect.poll(async () => {
    return (await (await request.get(`${process.env.AGG_URL}/api/system/status`)).json()).telemetry.state;
  }, { timeout: 10000 }).toBe('running');
  const after = (await (await request.get(`${process.env.AGG_URL}/api/system/status`)).json()).telemetry;
  expect(after.collector_epoch).toBe(before.collector_epoch);
});
