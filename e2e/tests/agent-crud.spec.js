const { test, expect } = require('../helpers/fixtures');
const { uniqueId } = require('../helpers/test-data');

test.describe('Agent CRUD API', () => {
  test('POST creates agent with 201 status', async ({ apiClient }) => {
    const id = uniqueId('crud-agent');
    const res = await apiClient.createAgent({
      id,
      display_name: `Agent ${id}`,
      role: 'worker',
      device_type: 'raspberry_pi',
    });
    expect(res.status).toBe(201);
    expect(res.data.id).toBe(id);
    expect(res.data.display_name).toBe(`Agent ${id}`);
  });

  test('POST duplicate agent returns 409', async ({ apiClient }) => {
    const id = uniqueId('dup-agent');
    await apiClient.createAgent({ id, display_name: id });

    try {
      await apiClient.createAgent({ id, display_name: id });
      expect(true).toBe(false); // Should not reach here
    } catch (err) {
      expect(err.response.status).toBe(409);
    }
  });

  test('GET list returns all agents', async ({ apiClient }) => {
    const id = uniqueId('list-agent');
    await apiClient.createAgent({ id, display_name: id });

    const res = await apiClient.listAgents();
    expect(res.status).toBe(200);
    expect(Array.isArray(res.data)).toBe(true);
    const found = res.data.find((a) => a.id === id);
    expect(found).toBeTruthy();
  });

  test('GET by ID returns correct agent', async ({ apiClient }) => {
    const id = uniqueId('get-agent');
    await apiClient.createAgent({ id, display_name: id, role: 'tester' });

    const res = await apiClient.getAgent(id);
    expect(res.status).toBe(200);
    expect(res.data.id).toBe(id);
    expect(res.data.role).toBe('tester');
  });

  test('GET nonexistent agent returns 404', async ({ apiClient }) => {
    try {
      await apiClient.getAgent('nonexistent-agent-xyz');
      expect(true).toBe(false);
    } catch (err) {
      expect(err.response.status).toBe(404);
    }
  });

  test('PATCH updates agent fields', async ({ apiClient }) => {
    const id = uniqueId('patch-agent');
    await apiClient.createAgent({ id, display_name: id, role: 'worker' });

    const res = await apiClient.updateAgent(id, {
      role: 'coordinator',
      display_name: 'Updated Name',
    });
    expect(res.status).toBe(200);
    expect(res.data.role).toBe('coordinator');
    expect(res.data.display_name).toBe('Updated Name');
  });

  test('DELETE removes agent with 204', async ({ apiClient }) => {
    const id = uniqueId('del-agent');
    await apiClient.createAgent({ id, display_name: id });

    const res = await apiClient.deleteAgent(id);
    expect(res.status).toBe(204);

    try {
      await apiClient.getAgent(id);
      expect(true).toBe(false);
    } catch (err) {
      expect(err.response.status).toBe(404);
    }
  });
});
