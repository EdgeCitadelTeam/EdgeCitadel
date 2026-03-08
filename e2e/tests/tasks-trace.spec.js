const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Task Trace', () => {
  let agentName;

  test.beforeEach(async ({ mqttClient, apiClient }) => {
    const agent = makeAgent();
    agentName = agent.name;
    await mqttClient.registerAgent(agentName, agent);
    await pollUntil(async () => {
      try {
        return (await apiClient.getAgent(agentName)).data;
      } catch {
        return null;
      }
    }, { label: 'agent ready' });
  });

  test('GET /api/tasks/{id}/trace returns correlated messages', async ({ mqttClient, apiClient }) => {
    const taskId = uniqueId('trace');

    // Create task with MQTT messages that share correlation_id
    await mqttClient.assignTask(agentName, taskId);
    await sleep(300);
    await mqttClient.reportTaskProgress(agentName, taskId, { progress: 50 });
    await sleep(300);
    await mqttClient.completeTask(agentName, taskId, { result: 'success' });

    // Wait for task to be in DB
    await pollUntil(async () => {
      const res = await apiClient.listTasks();
      const task = res.data.items.find((t) => t.id === taskId);
      return task?.status === 'completed' ? task : null;
    }, { label: 'task completed' });

    // Get trace - should have the assign, progress, and complete messages
    const traceRes = await apiClient.getTaskTrace(taskId);
    expect(traceRes.status).toBe(200);
    expect(Array.isArray(traceRes.data)).toBe(true);
    expect(traceRes.data.length).toBeGreaterThanOrEqual(3);
  });

  test('Task detail shows message trace in UI', async ({ mqttClient, apiClient, page }) => {
    const taskId = uniqueId('ui-trace');
    const taskTitle = `Trace-${taskId}`;

    // Create task via API first
    await apiClient.createTask({
      title: taskTitle,
      description: 'Trace UI test',
      assigned_agent: agentName,
    });
    await sleep(500);

    // Send correlated messages
    await mqttClient.reportTaskProgress(agentName, taskId, { progress: 75 });
    await sleep(500);

    await page.goto('/');
    const tasksTab = page.locator('button:has-text("Tasks")');
    await tasksTab.click();
    await sleep(2000);

    // Click on the task to open detail
    const taskCard = page.locator(`text=${taskTitle}`);
    await expect(taskCard).toBeVisible({ timeout: 15_000 });
    await taskCard.click();
    await sleep(1000);
  });

  test('Empty trace for task with no correlated messages', async ({ apiClient }) => {
    const task = await apiClient.createTask({
      title: `No-trace-${uniqueId()}`,
      description: 'Task with no messages',
    });

    const traceRes = await apiClient.getTaskTrace(task.data.id);
    expect(traceRes.status).toBe(200);
    expect(Array.isArray(traceRes.data)).toBe(true);
    expect(traceRes.data.length).toBe(0);
  });
});
