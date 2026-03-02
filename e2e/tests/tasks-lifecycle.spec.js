const { test, expect } = require('../helpers/fixtures');
const { makeAgent, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Task Lifecycle', () => {
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

  test('Full lifecycle: create -> assign -> progress -> complete', async ({ mqttClient, apiClient }) => {
    const taskId = uniqueId('lifecycle');

    // 1. Assign task via MQTT
    await mqttClient.assignTask(agentName, taskId, { title: 'Lifecycle Test' });
    await pollUntil(async () => {
      try {
        const res = await apiClient.listTasks();
        return res.data.items.find((t) => t.id === taskId) || null;
      } catch {
        return null;
      }
    }, { label: 'task created via MQTT' });

    // 2. Report progress
    await mqttClient.reportTaskProgress(agentName, taskId, { progress: 50 });
    await pollUntil(async () => {
      const res = await apiClient.listTasks();
      const task = res.data.items.find((t) => t.id === taskId);
      return task?.status === 'running' ? task : null;
    }, { label: 'task running' });

    // 3. Complete
    await mqttClient.completeTask(agentName, taskId, { output: 'done' });
    const completed = await pollUntil(async () => {
      const res = await apiClient.listTasks();
      const task = res.data.items.find((t) => t.id === taskId);
      return task?.status === 'completed' ? task : null;
    }, { label: 'task completed' });

    expect(completed.status).toBe('completed');
    expect(completed.completed_at).toBeTruthy();
  });

  test('Failure path: create -> assign -> progress -> failed', async ({ mqttClient, apiClient }) => {
    const taskId = uniqueId('fail-lifecycle');

    // 1. Assign
    await mqttClient.assignTask(agentName, taskId, { title: 'Fail Test' });
    await pollUntil(async () => {
      try {
        const res = await apiClient.listTasks();
        return res.data.items.find((t) => t.id === taskId) || null;
      } catch {
        return null;
      }
    }, { label: 'task created' });

    // 2. Progress
    await mqttClient.reportTaskProgress(agentName, taskId, { progress: 30 });
    await sleep(500);

    // 3. Fail
    await mqttClient.failTask(agentName, taskId, 'Segmentation fault');
    const failed = await pollUntil(async () => {
      const res = await apiClient.listTasks();
      const task = res.data.items.find((t) => t.id === taskId);
      return task?.status === 'failed' ? task : null;
    }, { label: 'task failed' });

    expect(failed.status).toBe('failed');
    expect(failed.error_message).toBe('Segmentation fault');
  });

  test('Lifecycle updates reflected in kanban columns', async ({ mqttClient, apiClient, page }) => {
    const taskId = uniqueId('kanban-lifecycle');
    const taskTitle = `Kanban-${taskId}`;

    // Create task via API
    await apiClient.createTask({
      title: taskTitle,
      description: 'Kanban lifecycle test',
      assigned_agent: agentName,
      priority: 'high',
    });
    await sleep(500);

    // Navigate to tasks
    await page.goto('/');
    const tasksTab = page.locator('text=Tasks').first();
    await tasksTab.click();
    await sleep(2000);

    // Task should be visible
    await expect(page.locator(`text=${taskTitle}`)).toBeVisible({ timeout: 15_000 });
  });

  test('MQTT assign for nonexistent task creates it', async ({ mqttClient, apiClient }) => {
    const taskId = uniqueId('auto-create');

    await mqttClient.assignTask(agentName, taskId, {
      title: 'Auto-created task',
      description: 'Created via MQTT assign',
    });

    const result = await pollUntil(async () => {
      try {
        const res = await apiClient.listTasks();
        return res.data.items.find((t) => t.id === taskId) || null;
      } catch {
        return null;
      }
    }, { label: 'task auto-created' });

    expect(result.id).toBe(taskId);
    expect(result.assigned_agent).toBe(agentName);
  });
});
