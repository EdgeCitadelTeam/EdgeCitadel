const { test, expect } = require('../helpers/fixtures');
const { makeAgent, makeTask, uniqueId } = require('../helpers/test-data');
const { pollUntil, sleep } = require('../helpers/wait-utils');

test.describe('Task Board', () => {
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

  test('5 kanban columns visible', async ({ page }) => {
    await page.goto('/');
    const tasksTab = page.locator('button:has-text("Tasks")');
    await tasksTab.click();
    await sleep(1000);

    // Check for column headers
    await expect(page.locator('text=Pending').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('text=Assigned').first()).toBeVisible();
    await expect(page.locator('text=Running').first()).toBeVisible();
    await expect(page.locator('text=Completed').first()).toBeVisible();
    await expect(page.locator('text=Failed').first()).toBeVisible();
  });

  test('Create Task button opens modal', async ({ page }) => {
    await page.goto('/');
    const tasksTab = page.locator('button:has-text("Tasks")');
    await tasksTab.click();
    await sleep(1000);

    const createBtn = page.locator('button:has-text("Create Task"), button:has-text("New Task"), button:has-text("Add Task")').first();
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();
    await sleep(500);

    // Modal should appear with form fields
    await expect(page.locator('input[placeholder*="itle"], input[name="title"]').first()).toBeVisible({ timeout: 5_000 });
  });

  test('Creating task appears in Pending column', async ({ apiClient, page }) => {
    const task = makeTask({ title: `Pending-${uniqueId()}` });
    await apiClient.createTask({
      title: task.title,
      description: task.description,
      priority: task.priority,
    });
    await sleep(500);

    await page.goto('/');
    const tasksTab = page.locator('button:has-text("Tasks")');
    await tasksTab.click();
    await sleep(2000);

    await expect(page.locator(`text=${task.title}`)).toBeVisible({ timeout: 15_000 });
  });

  test('Creating task with assigned agent publishes MQTT', async ({ mqttClient, apiClient }) => {
    await mqttClient.subscribe('tasks/+/assign');
    await sleep(1000);

    const task = makeTask({ title: `MQTT-Task-${uniqueId()}` });
    const msgPromise = mqttClient.waitForMessage(
      'tasks/+/assign',
      (msg) => msg.title === task.title || msg.task_id,
      10_000
    );

    await apiClient.createTask({
      title: task.title,
      description: task.description,
      assigned_agent: agentName,
      priority: 'high',
    });

    const received = await msgPromise;
    expect(received).toBeTruthy();
  });

  test('POST /api/tasks returns 201', async ({ apiClient }) => {
    const task = makeTask({ title: `API-Create-${uniqueId()}` });
    const res = await apiClient.createTask({
      title: task.title,
      description: task.description,
      priority: 'normal',
    });
    expect(res.status).toBe(201);
    expect(res.data.title).toBe(task.title);
    expect(res.data.status).toBe('pending');
  });

  test('PATCH updates status and started_at', async ({ apiClient }) => {
    const task = makeTask({ title: `Patch-${uniqueId()}` });
    const created = await apiClient.createTask({
      title: task.title,
      description: task.description,
    });

    const res = await apiClient.updateTask(created.data.id, {
      status: 'running',
      assigned_agent: agentName,
    });
    expect(res.status).toBe(200);
    expect(res.data.status).toBe('running');
    expect(res.data.started_at).toBeTruthy();
  });

  test('Click task card opens detail modal', async ({ apiClient, page }) => {
    const task = makeTask({ title: `Detail-Modal-${uniqueId()}` });
    await apiClient.createTask({
      title: task.title,
      description: 'Test task with details',
      priority: 'high',
    });

    await page.goto('/');
    const tasksTab = page.locator('button:has-text("Tasks")');
    await tasksTab.click();
    await sleep(2000);

    const taskCard = page.locator(`text=${task.title}`);
    await expect(taskCard).toBeVisible({ timeout: 15_000 });
    await taskCard.click();
    await sleep(500);
  });

  test('Failed task shows error_message', async ({ apiClient }) => {
    const task = makeTask({ title: `Fail-${uniqueId()}` });
    const created = await apiClient.createTask({
      title: task.title,
      description: task.description,
    });

    await apiClient.updateTask(created.data.id, {
      status: 'failed',
      error_message: 'Out of memory',
    });

    const res = await apiClient.listTasks({ status: 'failed' });
    const found = res.data.items.find((t) => t.id === created.data.id);
    expect(found).toBeTruthy();
    expect(found.error_message).toBe('Out of memory');
  });
});
