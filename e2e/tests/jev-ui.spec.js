const { test, expect } = require('@playwright/test')

// UI contract fixture; live TypeSafe/Hermes acceptance is recorded separately.
test('JEV submit, progress, interrupted result, resume and offline state', async ({ page }, testInfo) => {
  const run = '3034c407-c7a1-487a-8be2-37d05539b3fc'
  const task = 'baf8810c-0ea3-4f06-b189-bbcc6d37bfa2'
  const child = 'c36eb3c5-839d-41c4-beba-37d29f48bb0b'
  let online = true, messages = [], commands = []
  const message = (type, body, outcome, resumable) => ({ id: `${type}-${outcome}`, type,
    sender_id: 'jev', recipient_id: 'aggregator', task_id: task, timestamp: new Date().toISOString(),
    task_state: type === 'result' ? 'completed' : 'working',
    payload: { body, run_id: run, outcome, resumable,
      steps: [{ task_id: child, executor: 'jim-eq-hermes', observed_state: 'running' }] } })
  await page.route('**/api/**', async route => {
    const url = new URL(route.request().url())
    if (!url.pathname.startsWith('/api/')) return route.continue()
    let data = []
    if (url.pathname === '/api/agents') data = [{ agent_id: 'jev', card: { name: 'jev' }, agent_state: online ? 'online' : 'offline' }]
    if (url.pathname === '/api/system/status') data = { nats_connected: true, jetstream_stream_ok: true }
    if (url.pathname === '/api/messages') data = messages
    if (url.pathname === '/api/command/jev') {
      const command = route.request().postDataJSON()
      commands.push(command)
      data = { task_id: task }
      messages = [message('task.progress', '执行中：jim-eq-hermes', 'running', false)]
    }
    await route.fulfill({ json: data })
  })
  await page.goto('/')
  await page.getByLabel('Command body').fill('Explain gravity and review the answer')
  await page.getByRole('button', { name: '交给 JEV' }).click()
  await expect.poll(() => commands.length).toBe(1)
  expect(commands[0].skill_id).toBe('jev.run')
  expect(commands[0].args.request_id).toMatch(/^[0-9a-f-]{36}$/)
  await expect(page.getByText('执行中：jim-eq-hermes', { exact: true })).toBeVisible()
  messages = [message('result', '等待超时；未取消远端任务。', 'interrupted', true)]
  await expect(page.getByRole('button', { name: '继续', exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('jev-interrupted.png') })
  await page.getByRole('button', { name: '继续', exact: true }).click()
  await expect.poll(() => commands.length).toBe(2)
  expect(commands[1]).toMatchObject({ skill_id: 'jev.resume', args: { run_id: run } })
  messages = [message('result', 'Verified final answer', 'completed', false)]
  await expect(page.getByText('Verified final answer', { exact: true })).toBeVisible()
  online = false
  await page.reload()
  await expect(page.getByText('JEV offline', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '交给 JEV' })).toBeDisabled()
  await page.setViewportSize({ width: 390, height: 844 })
  await page.getByRole('button', { name: 'Open agent list', exact: true }).click()
  await page.getByRole('button', { name: 'All Agents', exact: true }).click()
  await expect.poll(async () => {
    const box = await page.getByRole('button', { name: 'All Agents', exact: true }).boundingBox()
    return box.x + box.width
  }).toBeLessThanOrEqual(0)
  await expect(page.getByRole('button', { name: '交给 JEV' })).toBeInViewport()
  await page.screenshot({ path: testInfo.outputPath('jev-mobile.png') })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
})
