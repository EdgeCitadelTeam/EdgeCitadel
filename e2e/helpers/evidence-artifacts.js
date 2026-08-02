const fs = require('node:fs/promises')
const path = require('node:path')
const { expect } = require('@playwright/test')
const { assertInViewport, assertNoOverlap, canonicalJson } = require('./operator-journey')

async function writeCanonical(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true })
  await fs.writeFile(filePath, `${canonicalJson(value)}\n`, 'utf8')
}

async function readJson(response, label) {
  if (!response.ok()) throw new Error(`${label} returned ${response.status()}`)
  return response.json()
}

async function captureProjectEvidence({ page, request, testInfo, aggUrl, metadata }) {
  const project = testInfo.project.name
  if (!['desktop', 'mobile'].includes(project)) throw new Error(`unsupported evidence project ${project}`)
  const evidenceDir = process.env.EVIDENCE_DIR
  if (!evidenceDir) throw new Error('EVIDENCE_DIR is required')
  if (metadata.project !== project) throw new Error('journey metadata project does not match Playwright')
  const projectDir = path.join(evidenceDir, 'raw', 'playwright', project)
  const apiDir = path.join(evidenceDir, 'raw', 'api', project)
  const taskId = metadata.task_id
  const command = page.locator(`[data-task-id="${taskId}"][data-message-type="command"]`)
  const result = page.locator(`[data-task-id="${taskId}"][data-message-type="result"][data-task-state="completed"]`)
  const target = page.getByLabel('Command target')
  const status = page.getByRole('status', { name: 'Selected agent shell-1: online' })
  const tracking = page.getByText(`Tracking task: ${taskId}`, { exact: true })
  await expect(target).toHaveValue('shell-1')
  await expect(command).toContainText(metadata.nonce)
  await expect(result).toContainText(metadata.expected_output)
  await assertNoOverlap([target, status, command, tracking, result])
  const chatPath = path.join(projectDir, 'chat.png')
  await fs.mkdir(projectDir, { recursive: true })
  await page.screenshot({ path: chatPath, fullPage: false })
  await page.getByRole('button', { name: /^Tasks/ }).click()
  const taskCard = page.locator(`[data-task-id="${taskId}"][data-task-state="completed"]`)
  await expect(taskCard).toBeVisible()
  await assertInViewport(taskCard)
  const tasksPath = path.join(projectDir, 'tasks.png')
  await page.screenshot({ path: tasksPath, fullPage: false })
  const snapshots = {
    'system-status.json': await readJson(await request.get(`${aggUrl}/api/system/status`), 'system status'),
    'registry.json': await readJson(await request.get(`${aggUrl}/api/registry`), 'registry'),
    'messages.json': await readJson(await request.get(`${aggUrl}/api/messages?task_id=${taskId}`), 'messages'),
    'queue.json': await readJson(await request.get(`${aggUrl}/api/agents/shell-1/queue`), 'queue'),
  }
  for (const [name, value] of Object.entries(snapshots)) await writeCanonical(path.join(apiDir, name), value)
  const metadataPath = path.join(projectDir, 'operator-metadata.json')
  await writeCanonical(metadataPath, Object.assign({}, metadata, {
    api_directory: path.relative(evidenceDir, apiDir),
    chat_screenshot: path.relative(evidenceDir, chatPath),
    tasks_screenshot: path.relative(evidenceDir, tasksPath),
  }))
  await Promise.all([
    testInfo.attach('chat', { path: chatPath, contentType: 'image/png' }),
    testInfo.attach('tasks', { path: tasksPath, contentType: 'image/png' }),
    testInfo.attach('operator-metadata', { path: metadataPath, contentType: 'application/json' }),
  ])
}

module.exports = { captureProjectEvidence }
