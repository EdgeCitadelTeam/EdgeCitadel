const crypto = require('node:crypto')
const fs = require('node:fs/promises')
const path = require('node:path')
const { test, expect } = require('@playwright/test')
const { canonicalJson, pollJson, requireEnvironment } = require('../helpers/operator-journey')
const { captureProjectEvidence } = require('../helpers/evidence-artifacts')

const AGG_URL = requireEnvironment('AGG_URL')
const TERMINAL_RELEASE_DIR = requireEnvironment('E2E_TERMINAL_RELEASE_DIR')
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

test('operator observes one deterministic task lifecycle', async ({ page, request }, testInfo) => {
  const errors = []
  page.on('console', (message) => { if (message.type() === 'error') errors.push(`console:${message.text()}`) })
  page.on('pageerror', (error) => errors.push(`pageerror:${error.message}`))
  page.on('requestfailed', (failed) => errors.push(`requestfailed:${failed.method()}:${failed.url()}`))
  page.on('response', (response) => {
    if (response.status() >= 400 && new URL(response.url()).pathname !== '/favicon.ico') errors.push(`http:${response.status()}:${response.url()}`)
  })

  const health = await request.get(`${AGG_URL}/api/system/status`)
  expect(await health.json()).toEqual(expect.objectContaining({ nats_connected: true, jetstream_stream_ok: true }))
  const registry = await pollJson(request, `${AGG_URL}/api/registry`, (rows) => rows.some((row) => row.agent_id === 'shell-1'))
  const shell = registry.find((row) => row.agent_id === 'shell-1')
  expect(shell).toEqual(expect.objectContaining({ agent_state: 'online' }))
  expect(shell.card.metadata['runtime.conformance']).toBe('L1')

  await page.goto('/')
  const agent = page.locator('[data-agent-id="shell-1"]')
  if (testInfo.project.name === 'mobile') {
    await page.getByRole('button', { name: 'Open agent list' }).click()
  }
  await agent.click()
  await expect(agent).toHaveAttribute('aria-pressed', 'true')
  await expect(page.getByLabel('Command target')).toHaveValue('shell-1')
  await expect(page.getByRole('status', { name: 'Selected agent shell-1: online' })).toBeVisible()

  const nonce = crypto.randomUUID()
  const holdPath = path.join(TERMINAL_RELEASE_DIR, `${nonce}.hold`)
  await fs.writeFile(holdPath, 'hold\n', { encoding: 'utf8', flag: 'wx', mode: 0o600 })
  const acceptedResponse = page.waitForResponse((response) => response.request().method() === 'POST' && new URL(response.url()).pathname === '/api/command/shell-1')
  await page.getByLabel('Command body').fill(nonce)
  await page.getByRole('button', { name: 'Send command' }).click()
  const accepted = await (await acceptedResponse).json()
  expect(accepted.task_id).toMatch(UUID_V4)
  const taskId = accepted.task_id

  await page.getByRole('button', { name: /^Tasks/ }).click()
  await expect(page.locator(`[data-task-id="${taskId}"][data-task-state="submitted"], [data-task-id="${taskId}"][data-task-state="working"]`)).toBeVisible()
  const releasePath = path.join(TERMINAL_RELEASE_DIR, `${taskId}.release`)
  await fs.writeFile(releasePath, 'release\n', { encoding: 'utf8', flag: 'wx', mode: 0o600 })
  await expect(page.locator(`[data-task-id="${taskId}"][data-task-state="completed"]`)).toBeVisible({ timeout: 15_000 })
  await Promise.all([fs.rm(holdPath, { force: true }), fs.rm(releasePath, { force: true })])

  await page.getByRole('button', { name: /^Chat/ }).click()
  await expect(page.locator(`[data-task-id="${taskId}"][data-message-type="command"]`)).toContainText(nonce)
  await expect(page.locator(`[data-task-id="${taskId}"][data-message-type="result"][data-task-state="completed"]`)).toContainText(`edgecitadel:${nonce}`)
  const messages = await pollJson(request, `${AGG_URL}/api/messages?task_id=${taskId}`, (rows) => rows.some((row) => row.type === 'result' && row.task_state === 'completed'))
  const command = messages.find((row) => row.type === 'command')
  const terminals = messages.filter((row) => row.type === 'result' && row.task_state === 'completed')
  const progress = messages.filter((row) => row.type === 'task.progress')
  expect(command.context_id).toMatch(UUID_V4)
  expect(command.hop_count).toBe(0)
  expect(terminals).toHaveLength(1)
  expect(terminals[0].payload.body).toBe(`edgecitadel:${nonce}`)
  expect(new Set(terminals.map((row) => `${row.task_state}:${canonicalJson(row.payload)}`)).size).toBe(1)
  for (const row of progress.concat(terminals)) {
    expect(row.context_id).toBe(command.context_id)
    expect(row.hop_count).toBe(0)
  }
  await pollJson(request, `${AGG_URL}/api/agents/shell-1/queue`, (queue) => queue.pending === 0 && queue.ack_pending === 0)
  const metadata = {
    project: testInfo.project.name,
    task_id: taskId,
    nonce,
    command_body: nonce,
    expected_output: `edgecitadel:${nonce}`,
    context_id: command.context_id,
    hop_count: command.hop_count,
    command_envelope_id: command.id,
    terminal_envelope_id: terminals[0].id,
    progress_envelope_ids: progress.map((row) => row.id),
    command_sender_id: command.sender_id,
    command_recipient_id: command.recipient_id,
    terminal_sender_id: terminals[0].sender_id,
    terminal_recipient_id: terminals[0].recipient_id,
    browser_name: 'chromium',
    browser_version: page.context().browser().version(),
    command_observation_index: command.observation_index,
    progress_observation_indices: progress.map((row) => row.observation_index),
    terminal_observation_index: terminals[0].observation_index,
  }
  if (process.env.EVIDENCE_DIR) {
    await captureProjectEvidence({ page, request, testInfo, aggUrl: AGG_URL, metadata })
  } else {
    await testInfo.attach('operator-metadata', { body: `${canonicalJson(metadata)}\n`, contentType: 'application/json' })
  }
  expect(errors).toEqual([])
})
