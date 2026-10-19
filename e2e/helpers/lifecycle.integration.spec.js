const assert = require('node:assert/strict')
const { spawn } = require('node:child_process')
const fs = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')

const e2eRoot = path.resolve(__dirname, '..')

async function waitForJson(filePath, predicate, timeoutMs = 180_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const value = JSON.parse(await fs.readFile(filePath, 'utf8'))
      if (predicate(value)) return value
    } catch (error) {
      if (!['ENOENT', 'EACCES'].includes(error.code) && !(error instanceof SyntaxError)) throw error
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${filePath}`)
}

function startHeldProbe(root, name) {
  const summaryFile = path.join(root, `${name}-summary.json`)
  const releaseFile = path.join(root, `${name}-release`)
  const child = spawn(process.execPath, [
    'run-isolated.js', '--probe-only', '--hold-after-ready', '--release-file', releaseFile,
    '--summary-file', summaryFile,
  ], { cwd: e2eRoot, shell: false, stdio: ['ignore', 'pipe', 'pipe'] })
  let stderr = ''
  child.stderr.on('data', (chunk) => { stderr += chunk.toString() })
  const exited = new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('exit', (code, signal) => resolve({ code, signal, stderr }))
  })
  return { child, exited, releaseFile, summaryFile }
}

async function waitForExit(probe, timeoutMs) {
  let timer
  try {
    return await Promise.race([
      probe.exited,
      new Promise((resolve) => { timer = setTimeout(() => resolve(null), timeoutMs) }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

async function stopHeldProbe(probe) {
  if (probe.child.exitCode !== null || probe.child.signalCode !== null) return probe.exited
  await fs.writeFile(probe.releaseFile, 'release\n', { mode: 0o600, flag: 'a' })
  let result = await waitForExit(probe, 15_000)
  if (result) return result
  probe.child.kill('SIGTERM')
  result = await waitForExit(probe, 10_000)
  if (result) return result
  probe.child.kill('SIGKILL')
  return probe.exited
}

function assertClean(summary) {
  assert.equal(summary.cleanup.valid, true)
  for (const resources of Object.values(summary.cleanup.resources)) assert.deepEqual(resources, [])
}

function hasLiveUrls(value) {
  return /^http:\/\/127\.0\.0\.1:[1-9][0-9]*$/.test(value.urls?.AGG_URL || '')
}

test('concurrent held probes isolate ports and database state', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-concurrent-'))
  const probes = []
  t.after(async () => {
    try {
      await Promise.allSettled(probes.map(stopHeldProbe))
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  })
  const left = startHeldProbe(root, 'left')
  probes.push(left)
  const right = startHeldProbe(root, 'right')
  probes.push(right)
  const [leftReady, rightReady] = await Promise.all([
    waitForJson(left.summaryFile, hasLiveUrls),
    waitForJson(right.summaryFile, hasLiveUrls),
  ])
  assert.notEqual(leftReady.run_id, rightReady.run_id)
  assert.notEqual(leftReady.project, rightReady.project)
  for (const key of ['APP_URL', 'AGG_URL', 'NATS_URL', 'MONITOR_URL']) {
    assert.notEqual(leftReady.urls[key], rightReady.urls[key])
  }
  const [leftRegistry, rightRegistry] = await Promise.all([
    fetch(`${leftReady.urls.AGG_URL}/api/registry`).then((response) => response.json()),
    fetch(`${rightReady.urls.AGG_URL}/api/registry`).then((response) => response.json()),
  ])
  assert.deepEqual(leftRegistry.map((row) => row.agent_id), ['shell-1'])
  assert.deepEqual(rightRegistry.map((row) => row.agent_id), ['shell-1'])
  await Promise.all([
    fs.writeFile(left.releaseFile, 'release\n', { mode: 0o600 }),
    fs.writeFile(right.releaseFile, 'release\n', { mode: 0o600 }),
  ])
  const [leftExit, rightExit] = await Promise.all([left.exited, right.exited])
  assert.deepEqual([leftExit.code, rightExit.code], [0, 0], `${leftExit.stderr}\n${rightExit.stderr}`)
  assertClean(await waitForJson(left.summaryFile, (value) => value.cleanup))
  assertClean(await waitForJson(right.summaryFile, (value) => value.cleanup))
})

test('SIGTERM cleans a held probe before exit 143', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-signal-'))
  let probe
  t.after(async () => {
    try {
      if (probe) await stopHeldProbe(probe)
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  })
  probe = startHeldProbe(root, 'signal')
  await waitForJson(probe.summaryFile, hasLiveUrls)
  probe.child.kill('SIGTERM')
  const result = await probe.exited
  assert.equal(result.code, 143, result.stderr)
  const completed = await waitForJson(probe.summaryFile, (value) => value.cleanup)
  assert.equal(completed.cleanup.reason, 'SIGTERM')
  assertClean(completed)
})
