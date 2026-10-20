const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')
const {
  NATS_IMAGE,
  makeStackConfig,
  parsePublishedPort,
  scrubRunFiles,
  validateOwnedPaths,
  validateRunId,
  writeRunFiles,
} = require('./stack-config')

function config(root, runId = 'run-a001') {
  return makeStackConfig({
    runId,
    repoRoot: '/repo',
    scratchRoot: root,
    natsImage: NATS_IMAGE,
  })
}

test('builds the deterministic run configuration', () => {
  const value = config('/repo/tmp/e2e')
  assert.equal(value.project, 'edgecitadel-e2e-run-a001')
  assert.equal(value.runDir, '/repo/tmp/e2e/run-a001')
  assert.equal(value.controlDir, '/repo/tmp/e2e/run-a001/control')
  assert.equal(value.terminalReleaseDir, value.controlDir)
  assert.deepEqual(value.fixtureConfig, {
    run_id: 'run-a001', agent_id: 'shell-1', mode: 'edgecitadel', behavior: 'echo',
    delay_ms: 1000, crash_point: null, heartbeat_interval_ms: 1000,
    outcome_db: '/run/state/outcomes.sqlite3', side_effect_db: '/run/state/side-effects.sqlite3',
  })
})

test('accepts only bounded lowercase run IDs', () => {
  assert.equal(validateRunId('run-a001'), 'run-a001')
  for (const value of ['../escape', 'RUN-A001', 'run', 'run_a001']) {
    assert.throws(() => validateRunId(value), /invalid run ID/)
  }
})

test('pins the NATS image to the E2E stack digest', () => {
  assert.throws(() => makeStackConfig({ runId: 'run-a001', repoRoot: '/repo', scratchRoot: '/scratch', natsImage: 'nats:latest' }), /NATS image/)
})

test('parses only loopback-published ports', () => {
  assert.equal(parsePublishedPort('127.0.0.1:49152\n'), 49152)
  assert.equal(parsePublishedPort('[::1]:49153\n'), 49153)
  for (const value of ['0.0.0.0:49152\n', '127.0.0.1:0\n', '127.0.0.1:65536\n']) {
    assert.throws(() => parsePublishedPort(value))
  }
})

test('rejects duplicate or escaped owned paths', () => {
  assert.throws(() => validateOwnedPaths('/scratch/run-a001', ['/scratch/run-a001/a', '/scratch/run-a001/a']), /distinct/)
  assert.throws(() => validateOwnedPaths('/scratch/run-a001', ['/scratch/run-a001/a', '/tmp/outside']), /escaped/)
})

test('writes private fixture files and a private control directory', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-stack-config-'))
  t.after(() => fs.rm(root, { recursive: true, force: true }))
  const value = config(root)
  const files = await writeRunFiles(value, crypto.randomBytes)
  assert.match(files.token, /^[0-9a-f]{64}$/)
  assert.equal((await fs.stat(value.runDir)).mode & 0o777, 0o700)
  assert.equal((await fs.stat(value.controlDir)).mode & 0o777, 0o700)
  assert.equal((await fs.stat(value.credentialFile)).mode & 0o777, 0o600)
  assert.equal((await fs.stat(value.fixtureConfigFile)).mode & 0o777, 0o600)
})

test('writes the complete fixture configuration', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-stack-config-'))
  t.after(() => fs.rm(root, { recursive: true, force: true }))
  const value = config(root)
  await writeRunFiles(value, crypto.randomBytes)
  assert.deepEqual(JSON.parse(await fs.readFile(value.fixtureConfigFile, 'utf8')), value.fixtureConfig)
})

test('scrubs the credential and removes the entire run directory', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-stack-config-'))
  t.after(() => fs.rm(root, { recursive: true, force: true }))
  const value = config(root)
  await writeRunFiles(value, crypto.randomBytes)
  await scrubRunFiles(value)
  await assert.rejects(fs.stat(value.runDir), { code: 'ENOENT' })
})

test('refuses a control directory outside the run directory', () => {
  const value = config('/scratch')
  value.controlDir = '/tmp/control'
  assert.throws(() => validateOwnedPaths(value.runDir, [value.credentialFile, value.fixtureConfigFile, value.summaryFile, value.controlDir]), /escaped/)
})

test('run IDs cannot escape the scratch root through normalization', () => {
  assert.throws(() => makeStackConfig({ runId: '../outside', repoRoot: '/repo', scratchRoot: '/scratch', natsImage: NATS_IMAGE }), /invalid run ID/)
})

test('run file setup removes partial state after a write failure', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-stack-config-'))
  t.after(() => fs.rm(root, { recursive: true, force: true }))
  const value = config(root)
  const realWriteFile = fs.writeFile
  let calls = 0
  fs.writeFile = async (...args) => {
    calls += 1
    if (calls === 2) throw new Error('write fixture failed')
    return realWriteFile(...args)
  }
  try {
    await assert.rejects(writeRunFiles(value, crypto.randomBytes), /write fixture failed/)
  } finally {
    fs.writeFile = realWriteFile
  }
  await assert.rejects(fs.stat(value.runDir), { code: 'ENOENT' })
})
