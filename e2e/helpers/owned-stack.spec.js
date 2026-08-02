const assert = require('node:assert/strict')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')
const { makeStackConfig, NATS_IMAGE } = require('./stack-config')
const { OwnedStack, normalizeRuntimeText, redactSecrets } = require('./owned-stack')

function config() {
  return Object.assign(makeStackConfig({
    runId: 'run-a001', repoRoot: '/repo', scratchRoot: path.join(os.tmpdir(), 'edgecitadel-owned-stack'), natsImage: NATS_IMAGE,
  }), {
    composeEnvironment: { NATS_TOKEN: 'secret-token' },
    secretValues: ['secret-token'],
  })
}

function readyResponse(value, ok = true) {
  return { ok, json: async () => value }
}

function readyFetch() {
  return async (url) => {
    if (url.includes('/api/system/status')) return readyResponse({ nats_connected: true, jetstream_stream_ok: true })
    if (url.includes('/api/registry')) return readyResponse([{
      agent_id: 'shell-1', agent_state: 'online', card: { metadata: {
        'runtime.kind': 'native', 'runtime.roles': ['worker'], 'runtime.conformance': 'L1',
      }, capabilities: { extensions: [{ uri: 'https://edgecitadel.local/ext/nats-binding/v1' }] } },
    }])
    return { ok: true, json: async () => ({}) }
  }
}

function imageRows(project) {
  return JSON.stringify([
    { Service: 'nats', Repository: 'nats', Tag: 'digest', ID: 'sha256:nats' },
    { Service: 'backend', Repository: `${project}-backend`, Tag: 'latest', ID: 'sha256:backend' },
    { Service: 'frontend', Repository: `${project}-frontend`, Tag: 'latest', ID: 'sha256:frontend' },
    { Service: 'fixture-agent', Repository: `${project}-fixture-agent`, Tag: 'latest', ID: 'sha256:fixture' },
  ])
}

function runner(calls, value = config()) {
  return async (command, args, options) => {
    calls.push({ command, args, options })
    if (args.includes('port')) {
      const service = args[args.indexOf('port') + 1]
      const containerPort = args[args.indexOf('port') + 2]
      const hostPort = service === 'nats' && containerPort === '8222'
        ? 41004
        : ({ frontend: 41001, backend: 41002, nats: 41003 }[service])
      return { code: 0, stdout: `127.0.0.1:${hostPort}\n`, stderr: '' }
    }
    if (args.includes('images')) return { code: 0, stdout: imageRows(value.project), stderr: '' }
    if (args.includes('config')) return { code: 0, stdout: `token=secret-token path=${value.runDir}\n`, stderr: '' }
    if (args.includes('inspect')) return { code: 1, stdout: '', stderr: '' }
    return { code: 0, stdout: '', stderr: '' }
  }
}

test('start uses a unique compose project, shell-free runner, and resolved ports', async () => {
  const value = config()
  const calls = []
  const stack = new OwnedStack({ config: value, runCommand: runner(calls, value), fetchImpl: readyFetch(), exit: () => {} })
  const ports = await stack.start()
  assert.deepEqual(ports, { app: 41001, api: 41002, nats: 41003, monitor: 41004 })
  assert.deepEqual(calls[0].args.slice(-4), ['up', '--build', '-d', '--wait'])
  assert.equal(calls[0].args[2], value.project)
  assert.equal(calls[0].options.shell, false)
})

test('rejects non-loopback ports returned by compose', async () => {
  const value = config()
  const stack = new OwnedStack({
    config: value,
    runCommand: async (command, args) => args.includes('port') ? { code: 0, stdout: '0.0.0.0:41001\n', stderr: '' } : { code: 0, stdout: '', stderr: '' },
    fetchImpl: readyFetch(), exit: () => {},
  })
  await assert.rejects(stack.resolvePort('frontend', 80), /non-loopback/)
})

test('requires project-owned build references but excludes the external NATS digest', async () => {
  const value = config()
  const stack = new OwnedStack({ config: value, runCommand: runner([], value), fetchImpl: readyFetch(), exit: () => {} })
  await stack.start()
  assert.equal(stack.allImages.find((image) => image.service === 'nats').reference, NATS_IMAGE)
  assert.deepEqual(stack.ownedBuildImages.map((image) => image.service).sort(), ['backend', 'fixture-agent', 'frontend'])
})

test('derives owned services when Compose omits its Service JSON field', async () => {
  const value = config()
  const stack = new OwnedStack({
    config: value,
    runCommand: async () => ({ code: 0, stdout: JSON.stringify([
      { Repository: `${value.project}-backend`, Tag: 'latest', ImageID: 'sha256:backend' },
      { Repository: 'nats', Tag: '', ImageID: 'sha256:nats' },
    ]), stderr: '' }),
    fetchImpl: readyFetch(), exit: () => {},
  })
  const images = await stack.readProjectImages()
  assert.deepEqual(images, [
    { service: 'backend', reference: `${value.project}-backend:latest`, image_id: 'sha256:backend' },
    { service: 'nats', reference: NATS_IMAGE, image_id: 'sha256:nats' },
  ])
})

test('fails startup if a project-owned build image is absent', async () => {
  const value = config()
  const stack = new OwnedStack({
    config: value,
    runCommand: async (command, args) => {
      if (args.includes('port')) return { code: 0, stdout: '127.0.0.1:41001\n', stderr: '' }
      if (args.includes('images')) return { code: 0, stdout: JSON.stringify(imageRows(value.project) && JSON.parse(imageRows(value.project)).slice(0, 3)), stderr: '' }
      return { code: 0, stdout: '', stderr: '' }
    }, fetchImpl: readyFetch(), exit: () => {},
  })
  await assert.rejects(stack.start(), /missing owned build image/)
})

test('cleanup runs compose down once and returns one cached promise', async () => {
  const value = config()
  const calls = []
  const stack = new OwnedStack({ config: value, runCommand: runner(calls, value), fetchImpl: readyFetch(), exit: () => {} })
  const first = stack.cleanup('test')
  const second = stack.cleanup('ignored')
  assert.strictEqual(first, second)
  const report = await first
  assert.equal(report.valid, true)
  assert.equal(calls.filter((call) => call.args.includes('down')).length, 1)
  assert.deepEqual(calls.find((call) => call.args.includes('down')).args.slice(-5), ['down', '-v', '--remove-orphans', '--rmi', 'local'])
})

test('surviving external images do not invalidate cleanup', async () => {
  const value = config()
  const calls = []
  const stack = new OwnedStack({ config: value, runCommand: runner(calls, value), fetchImpl: readyFetch(), exit: () => {} })
  stack.ownedBuildImages = []
  const report = await stack.cleanup('test')
  assert.equal(report.valid, true)
  assert.equal(calls.filter((call) => call.args.includes('inspect')).length, 0)
})

test('surviving project-owned image references invalidate cleanup', async () => {
  const value = config()
  const stack = new OwnedStack({
    config: value,
    runCommand: async (command, args) => {
      if (args.includes('inspect')) return { code: 0, stdout: 'still-present\n', stderr: '' }
      return { code: 0, stdout: '', stderr: '' }
    }, fetchImpl: readyFetch(), exit: () => {},
  })
  stack.ownedBuildImages = [{ service: 'backend', reference: `${value.project}-backend:latest`, image_id: 'sha256:backend' }]
  const report = await stack.cleanup('test')
  assert.equal(report.valid, false)
  assert.deepEqual(report.resources.owned_build_images, [`${value.project}-backend:latest`])
})

test('redacts generated tokens from reports and normalized runtime text', () => {
  const value = config()
  assert.equal(redactSecrets('secret-token failed', ['secret-token']), '<generated-per-run-token> failed')
  assert.equal(normalizeRuntimeText(`at ${value.runDir} in ${value.repoRoot}`, value), 'at <run-owned-path> in $SOURCE_ROOT')
})

test('signal handlers clean up before reporting the signal exit code', async () => {
  const value = config()
  const handlers = new Map()
  const exits = []
  const stack = new OwnedStack({ config: value, runCommand: runner([], value), fetchImpl: readyFetch(), exit: (code) => exits.push(code) })
  stack.installSignalHandlers({ once: (signal, callback) => handlers.set(signal, callback) })
  handlers.get('SIGTERM')()
  await new Promise((resolve) => setImmediate(resolve))
  await stack.cleanup('wait')
  assert.deepEqual(exits, [143])
})
