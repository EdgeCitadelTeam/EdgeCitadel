const fs = require('node:fs/promises')
const path = require('node:path')

const NATS_IMAGE = 'nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927'

function validateRunId(runId) {
  if (typeof runId !== 'string' || !/^[a-z0-9][a-z0-9-]{7,63}$/.test(runId)) {
    throw new Error(`invalid run ID: ${runId}`)
  }
  return runId
}

function validateOwnedPaths(runDir, ownedPaths) {
  const root = path.resolve(runDir)
  const resolved = ownedPaths.map((value) => path.resolve(value))
  if (new Set(resolved).size !== resolved.length) {
    throw new Error('owned run paths must be distinct')
  }
  if (resolved.some((value) => path.relative(root, value).startsWith('..') || path.isAbsolute(path.relative(root, value)))) {
    throw new Error('owned path escaped run directory')
  }
  return resolved
}

function makeStackConfig({ runId, repoRoot, scratchRoot, natsImage }) {
  validateRunId(runId)
  if (natsImage !== NATS_IMAGE) {
    throw new Error('NATS image must match the Slice 1 toolchain digest')
  }
  const scratch = path.resolve(scratchRoot)
  const runDir = path.resolve(scratch, runId)
  if (path.relative(scratch, runDir).startsWith('..')) {
    throw new Error('run directory escaped scratch root')
  }
  const credentialFile = path.join(runDir, 'transport-token')
  const fixtureConfigFile = path.join(runDir, 'native-control.json')
  const summaryFile = path.join(runDir, 'launcher-summary.json')
  const controlDir = path.join(runDir, 'control')
  validateOwnedPaths(runDir, [
    credentialFile,
    fixtureConfigFile,
    summaryFile,
    controlDir,
  ])
  return {
    runId,
    project: `edgecitadel-e2e-${runId}`,
    natsImage,
    repoRoot: path.resolve(repoRoot),
    runDir,
    composeFile: path.join(path.resolve(repoRoot), 'e2e/docker-compose.test.yml'),
    credentialFile,
    fixtureConfigFile,
    summaryFile,
    controlDir,
    terminalReleaseDir: controlDir,
    fixtureConfig: {
      run_id: runId,
      agent_id: 'shell-1',
      mode: 'edgecitadel',
      behavior: 'echo',
      delay_ms: 1000,
      crash_point: null,
      heartbeat_interval_ms: 1000,
      outcome_db: '/run/state/outcomes.sqlite3',
      side_effect_db: '/run/state/side-effects.sqlite3',
    },
  }
}

function parsePublishedPort(output) {
  const text = String(output).trim()
  const match = text.match(/^(?:127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})$/)
  if (!match) throw new Error(`non-loopback published port: ${text}`)
  const port = Number(match[1])
  if (port > 65535) throw new Error(`invalid published port: ${port}`)
  return port
}

async function scrubRunFiles(config) {
  let firstError = null
  let handle
  try {
    handle = await fs.open(config.credentialFile, 'r+')
    const { size } = await handle.stat()
    if (size > 0) {
      const zeros = Buffer.alloc(size)
      let offset = 0
      while (offset < zeros.length) {
        const { bytesWritten } = await handle.write(zeros, offset, zeros.length - offset, offset)
        if (bytesWritten === 0) throw new Error('credential overwrite made no progress')
        offset += bytesWritten
      }
      await handle.sync()
    }
  } catch (error) {
    if (error.code !== 'ENOENT') firstError = error
  } finally {
    if (handle) {
      try {
        await handle.close()
      } catch (error) {
        firstError ||= error
      }
    }
    for (const remove of [
      () => fs.rm(config.credentialFile, { force: true }),
      () => fs.rm(config.runDir, { recursive: true, force: true }),
    ]) {
      try {
        await remove()
      } catch (error) {
        firstError ||= error
      }
    }
  }
  if (firstError) throw firstError
}

function redactToken(value, token) {
  return String(value).split(token).join('<generated-per-run-token>')
}

async function writeRunFiles(config, randomBytes) {
  const entropy = randomBytes(32).toString('hex')
  const token = `a${entropy.slice(1)}`
  try {
    await fs.mkdir(path.dirname(config.runDir), { recursive: true, mode: 0o700 })
    await fs.mkdir(config.runDir, { mode: 0o700 })
    await fs.chmod(config.runDir, 0o700)
    await fs.mkdir(config.controlDir, { mode: 0o700 })
    await fs.chmod(config.controlDir, 0o700)
    await fs.writeFile(config.credentialFile, `${token}\n`, { mode: 0o600 })
    await fs.chmod(config.credentialFile, 0o600)
    await fs.writeFile(config.fixtureConfigFile, `${JSON.stringify(config.fixtureConfig, null, 2)}\n`, { mode: 0o600 })
    await fs.chmod(config.fixtureConfigFile, 0o600)
    return {
      token,
      composeEnvironment: {
        E2E_CREDENTIAL_FILE: config.credentialFile,
        E2E_FIXTURE_CONFIG: config.fixtureConfigFile,
        E2E_CONTROL_DIR: config.controlDir,
        NATS_IMAGE: config.natsImage,
        NATS_TOKEN: token,
      },
    }
  } catch (error) {
    let scrubError = null
    try {
      await scrubRunFiles(config)
    } catch (caught) {
      scrubError = caught
    }
    const setup = redactToken(error.stack || error.message, token)
    const scrub = scrubError && redactToken(scrubError.stack || scrubError.message, token)
    throw new Error(scrub ? `${setup}; setup scrub failed: ${scrub}` : setup)
  }
}

module.exports = {
  NATS_IMAGE,
  makeStackConfig,
  parsePublishedPort,
  scrubRunFiles,
  validateOwnedPaths,
  validateRunId,
  writeRunFiles,
}
