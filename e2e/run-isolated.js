const crypto = require('node:crypto')
const fs = require('node:fs/promises')
const path = require('node:path')
const { NATS_IMAGE, makeStackConfig, scrubRunFiles, writeRunFiles } = require('./helpers/stack-config')
const { OwnedStack, runCommand } = require('./helpers/owned-stack')

function parseLauncherArgs(argv) {
  const options = {
    configPath: null, probeOnly: false, summaryFile: null, holdAfterReady: false,
    releaseFile: null, evidenceRuntimeDir: null, forwarded: [],
  }
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index]
    if (value === '--') {
      options.forwarded = argv.slice(index + 1)
      break
    }
    if (['--config', '--summary-file', '--release-file', '--evidence-runtime-dir'].includes(value)) {
      const argument = argv[index + 1]
      if (!argument) throw new Error(`${value} requires a value`)
      if (value === '--config') options.configPath = argument
      if (value === '--summary-file') options.summaryFile = argument
      if (value === '--release-file') options.releaseFile = argument
      if (value === '--evidence-runtime-dir') options.evidenceRuntimeDir = argument
      index += 1
    } else if (value === '--probe-only') {
      options.probeOnly = true
    } else if (value === '--hold-after-ready') {
      options.holdAfterReady = true
    } else {
      options.forwarded.push(value)
    }
  }
  if (!options.probeOnly && !options.configPath) {
    throw new Error('--config is required unless --probe-only is set')
  }
  if (options.holdAfterReady) {
    if (!options.probeOnly || !options.releaseFile) {
      throw new Error('--hold-after-ready requires --probe-only and --release-file')
    }
  } else if (options.releaseFile) {
    throw new Error('--release-file requires --hold-after-ready')
  }
  for (const [flag, value] of [
    ['--summary-file', options.summaryFile],
    ['--release-file', options.releaseFile],
    ['--evidence-runtime-dir', options.evidenceRuntimeDir],
  ]) {
    if (value && !path.isAbsolute(value)) throw new Error(`${flag} must be absolute`)
  }
  return options
}

async function waitForRelease(releaseFile) {
  for (;;) {
    try {
      await fs.access(releaseFile)
      return
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

async function main(argv, dependencies = {}) {
  const randomBytes = dependencies.randomBytes || crypto.randomBytes
  const createStack = dependencies.createStack || ((options) => new OwnedStack(options))
  const options = parseLauncherArgs(argv)
  const runId = ['run', Date.now().toString(36), randomBytes(6).toString('hex')].join('-')
  const repoRoot = path.resolve(__dirname, '..')
  const config = makeStackConfig({
    runId, repoRoot, scratchRoot: path.join(repoRoot, 'tmp/e2e'), natsImage: NATS_IMAGE,
  })
  let runFiles = null
  let stack = null
  let testExitCode = 0
  let cleanupReport = null
  try {
    runFiles = await writeRunFiles(config, randomBytes)
    config.composeEnvironment = runFiles.composeEnvironment
    config.secretValues = [runFiles.token]
    if (options.summaryFile) config.summaryFile = options.summaryFile
    if (options.evidenceRuntimeDir) {
      config.evidenceRuntimeDir = options.evidenceRuntimeDir
      config.evidenceDir = path.dirname(path.dirname(options.evidenceRuntimeDir))
    }
    stack = createStack({
      config, runCommand, fetchImpl: fetch, exit: (code) => process.exit(code),
    })
    stack.installSignalHandlers(process)
    await stack.start()
    await stack.collectRuntimeSummary(runFiles.token)
    if (options.holdAfterReady) {
      await waitForRelease(options.releaseFile)
    } else if (!options.probeOnly) {
      const result = await stack.runPlaywright(path.resolve(__dirname, options.configPath), options.forwarded)
      testExitCode = result.code
      if (result.stdout) process.stdout.write(result.stdout)
      if (result.stderr) process.stderr.write(result.stderr)
    }
  } finally {
    if (stack) {
      cleanupReport = await stack.cleanup('normal-exit')
    } else {
      await scrubRunFiles(config)
    }
  }
  return cleanupReport.valid ? testExitCode : 1
}

if (require.main === module) {
  void main(process.argv.slice(2)).then(
    (code) => { process.exitCode = code },
    (error) => { process.stderr.write(`${error.message}\n`); process.exitCode = 1 },
  )
}

module.exports = { main, parseLauncherArgs, waitForRelease }
