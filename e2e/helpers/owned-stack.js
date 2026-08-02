const { spawn } = require('node:child_process')
const fs = require('node:fs/promises')
const path = require('node:path')
const { parsePublishedPort, scrubRunFiles } = require('./stack-config')

const OWNED_BUILD_SERVICES = new Set(['backend', 'frontend', 'fixture-agent'])
const SECRET_MARKER = '<generated-per-run-token>'
const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds))

function redactSecrets(value, secrets = []) {
  return secrets.reduce((text, secret) => secret ? text.split(secret).join(SECRET_MARKER) : text, String(value))
}

function normalizeRuntimeText(value, config) {
  const replacements = [
    [config.evidenceRuntimeDir, '$EVIDENCE_DIR/raw/runtime'],
    [config.fixtureConfigFile, '<fixture-config>'],
    [config.credentialFile, '<credential-file>'],
    [config.controlDir, '<control-dir>'],
    [config.repoRoot, '$SOURCE_ROOT'],
    [config.runDir, '<run-owned-path>'],
  ].filter(([source]) => source).sort((left, right) => right[0].length - left[0].length)
  return replacements.reduce((text, [source, replacement]) => text.split(source).join(replacement), String(value))
}

function runCommand(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      shell: false,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => { stdout += chunk.toString() })
    child.stderr.on('data', (chunk) => { stderr += chunk.toString() })
    child.once('error', (error) => reject(new Error(redactSecrets(error.message, options.redactions))))
    child.once('close', (code) => {
      const result = {
        code: code ?? 1,
        stdout: redactSecrets(stdout, options.redactions),
        stderr: redactSecrets(stderr, options.redactions),
      }
      if (result.code !== 0 && !options.allowFailure) {
        reject(new Error(`${command} exited ${result.code}: ${result.stderr.trim()}`))
      } else {
        resolve(result)
      }
    })
  })
}

class OwnedStack {
  constructor({ config, runCommand: commandRunner, fetchImpl, exit }) {
    this.config = config
    this.runCommand = commandRunner
    this.fetch = fetchImpl
    this.exit = exit
    this.ports = null
    this.allImages = []
    this.ownedBuildImages = []
    this.runtimeSummary = null
    this.cleanupPromise = null
    this.startedAt = new Date().toISOString()
  }

  composeArgs(args) {
    return ['compose', '-p', this.config.project, '-f', this.config.composeFile].concat(args)
  }

  docker(args, allowFailure = false) {
    return this.runCommand('docker', this.composeArgs(args), {
      cwd: this.config.repoRoot,
      env: Object.assign({}, process.env, this.config.composeEnvironment),
      shell: false,
      allowFailure,
      redactions: this.config.secretValues,
    })
  }

  async start() {
    try {
      await this.docker(['up', '--build', '-d', '--wait'])
    } catch (error) {
      const logs = await this.docker(['logs', '--no-color'], true)
      throw new Error(`${error.message}\n${logs.stdout}${logs.stderr}`)
    }
    this.ports = await this.resolvePorts()
    this.allImages = await this.readProjectImages()
    this.ownedBuildImages = this.allImages.filter((image) => (
      OWNED_BUILD_SERVICES.has(image.service) && image.reference.startsWith(`${this.config.project}-${image.service}:`)
    ))
    const found = new Set(this.ownedBuildImages.map((image) => image.service))
    for (const service of OWNED_BUILD_SERVICES) {
      if (!found.has(service)) throw new Error(`missing owned build image for ${service}`)
    }
    await this.waitReady()
    return this.ports
  }

  async resolvePort(service, containerPort) {
    const result = await this.docker(['port', service, String(containerPort)])
    return parsePublishedPort(result.stdout)
  }

  async resolvePorts() {
    return {
      app: await this.resolvePort('frontend', 80),
      api: await this.resolvePort('backend', 8000),
      nats: await this.resolvePort('nats', 4222),
      monitor: await this.resolvePort('nats', 8222),
    }
  }

  urls() {
    if (!this.ports) throw new Error('stack ports are unresolved')
    return {
      APP_URL: `http://127.0.0.1:${this.ports.app}`,
      AGG_URL: `http://127.0.0.1:${this.ports.api}`,
      NATS_URL: `nats://127.0.0.1:${this.ports.nats}`,
      MONITOR_URL: `http://127.0.0.1:${this.ports.monitor}`,
      WS_BASE_URL: `ws://127.0.0.1:${this.ports.api}/ws`,
    }
  }

  async waitReady() {
    const urls = this.urls()
    const deadline = Date.now() + 180_000
    while (Date.now() < deadline) {
      try {
        const [app, health, monitor, registryResponse] = await Promise.all([
          this.fetch(urls.APP_URL),
          this.fetch(`${urls.AGG_URL}/api/system/status`),
          this.fetch(`${urls.MONITOR_URL}/healthz`),
          this.fetch(`${urls.AGG_URL}/api/registry`),
        ])
        const [status, registry] = await Promise.all([health.json(), registryResponse.json()])
        const rows = registry.filter((row) => row.agent_id === 'shell-1')
        const card = rows[0]?.card
        const extensions = card?.capabilities?.extensions || []
        const isL1 = extensions.some((entry) => entry.uri === 'https://edgecitadel.local/ext/nats-binding/v1')
        if (app.ok && monitor.ok && status.nats_connected === true && status.jetstream_stream_ok === true &&
          rows.length === 1 && rows[0].agent_state === 'online' &&
          card?.metadata?.['runtime.kind'] === 'native' &&
          card.metadata['runtime.roles']?.includes('worker') &&
          card.metadata['runtime.conformance'] === 'L1' && isL1) return
      } catch (error) {
        if (Date.now() >= deadline) throw error
      }
      await sleep(250)
    }
    throw new Error('stack readiness timed out')
  }

  async readProjectImages() {
    const result = await this.docker(['images', '--format', 'json'])
    const parsed = JSON.parse(result.stdout)
    const rows = Array.isArray(parsed) ? parsed : [parsed]
    return rows.map((row) => {
      const repository = row.Repository || ''
      const service = row.Service || (repository === 'nats'
        ? 'nats'
        : [...OWNED_BUILD_SERVICES].find((name) => repository === `${this.config.project}-${name}`))
      return {
        service,
        reference: service === 'nats' ? this.config.natsImage : `${repository}:${row.Tag}`,
        image_id: row.ID || row.ImageID,
      }
    })
  }

  runPlaywright(configPath, forwardedArgs) {
    return this.runCommand('npx', ['playwright', 'test', '--config', path.resolve(configPath)].concat(forwardedArgs), {
      cwd: path.join(this.config.repoRoot, 'e2e'),
      env: Object.assign({}, process.env, this.urls(), {
        E2E_RUN_ID: this.config.runId,
        E2E_TERMINAL_RELEASE_DIR: this.config.terminalReleaseDir,
        ...(this.config.evidenceDir ? { EVIDENCE_DIR: this.config.evidenceDir } : {}),
      }),
      shell: false,
      allowFailure: true,
      redactions: this.config.secretValues,
    })
  }

  async collectRuntimeSummary(token) {
    const result = await this.docker(['config'])
    this.runtimeSummary = {
      run_id: this.config.runId,
      project: this.config.project,
      run_dir: this.config.runDir,
      started_at: this.startedAt,
      captured_at: new Date().toISOString(),
      urls: this.urls(),
      images: { all: this.allImages, owned_build_references: this.ownedBuildImages },
      compose_config: redactSecrets(result.stdout, [token]),
    }
    await fs.writeFile(this.config.summaryFile, `${JSON.stringify(this.runtimeSummary, null, 2)}\n`, { mode: 0o600 })
    return this.runtimeSummary
  }

  async verifyCleanup() {
    const label = `label=com.docker.compose.project=${this.config.project}`
    const checks = {}
    for (const [key, args] of [
      ['containers', ['ps', '-aq', '--filter', label]],
      ['networks', ['network', 'ls', '-q', '--filter', label]],
      ['volumes', ['volume', 'ls', '-q', '--filter', label]],
    ]) {
      const result = await this.runCommand('docker', args, {
        shell: false, allowFailure: false, redactions: this.config.secretValues,
      })
      checks[key] = result.stdout.trim() ? result.stdout.trim().split('\n') : []
    }
    checks.owned_build_images = []
    for (const image of this.ownedBuildImages) {
      const result = await this.runCommand('docker', ['image', 'inspect', image.reference], {
        shell: false, allowFailure: true, redactions: this.config.secretValues,
      })
      if (result.code === 0) checks.owned_build_images.push(image.reference)
    }
    return { valid: Object.values(checks).every((resources) => resources.length === 0), resources: checks }
  }

  async persistCleanup(report) {
    const completed = Object.assign({ run_id: this.config.runId, project: this.config.project }, this.runtimeSummary || {}, {
      completed_at: new Date().toISOString(), run_directory: '<run-owned-path>', scratch_removed: true, cleanup: report,
    })
    delete completed.run_dir
    completed.compose_config = normalizeRuntimeText(completed.compose_config, this.config)
    completed.urls = {
      APP_URL: 'http://127.0.0.1:<loopback-port:app>', AGG_URL: 'http://127.0.0.1:<loopback-port:api>',
      NATS_URL: 'nats://127.0.0.1:<loopback-port:nats>', MONITOR_URL: 'http://127.0.0.1:<loopback-port:monitor>',
      WS_BASE_URL: 'ws://127.0.0.1:<loopback-port:api>/ws',
    }
    const summaryOutsideRun = !path.resolve(this.config.summaryFile).startsWith(`${path.resolve(this.config.runDir)}${path.sep}`)
    if (summaryOutsideRun) await fs.writeFile(this.config.summaryFile, `${JSON.stringify(completed, null, 2)}\n`, { mode: 0o600 })
    if (this.config.evidenceRuntimeDir) {
      await fs.mkdir(this.config.evidenceRuntimeDir, { recursive: true, mode: 0o700 })
      await fs.writeFile(path.join(this.config.evidenceRuntimeDir, 'launcher-summary.json'), `${JSON.stringify(completed, null, 2)}\n`, { mode: 0o600 })
      await fs.writeFile(path.join(this.config.evidenceRuntimeDir, 'cleanup.json'), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 })
    }
  }

  cleanup(reason) {
    if (this.cleanupPromise) return this.cleanupPromise
    this.cleanupPromise = (async () => {
      let down = { code: 1 }
      let verification = { valid: false, resources: { containers: [], networks: [], volumes: [], owned_build_images: [] } }
      let verificationError = null
      try {
        down = await this.docker(['down', '-v', '--remove-orphans', '--rmi', 'local'], true)
        verification = await this.verifyCleanup()
      } catch (error) {
        verificationError = redactSecrets(error.stack || error.message, this.config.secretValues)
      }
      const report = {
        reason, down_exit_code: down.code, all_images: this.allImages, owned_build_images: this.ownedBuildImages,
        valid: verificationError === null && down.code === 0 && verification.valid,
        resources: verification.resources,
        ...(verificationError ? { verification_error: verificationError } : {}),
      }
      try {
        await scrubRunFiles(this.config)
      } catch (error) {
        report.valid = false
        report.scrub_error = redactSecrets(error.stack || error.message, this.config.secretValues)
      }
      await this.persistCleanup(report)
      return report
    })()
    return this.cleanupPromise
  }

  installSignalHandlers(processObject) {
    for (const [signal, code] of [['SIGINT', 130], ['SIGTERM', 143]]) {
      processObject.once(signal, () => { void this.cleanup(signal).finally(() => this.exit(code)) })
    }
  }
}

module.exports = { OwnedStack, normalizeRuntimeText, redactSecrets, runCommand }
