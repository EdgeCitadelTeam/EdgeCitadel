const assert = require('node:assert/strict')
const { execFile } = require('node:child_process')
const fs = require('node:fs/promises')
const os = require('node:os')
const path = require('node:path')
const { promisify } = require('node:util')

const exec = promisify(execFile)

function requestedTree(argv) {
  if (argv.length === 0) return process.env.E2E_CANDIDATE_TREE || 'HEAD'
  if (argv.length !== 2 || argv[0] !== '--tree') {
    throw new Error('usage: clean-checkout.js [--tree TREEISH]')
  }
  return argv[1]
}

async function main(argv) {
  const repoRoot = path.resolve(__dirname, '../..')
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'edgecitadel-clean-checkout-'))
  try {
    const resolved = await exec('git', ['rev-parse', `${requestedTree(argv)}^{tree}`], { cwd: repoRoot })
    const tree = resolved.stdout.trim()
    if (!/^[0-9a-f]{40,64}$/.test(tree)) throw new Error(`invalid resolved tree: ${tree}`)
    const archive = path.join(root, 'candidate.tar')
    const checkout = path.join(root, 'checkout')
    const summaryFile = path.join(root, 'summary.json')
    await fs.mkdir(checkout)
    await exec('git', ['archive', '--format=tar', `--output=${archive}`, tree], { cwd: repoRoot })
    await exec('tar', ['-xf', archive, '-C', checkout])
    await exec(process.execPath, ['e2e/run-isolated.js', '--probe-only', '--summary-file', summaryFile], {
      cwd: checkout, maxBuffer: 10 * 1024 * 1024,
    })
    const summary = JSON.parse(await fs.readFile(summaryFile, 'utf8'))
    assert.equal(summary.cleanup.valid, true)
    for (const resources of Object.values(summary.cleanup.resources)) assert.deepEqual(resources, [])
    process.stdout.write('PASS clean-checkout\n')
  } finally {
    await fs.rm(root, { recursive: true, force: true })
  }
}

if (require.main === module) {
  void main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack || error.message}\n`)
    process.exitCode = 1
  })
}

module.exports = { main, requestedTree }
