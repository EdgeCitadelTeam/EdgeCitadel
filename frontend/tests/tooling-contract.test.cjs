const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const test = require('node:test')

const root = path.resolve(__dirname, '..')

test('frontend exposes locked unit and lint gates', () => {
  const pkg = JSON.parse(
    fs.readFileSync(path.join(root, 'package.json'), 'utf8'),
  )

  assert.equal(pkg.scripts.test, 'vitest run')
  assert.equal(pkg.scripts.lint, 'eslint . --max-warnings=0')
  assert.equal(fs.existsSync(path.join(root, 'eslint.config.js')), true)
  assert.equal(fs.existsSync(path.join(root, 'src/test/setup.js')), true)
  assert.equal(typeof pkg.dependencies['@noble/hashes'], 'string')
  assert.equal(typeof pkg.devDependencies.eslint, 'string')
})
