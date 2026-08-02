function requireEnvironment(name) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

async function pollJson(request, url, predicate, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const response = await request.get(url)
    if (response.ok()) {
      const value = await response.json()
      if (predicate(value)) return value
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error(`timed out polling ${url}`)
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(
      (key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`,
    ).join(',')}}`
  }
  return JSON.stringify(value)
}

async function assertNoOverlap(locators) {
  for (const locator of locators) await locator.scrollIntoViewIfNeeded()
  const viewport = await locators[0].evaluate(() => ({ width: window.innerWidth, height: window.innerHeight }))
  const boxes = []
  for (const locator of locators) {
    const box = await locator.boundingBox()
    if (!box) throw new Error('evidence locator has no bounding box')
    if (box.x < 0 || box.y < 0 || box.x + box.width > viewport.width || box.y + box.height > viewport.height) {
      throw new Error('evidence locator is outside the viewport')
    }
    boxes.push(box)
  }
  for (let left = 0; left < boxes.length; left += 1) {
    for (let right = left + 1; right < boxes.length; right += 1) {
      const a = boxes[left]
      const b = boxes[right]
      if (!(a.x + a.width <= b.x || b.x + b.width <= a.x || a.y + a.height <= b.y || b.y + b.height <= a.y)) {
        throw new Error(`overlap at ${left}/${right}`)
      }
    }
  }
}

async function assertInViewport(locator) {
  const viewport = await locator.evaluate(() => ({ width: window.innerWidth, height: window.innerHeight }))
  const box = await locator.boundingBox()
  if (!box) throw new Error('evidence locator has no bounding box')
  if (box.x < 0 || box.y < 0 || box.x + box.width > viewport.width || box.y + box.height > viewport.height) {
    throw new Error('evidence locator is outside the viewport')
  }
}

module.exports = { assertInViewport, assertNoOverlap, canonicalJson, pollJson, requireEnvironment }
