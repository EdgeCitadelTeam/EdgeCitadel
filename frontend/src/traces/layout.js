export const NODE_WIDTH = 204
export const NODE_HEIGHT = 88
const X_STEP = 236, Y_STEP = 132

// Positions belong to the visible run, not arrival order or node status. Keep
// surviving nodes fixed; place new observations in unoccupied cells. A late
// parent can yield a sideways/upward edge, whose arrow remains authoritative.
export function extendLayout(previous, graph) {
  const ids = new Set(graph.nodes.map(node => node.id))
  const positions = new Map([...previous].filter(([id]) => ids.has(id)))
  const occupied = new Set([...positions.values()].map(({ column, row }) => `${column}/${row}`))
  const parents = new Map(graph.edges.filter(edge => edge.status === 'resolved' && edge.kind !== 'join').map(edge => [edge.to, edge.from]))
  const assign = id => {
    const chain = [], visited = new Set()
    let current = id
    while (ids.has(current) && !positions.has(current) && !visited.has(current)) {
      chain.push(current); visited.add(current); current = parents.get(current)
    }
    while (chain.length) {
      const key = chain.pop(), parent = positions.get(parents.get(key))
      const row = parent ? parent.row + 1 : 0
      const preferred = parent?.column ?? 0
      let column = preferred, offset = 0
      while (column < 0 || occupied.has(`${column}/${row}`)) {
        offset++; column = preferred + (offset % 2 ? -(offset + 1) / 2 : offset / 2)
      }
      occupied.add(`${column}/${row}`)
      positions.set(key, { column, row, x: 28 + column * X_STEP, y: 28 + row * Y_STEP })
    }
  }
  for (const node of [...graph.nodes].sort((a, b) => a.id.localeCompare(b.id))) assign(node.id)
  return positions
}

export function causalContext(edges, selected) {
  const incoming = new Map(), outgoing = new Map()
  for (const edge of edges) {
    if (edge.status !== 'resolved') continue
    if (!incoming.has(edge.to)) incoming.set(edge.to, [])
    if (!outgoing.has(edge.from)) outgoing.set(edge.from, [])
    incoming.get(edge.to).push(edge.from); outgoing.get(edge.from).push(edge.to)
  }
  const walk = links => {
    const seen = new Set(), queue = [selected]
    for (let index = 0; index < queue.length; index++) {
      for (const id of links.get(queue[index]) ?? []) {
        if (id !== selected && !seen.has(id)) { seen.add(id); queue.push(id) }
      }
    }
    return seen
  }
  return { upstream: walk(incoming), downstream: walk(outgoing) }
}

export function nodeTitle(node, traceId) {
  if (node.id === 'run:' + traceId) return 'Initiating run'
  if (node.kind === 'run') return 'Execution attempt'
  if (node.kind === 'permission') return 'Permission check'
  if (node.kind === 'dispatch') return 'Delegation'
  if (node.kind === 'unresolved') return 'Unresolved step'
  return node.operation || (node.kind === 'task' ? 'Task' : node.kind === 'tool' ? 'Tool operation' : 'Model operation')
}
export const readable = value => value ? value.replaceAll('_', ' ') : 'Not reported'
