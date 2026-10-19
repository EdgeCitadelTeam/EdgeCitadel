import { requireProtocol, validateGraph } from './protocol'

export function assertScope(page, traceId, generation) {
  requireProtocol(page.trace_id === traceId && page.projection_generation === generation)
}

// The complete replacement is staged offscreen. Edge-only continuation pages
// still matter even when remaining_nodes is zero; follow every cursor.
export async function loadSnapshot(api, traceId, at, signal, onProgress = () => {}) {
  const first = await api.graph(traceId, at ? { at } : {}, signal)
  requireProtocol(first.page_kind === 'snapshot' && first.trace_id === traceId)
  requireProtocol(!at || first.at === at)
  const nodes = new Map(), edges = new Map(), visited = new Set()
  const pending = []
  let freshness = first.freshness
  const merge = (target, rows) => {
    for (const row of rows) {
      // Pages at one exact snapshot may repeat edge endpoints, never revise them.
      if (target.has(row.id)) {
        const previous = target.get(row.id)
        // Canonical node/edge records have scalar fields. JSON member order is
        // not identity and may differ between equivalent endpoint copies.
        requireProtocol(Object.keys(previous).length === Object.keys(row).length &&
          Object.keys(row).every(key => Object.hasOwn(previous, key) && previous[key] === row[key]))
      } else target.set(row.id, row)
    }
  }
  const add = page => {
    assertScope(page, traceId, first.projection_generation)
    requireProtocol(page.at === first.at && page.resume_cursor === first.resume_cursor &&
      page.ingest_high_watermark === first.ingest_high_watermark && page.total_nodes === first.total_nodes)
    merge(nodes, page.nodes); merge(edges, page.edges)
    for (const expansion of page.expansions) {
      requireProtocol(!visited.has(expansion.cursor))
      visited.add(expansion.cursor); pending.push(expansion.cursor)
    }
    freshness = page.freshness
    onProgress({ loaded: nodes.size, total: first.total_nodes })
  }
  add(first)
  for (let index = 0; index < pending.length; index++) {
    signal?.throwIfAborted()
    const page = await api.graph(traceId, { expand: pending[index] }, signal)
    requireProtocol(page.page_kind === 'expansion')
    add(page)
  }
  requireProtocol(first.total_nodes === null || first.total_nodes === nodes.size)
  const graph = { ...first, nodes: [...nodes.values()], edges: [...edges.values()], expansions: [], freshness }
  validateGraph(graph.nodes, graph.edges)
  return graph
}

// A patch and its inspector/replay cursors are one publishable value.
export function patchGraph(graph, change) {
  const nodes = new Map(graph.nodes.map(node => [node.id, node]))
  const edges = new Map(graph.edges.map(edge => [edge.id, edge]))
  for (const id of change.remove_node_ids) nodes.delete(id)
  for (const id of change.remove_edge_ids) edges.delete(id)
  for (const node of change.upsert_nodes) nodes.set(node.id, node)
  for (const edge of change.upsert_edges) edges.set(edge.id, edge)
  const result = {
    ...graph, nodes: [...nodes.values()], edges: [...edges.values()],
    at: change.at, resume_cursor: change.cursor,
    ingest_high_watermark: change.ingest_high_watermark,
    coverage: change.coverage, total_nodes: nodes.size,
  }
  validateGraph(result.nodes, result.edges)
  return result
}
