import { expect, it, vi } from 'vitest'
import { loadSnapshot, patchGraph } from './graph'
import { graph, token, change, traceId } from './testFixtures'

it('assembles repeated endpoints and follows zero-node edge continuation', async () => {
  const whole = graph()
  const first = { ...whole, nodes: whole.nodes.slice(0, 1), edges: [], expansions: [{ node_id: whole.nodes[0].id, cursor: token('next'), remaining_nodes: whole.nodes.length - 1 }] }
  const second = { ...whole, page_kind: 'expansion', edges: [], expansions: [{ node_id: whole.nodes[0].id, cursor: token('edges'), remaining_nodes: 0 }] }
  const third = { ...whole, page_kind: 'expansion', expansions: [] }
  const api = { graph: vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second).mockResolvedValueOnce(third) }
  const result = await loadSnapshot(api, traceId)
  expect(result.nodes).toEqual(whole.nodes)
  expect(result.edges).toEqual(whole.edges)
  expect(result.expansions).toEqual([])
  expect(api.graph.mock.calls.slice(1).map(call => call[1])).toEqual([
    { expand: token('next') }, { expand: token('edges') },
  ])
})

it('rejects snapshot drift and repeated continuation instead of mixing views', async () => {
  const whole = graph()
  const first = { ...whole, expansions: [{ node_id: whole.nodes[0].id, cursor: token('next'), remaining_nodes: 0 }] }
  for (const second of [{ ...first, page_kind: 'expansion' }, { ...whole, page_kind: 'expansion', at: token('wrong') }]) {
    const api = { graph: vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second) }
    await expect(loadSnapshot(api, traceId)).rejects.toThrow('invalid_response')
  }
})

it('applies node/edge removal and inspector cursor atomically without mutating the base', () => {
  const original = graph()
  const row = change({ remove_node_ids: original.nodes.slice(1).map(node => node.id), remove_edge_ids: original.edges.map(edge => edge.id) })
  const patched = patchGraph(original, row)
  expect(patched.nodes).toHaveLength(1)
  expect(patched.edges).toHaveLength(0)
  expect(patched.at).toBe(row.at)
  expect(patched.resume_cursor).toBe(row.cursor)
  expect(original.nodes.length).toBeGreaterThan(1)
  expect(() => patchGraph(original, change({ remove_node_ids: [original.nodes[0].id] }))).toThrow('invalid_response')
})


it('reaches every node beyond the initial 500-node page and accepts reordered endpoint fields', async () => {
  const template = graph()
  const nodes = Array.from({ length: 501 }, (_, index) => {
    const task = '00000000-0000-4000-8000-' + index.toString(16).padStart(12, '0')
    return { ...template.nodes[0], id: 'task:' + task, task_id: task }
  })
  const first = { ...template, nodes: nodes.slice(0, 500), edges: [], total_nodes: 501,
    expansions: [{ node_id: nodes[0].id, cursor: token('tail'), remaining_nodes: 1 }] }
  const last = { ...template, page_kind: 'expansion', nodes: [
    Object.fromEntries(Object.entries(nodes[0]).reverse()), nodes[500],
  ], edges: [], total_nodes: 501, expansions: [] }
  const api = { graph: vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(last) }
  const result = await loadSnapshot(api, traceId)
  expect(result.nodes).toEqual(nodes)
})

it('rejects a causal cycle assembled across individually acyclic expansion pages', async () => {
  const template = graph()
  const nodes = template.nodes.slice(0, 3)
  const cursor = index => token('page' + index)
  const expansion = index => [{ node_id: nodes[0].id, cursor: cursor(index), remaining_nodes: 0 }]
  const first = { ...template, nodes, edges: [], total_nodes: 3, expansions: expansion(1) }
  const api = { graph: vi.fn().mockResolvedValueOnce(first) }
  for (let index = 0; index < 3; index++) {
    api.graph.mockResolvedValueOnce({ ...first, page_kind: 'expansion',
      edges: [{ id: 'parent' + index, kind: 'parent_task', from: nodes[index].id, to: nodes[(index + 1) % 3].id, status: 'resolved' }],
      expansions: index < 2 ? expansion(index + 2) : [],
    })
  }
  await expect(loadSnapshot(api, traceId)).rejects.toThrow('invalid_response')
})
