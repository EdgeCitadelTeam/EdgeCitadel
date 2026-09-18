import { describe, expect, it } from 'vitest'
import { readResponse } from './protocol'
import { fixture, graph, clone, change, changes, readError } from './testFixtures'

describe('canonical trace response contract', () => {
  it('accepts every canonical read fixture or reports its fixed error', () => {
    for (const response of fixture.responses) {
      if (response.kind === 'trace_error') expect(() => readResponse(response)).toThrow(response.code)
      else expect(readResponse(response)).toBe(response)
    }
    expect(readResponse(graph()).nodes.length).toBeGreaterThan(0)
  })
  it('rejects wrong scope, malformed rows and conflicting ancestry', () => {
    const value = graph()
    expect(() => readResponse(value, { traceId: 'b'.repeat(32) })).toThrow('invalid_response')
    expect(() => readResponse(value, { generation: 'other' })).toThrow('generation_changed')
    const duplicate = clone(value)
    duplicate.nodes.push(duplicate.nodes[0])
    expect(() => readResponse(duplicate)).toThrow('invalid_response')
    const cycle = clone(value)
    cycle.edges.push({ ...cycle.edges[0], id: 'cycle', from: cycle.edges[0].to, to: cycle.edges[0].from })
    expect(() => readResponse(cycle)).toThrow('invalid_response')
    expect(() => readResponse({ ...value, private_unexpected: 'NEVER_DISPLAY' })).toThrow('invalid_response')
  })
  it('rejects incomplete replacement and clear semantics', () => {
    expect(() => readResponse(changes([change({ mode: 'snapshot', upsert_nodes: graph().nodes })]))).toThrow('invalid_response')
    expect(() => readResponse(changes([change({ mode: 'clear', at: null })]))).toThrow('invalid_response')
    expect(() => readResponse({ ...readError('history_expired'), resnapshot_required: false })).toThrow('invalid_response')
  })
})
