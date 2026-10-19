import { render, screen } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import ObservationInspector, { eventKey } from './ObservationInspector'
import { graph } from './testFixtures'
import fixtures from '../../../agent-runtime/tests/fixtures/traces/events.v1.json'

it('keeps hostile display text inert and identifies canonical local references without links', async () => {
  const selectedGraph = graph()
  const event = structuredClone(fixtures.fixtures.find(item => item.name === 'tool').event)
  event.attributes = { name: 'javascript:trace-fixture', local_content_ref: '11111111-1111-4111-8111-111111111111', content_available: true }
  const hostile = '<img src=x onerror="window.traceInjected=true">'
  const node = { ...selectedGraph.nodes[0], operation: hostile, kind: 'tool' }
  const api = { events: vi.fn(async () => ({ as_of: selectedGraph.at, projection_generation: selectedGraph.projection_generation, events: [event], next_cursor: null })) }
  const view = render(<ObservationInspector api={api} graph={selectedGraph} node={node} route={{ event: eventKey(event) }} onDenied={vi.fn()} onSelect={vi.fn()} />)
  expect(screen.getByRole('heading', { name: hostile })).toBeInTheDocument()
  await screen.findByText('Local-only reference; not fetched by this dashboard')
  expect(view.container.querySelectorAll('img, script, iframe, a')).toHaveLength(0)
  expect(window.traceInjected).toBeUndefined()
  expect(api.events).toHaveBeenCalledTimes(1)
})
