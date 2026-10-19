import { act, fireEvent, render, screen } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import ExecutionMap from './ExecutionMap'
import { causalContext, extendLayout } from './layout'
import { graph } from './testFixtures'

it('keeps positions for existing identities while placing late parents and branches without overlap', () => {
  const first = { nodes: [{ id: 'child' }], edges: [] }
  const positions = extendLayout(new Map(), first)
  const next = extendLayout(positions, { nodes: [{ id: 'parent' }, { id: 'child' }, { id: 'sibling' }], edges: [
    { from: 'parent', to: 'child', status: 'resolved', kind: 'parent' },
    { from: 'parent', to: 'sibling', status: 'resolved', kind: 'parent' },
  ] })
  expect(next.get('child')).toEqual(positions.get('child'))
  expect(new Set([...next.values()].map(point => `${point.x}/${point.y}`)).size).toBe(3)
  expect(causalContext([
    { from: 'parent', to: 'child', status: 'resolved' },
    { from: 'untrusted', to: 'child', status: 'invalid' },
    { from: 'child', to: 'result', status: 'resolved', kind: 'join' },
  ], 'child')).toEqual({ upstream: new Set(['parent']), downstream: new Set(['result']) })
})

it('makes every step of a large graph reachable in bounded pages, search and text view', () => {
  const value = graph()
  value.nodes = Array.from({ length: 501 }, (_, index) => ({ ...value.nodes[0], id: `step${String(index).padStart(3, '0')}`, operation: `Operation ${index}` }))
  value.edges = []
  const select = vi.fn()
  const { container, rerender } = render(<ExecutionMap graph={value} selected="step500" onSelect={select} />)
  expect(container.querySelectorAll('[data-node-id]')).toHaveLength(100)
  fireEvent.click(screen.getByRole('button', { name: 'Show selected step' }))
  expect(screen.getByText('Showing 501–501 of 501 steps')).toBeInTheDocument()
  fireEvent.click(container.querySelector('[data-node-id="step500"]'))
  expect(select).toHaveBeenCalledWith('step500')
  fireEvent.click(screen.getByRole('button', { name: 'Text view' }))
  expect(screen.getByRole('list', { name: 'Execution step list' })).toBeInTheDocument()
  fireEvent.change(screen.getByLabelText('Find a step'), { target: { value: 'step042' } })
  expect(screen.getByText('Showing 1–1 of 1 steps')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: 'Map view' }))
  const before = container.querySelector('[data-node-id="step042"]').style.cssText
  rerender(<ExecutionMap graph={{ ...value, nodes: value.nodes.map(node => ({ ...node, state: 'completed' })) }} selected="step500" onSelect={select} />)
  expect(container.querySelector('[data-node-id="step042"]').style.cssText).toBe(before)
})

it('supports keyboard step navigation without requiring pointer selection', () => {
  const { container } = render(<ExecutionMap graph={graph()} selected={null} onSelect={vi.fn()} />)
  const buttons = container.querySelectorAll('[data-node-id]')
  buttons[0].focus()
  fireEvent.keyDown(buttons[0], { key: 'ArrowDown' })
  expect(buttons[1]).toHaveFocus()
  fireEvent.keyDown(buttons[1], { key: 'Home' })
  expect(buttons[0]).toHaveFocus()
})

it.each(['map', 'text'])('keeps a focused %s step mounted when live insertion moves it across a page boundary', mode => {
  const value = graph()
  value.nodes = Array.from({ length: 101 }, (_, index) => ({ ...value.nodes[0], id: `step${String(index).padStart(3, '0')}` }))
  value.edges = []
  const select = vi.fn()
  const { container, rerender } = render(<ExecutionMap graph={value} selected="step099" onSelect={select} />)
  if (mode === 'text') fireEvent.click(screen.getByRole('button', { name: 'Text view' }))
  const selector = mode === 'text' ? '[data-text-node-id="step099"]' : '[data-node-id="step099"]'
  const focused = container.querySelector(selector)
  act(() => focused.focus())
  const next = { ...value, nodes: [{ ...value.nodes[0], id: 'step-before' }, ...value.nodes] }
  rerender(<ExecutionMap graph={next} selected="step099" onSelect={select} />)
  expect(container.querySelector(selector)).toHaveFocus()
  expect(screen.getByText('Showing 101–102 of 102 steps')).toBeInTheDocument()
  fireEvent.click(container.querySelector(selector))
  expect(select).toHaveBeenCalledWith('step099')
  const search = screen.getByLabelText('Find a step')
  act(() => search.focus())
  rerender(<ExecutionMap graph={value} selected="step099" onSelect={select} />)
  expect(search).toHaveFocus()
  expect(screen.getByText('Showing 101–101 of 101 steps')).toBeInTheDocument()
})
