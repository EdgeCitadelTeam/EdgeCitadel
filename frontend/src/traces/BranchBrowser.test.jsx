import { fireEvent, render, screen, within } from '@testing-library/react'
import { expect, it, vi } from 'vitest'
import BranchBrowser, { groupBranches } from './BranchBrowser'
import { graph } from './testFixtures'

function workload(count = 2) {
  const source = graph(), task = { ...source.nodes[0], kind: 'task', task_id: 'task-a', id: 'task:task-a', agent_id: 'worker' }
  const nodes = [task, ...Array.from({ length: count }, (_, index) => ({ ...task, id: `span:${String(index).padStart(3, '0')}`, kind: 'model', operation: 'Repeated model', state: 'running' }))]
  return { ...source, nodes, edges: [] }
}

it('keeps identical operations separate across tasks and owners, preserving every canonical step', () => {
  const source = workload()
  source.nodes.push({ ...source.nodes[1], id: 'other-task', task_id: 'task-b' }, { ...source.nodes[1], id: 'other-owner', agent_id: 'another' })
  const branches = groupBranches(source)
  expect(branches).toHaveLength(2)
  expect(branches[0].operations).toHaveLength(3)
  expect(branches.flatMap(branch => branch.operations.flatMap(operation => operation.nodes.map(node => node.id))).sort()).toEqual(source.nodes.map(node => node.id).sort())
})

it('preserves expanded groups on live updates and selects an exact repeated step beyond the first page', () => {
  const source = workload(51), select = vi.fn()
  const view = render(<BranchBrowser graph={source} selected={null} onSelect={select} />)
  fireEvent.click(screen.getByRole('button', { name: 'Browse branches and repeated steps' }))
  fireEvent.click(screen.getByRole('button', { name: /worker · 52 steps/ }))
  fireEvent.click(screen.getByRole('button', { name: /Repeated model · 51 steps/ }))
  expect(within(screen.getByRole('list', { name: 'Repeated model steps' })).getAllByRole('button')).toHaveLength(50)
  fireEvent.click(within(screen.getByLabelText('Operation step pages')).getByRole('button', { name: 'Next' }))
  fireEvent.click(screen.getByRole('button', { name: 'running span:050' }))
  expect(select).toHaveBeenCalledWith('span:050')
  const updated = { ...source, nodes: source.nodes.map(node => node.id === 'span:050' ? { ...node, state: 'finished' } : node) }
  view.rerender(<BranchBrowser graph={updated} selected="span:050" onSelect={select} />)
  expect(screen.getByRole('button', { name: 'finished span:050' })).toHaveAttribute('aria-pressed', 'true')
  expect(screen.getByRole('button', { name: /Repeated model · 51 steps/ })).toHaveAttribute('aria-expanded', 'true')
  expect(screen.getByRole('button', { name: /Repeated model · 51 steps/ })).toHaveTextContent('1 finished · 50 running')
})

it('expands one task at a time without carrying another task’s operation controls across', () => {
  const source = workload()
  source.nodes.push(...source.nodes.map(node => ({ ...node, id: 'other-' + node.id, task_id: 'task-b' })))
  render(<BranchBrowser graph={source} selected={null} onSelect={vi.fn()} />)
  fireEvent.click(screen.getByRole('button', { name: 'Browse branches and repeated steps' }))
  const branches = screen.getAllByRole('button', { name: /worker · 3 steps/ })
  fireEvent.click(branches[0])
  fireEvent.click(screen.getByRole('button', { name: /Repeated model · 2 steps/ }))
  expect(screen.getByRole('list', { name: 'Repeated model steps' })).toBeInTheDocument()
  fireEvent.click(branches[1])
  expect(branches[0]).toHaveAttribute('aria-expanded', 'false')
  expect(screen.queryByRole('list', { name: 'Repeated model steps' })).not.toBeInTheDocument()
  expect(screen.getAllByRole('list', { name: 'Operation groups' })).toHaveLength(1)
})
