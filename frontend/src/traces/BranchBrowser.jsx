import { useMemo, useState } from 'react'
import { nodeTitle, readable } from './layout'

const PAGE_SIZE = 50
const countSteps = count => `${count} ${count === 1 ? 'step' : 'steps'}`

// Task identity, rather than clock order or owner name, defines membership.
// Grouping is a navigation aid; it does not create graph nodes or causal edges.
export function groupBranches(graph) {
  const branches = new Map()
  for (const node of graph.nodes) {
    const key = node.task_id ?? ''
    if (!branches.has(key)) branches.set(key, { id: key, task: null, nodes: [], operations: new Map() })
    const branch = branches.get(key)
    branch.nodes.push(node)
    if (node.kind === 'task') branch.task = node
    const operationKey = JSON.stringify([node.kind, node.operation, node.agent_id])
    if (!branch.operations.has(operationKey)) branch.operations.set(operationKey, { id: operationKey, title: nodeTitle(node, graph.trace_id), kind: node.kind, owner: node.agent_id, nodes: [] })
    branch.operations.get(operationKey).nodes.push(node)
  }
  return [...branches.values()].sort((a, b) => a.id.localeCompare(b.id)).map(branch => ({
    ...branch,
    operations: [...branch.operations.values()].sort((a, b) => a.id.localeCompare(b.id)).map(operation => ({ ...operation, nodes: operation.nodes.sort((a, b) => a.id.localeCompare(b.id)) })),
  }))
}

function states(nodes) {
  const counts = new Map()
  for (const node of nodes) counts.set(node.state, (counts.get(node.state) ?? 0) + 1)
  return [...counts].sort(([a], [b]) => a.localeCompare(b)).map(([state, count]) => `${count} ${readable(state)}`).join(' · ')
}

function Pages({ page, count, onPage, label }) {
  if (count <= PAGE_SIZE) return null
  const last = Math.ceil(count / PAGE_SIZE) - 1
  return <div className="trace-pagination" aria-label={label}>
    <button disabled={page === 0} onClick={() => onPage(page - 1)}>Previous</button>
    <span>Page {page + 1} of {last + 1}</span>
    <button disabled={page === last} onClick={() => onPage(page + 1)}>Next</button>
  </div>
}

function Operation({ operation, selected, onSelect, open, onToggle }) {
  const [requestedPage, setPage] = useState(0)
  const page = Math.min(requestedPage, Math.max(0, Math.ceil(operation.nodes.length / PAGE_SIZE) - 1))
  return <li>
    <button aria-expanded={open} onClick={onToggle}>
      <strong>{operation.title} · {countSteps(operation.nodes.length)}</strong>
      <span>{readable(operation.kind)} · {operation.owner ?? 'Owner unknown'}</span>
      <span>{states(operation.nodes)}{operation.nodes.some(node => node.conflict) ? ' · conflicting evidence' : ''}</span>
    </button>
    {open && <><ol aria-label={`${operation.title} steps`}>
      {operation.nodes.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE).map(node => <li key={node.id}>
        <button data-step-id={node.id} aria-pressed={selected === node.id} onClick={() => onSelect(node.id)}>
          <span>{readable(node.state)}{node.conflict ? ' · conflicting evidence' : ''}</span><code title={node.id}>{node.id.length > 20 ? node.id.slice(0, 17) + '…' : node.id}</code>
        </button>
      </li>)}
    </ol><Pages page={page} count={operation.nodes.length} onPage={setPage} label="Operation step pages" /></>}
  </li>
}

function Branch({ branch, selected, onSelect, open, onToggle }) {
  const [expandedOperation, setExpandedOperation] = useState(null)
  const [requestedPage, setPage] = useState(0)
  const page = Math.min(requestedPage, Math.max(0, Math.ceil(branch.operations.length / PAGE_SIZE) - 1))
  return <li>
    <button aria-expanded={open} onClick={onToggle}>
      <strong>{branch.id ? branch.task?.agent_id ?? 'Task owner not observed' : 'Steps without a task'} · {countSteps(branch.nodes.length)}</strong>
      {branch.id && <code data-branch-id={branch.id} title={branch.id}>Task {branch.id.slice(0, 8)}</code>}
      <span>{states(branch.nodes)}</span>
    </button>
    {open && <>
      <ul aria-label="Operation groups">{branch.operations.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE).map(operation =>
        <Operation key={operation.id} operation={operation} selected={selected} onSelect={onSelect}
          open={expandedOperation === operation.id} onToggle={() => setExpandedOperation(expandedOperation === operation.id ? null : operation.id)} />)}</ul>
      <Pages page={page} count={branch.operations.length} onPage={setPage} label="Operation group pages" />
    </>}
  </li>
}

export default function BranchBrowser({ graph, selected, onSelect }) {
  const [open, setOpen] = useState(false)
  const [requestedPage, setPage] = useState(0)
  const [expandedBranch, setExpandedBranch] = useState(null)
  const branches = useMemo(() => groupBranches(graph), [graph])
  const page = Math.min(requestedPage, Math.max(0, Math.ceil(branches.length / PAGE_SIZE) - 1))
  return <section className="trace-branches" aria-label="Branch browser">
    <button aria-expanded={open} onClick={() => setOpen(!open)}>Browse branches and repeated steps</button>
    {open && <>
      <p>{branches.filter(branch => branch.id).length} task branches · {graph.nodes.length} total steps. Repeated operations are grouped within their task and owner; recorded relationships remain in the map.</p>
      <ul aria-label="Task branches">{branches.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE).map(branch =>
        <Branch key={branch.id} branch={branch} selected={selected} onSelect={onSelect}
          open={expandedBranch === branch.id} onToggle={() => setExpandedBranch(expandedBranch === branch.id ? null : branch.id)} />)}</ul>
      <Pages page={page} count={branches.length} onPage={setPage} label="Branch pages" />
    </>}
  </section>
}
