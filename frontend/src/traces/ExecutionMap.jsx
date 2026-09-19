import { useEffect, useId, useMemo, useRef, useState } from 'react'
import BranchBrowser from './BranchBrowser'
import { causalContext, extendLayout, NODE_HEIGHT, NODE_WIDTH, nodeTitle, readable } from './layout'

const PAGE_SIZE = 100
export default function ExecutionMap({ graph, selected, onSelect }) {
  const [memory, setMemory] = useState(() => ({ graph, positions: extendLayout(new Map(), graph) }))
  const positions = memory.graph === graph ? memory.positions : extendLayout(memory.positions, graph)
  const [textView, setTextView] = useState(false)
  const [filter, setFilter] = useState('')
  const [group, setGroup] = useState('all')
  const [requestedPage, setPage] = useState(0)
  const [focusedStep, setFocusedStep] = useState(null)
  const scroll = useRef(null), initialized = useRef(null)
  const marker = useId().replaceAll(':', '')
  const ordered = useMemo(() => [...graph.nodes].sort((a, b) => a.id.localeCompare(b.id)), [graph.nodes])
  const groups = useMemo(() => {
    const counts = new Map()
    for (const node of graph.nodes) counts.set(node.agent_id, (counts.get(node.agent_id) ?? 0) + 1)
    return [...counts].sort(([a], [b]) => (a ?? '').localeCompare(b ?? ''))
  }, [graph.nodes])
  const filtered = ordered.filter(node => (group === 'all' || (node.agent_id ?? '') === group.slice(6)) &&
    `${nodeTitle(node, graph.trace_id)} ${node.agent_id ?? ''} ${node.state} ${node.id}`.toLowerCase().includes(filter.toLowerCase()))
  if (memory.graph !== graph) {
    setMemory({ graph, positions: extendLayout(memory.positions, graph) })
    // Retain the focused DOM identity when new evidence shifts a page boundary.
    // Adjust before commit so React never unmounts the focused step in between.
    const focusedIndex = focusedStep === null ? -1 : filtered.findIndex(node => node.id === focusedStep)
    if (focusedIndex >= 0) setPage(Math.floor(focusedIndex / PAGE_SIZE))
  }
  const lastPage = Math.max(0, Math.ceil(filtered.length / PAGE_SIZE) - 1)
  const page = Math.min(requestedPage, lastPage)
  const visible = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const ids = new Set(visible.map(node => node.id))
  const edges = graph.edges.filter(edge => ids.has(edge.from) && ids.has(edge.to))
  const context = useMemo(() => causalContext(graph.edges, selected), [graph.edges, selected])
  const select = id => {
    onSelect(id)
  }
  const revealSelection = () => {
    setFilter(''); setGroup('all')
    setPage(Math.max(0, Math.floor(ordered.findIndex(node => node.id === selected) / PAGE_SIZE)))
  }
  const viewKey = `${page}/${group}/${filter}/${textView}`
  useEffect(() => {
    if (!scroll.current || initialized.current === viewKey || !visible.length) return
    const first = positions.get(visible.find(node => node.id === 'run:' + graph.trace_id)?.id ?? visible[0].id)
    scroll.current.scrollLeft = Math.max(0, first.x + NODE_WIDTH / 2 - scroll.current.clientWidth / 2)
    scroll.current.scrollTop = Math.max(0, first.y - 28)
    initialized.current = viewKey
  }, [graph.trace_id, positions, visible, viewKey])
  useEffect(() => {
    const button = [...(scroll.current?.querySelectorAll('[data-node-id]') ?? [])].find(element => element.dataset.nodeId === selected)
    button?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' })
  }, [selected, viewKey])
  const width = Math.max(760, ...visible.map(node => positions.get(node.id).x + NODE_WIDTH + 28))
  const height = Math.max(360, ...visible.map(node => positions.get(node.id).y + NODE_HEIGHT + 28))
  const relation = id => id === selected ? 'selected' : context.upstream.has(id) ? 'upstream' : context.downstream.has(id) ? 'downstream' : ''
  function keyboard(event, index) {
    const movement = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 }
    let next = movement[event.key] === undefined ? null : Math.max(0, Math.min(visible.length - 1, index + movement[event.key]))
    if (event.key === 'Home') next = 0
    if (event.key === 'End') next = visible.length - 1
    if (next === null) return
    event.preventDefault()
    event.currentTarget.parentElement.querySelector(`[data-map-index="${next}"]`)?.focus()
  }
  return <section className="execution-map" aria-label="Execution steps">
    <div className="trace-map-tools">
      <label>Find a step<input value={filter} onChange={event => { setFilter(event.target.value); setPage(0) }} placeholder="Owner, operation, state or ID" /></label>
      <label>Step group<select value={group} onChange={event => { setGroup(event.target.value); setPage(0) }}>
        <option value="all">All owners ({graph.nodes.length})</option>
        {groups.map(([agent, count]) => <option key={agent ?? ''} value={'owner:' + (agent ?? '')}>{agent ?? 'Owner unknown'} ({count})</option>)}
      </select></label>
      <button type="button" aria-pressed={textView} onClick={() => setTextView(!textView)}>{textView ? 'Map view' : 'Text view'}</button>
    </div>
    <BranchBrowser graph={graph} selected={selected} onSelect={id => {
      setFilter(''); setGroup('all')
      setPage(Math.max(0, Math.floor(ordered.findIndex(node => node.id === id) / PAGE_SIZE)))
      onSelect(id)
    }} />
    <div className="trace-map-caption">
      <span>{filtered.length === 0 ? 'No matching steps' : `Showing ${page * PAGE_SIZE + 1}–${page * PAGE_SIZE + visible.length} of ${filtered.length} steps`}</span>
      {selected && !ids.has(selected) && graph.nodes.some(node => node.id === selected) && <button onClick={revealSelection}>Show selected step</button>}
      <span>Arrows show recorded relationships; pan to follow branches.</span>
    </div>
    {textView ? <ul className="trace-text-view" aria-label="Execution step list">
      {visible.map(node => <li key={node.id}>
        <button data-text-node-id={node.id} aria-pressed={selected === node.id} onFocus={() => setFocusedStep(node.id)} onBlur={() => setFocusedStep(null)} onClick={() => select(node.id)}>
          <strong>{nodeTitle(node, graph.trace_id)}</strong><span>{node.agent_id ?? 'Owner unknown'}</span><span>{readable(node.state)}{node.conflict ? ' · conflicting evidence' : ''}</span>
        </button>
        <ul>{graph.edges.filter(edge => edge.from === node.id || edge.to === node.id).map(edge => {
          const other = graph.nodes.find(item => item.id === (edge.from === node.id ? edge.to : edge.from))
          return <li key={edge.id}>{readable(edge.kind)} ({edge.status}): <button onClick={() => select(other.id)}>{nodeTitle(other, graph.trace_id)} · {other.agent_id ?? 'Owner unknown'}</button></li>
        })}</ul>
      </li>)}
    </ul> : <div className="trace-map-scroll" ref={scroll} tabIndex={0} aria-label="Scrollable execution map">
      <div className="trace-canvas" style={{ width, height }} role="group" aria-label="Execution map page">
        <svg width={width} height={height} aria-hidden="true">
          <defs><marker id={marker} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" /></marker></defs>
          {edges.map(edge => {
            const start = positions.get(edge.from), end = positions.get(edge.to)
            const x1 = start.x + NODE_WIDTH / 2, y1 = start.y + NODE_HEIGHT, x2 = end.x + NODE_WIDTH / 2, y2 = end.y
            const bend = y1 + Math.max(20, (y2 - y1) / 2)
            const highlighted = edge.from === selected || edge.to === selected ||
              (context.upstream.has(edge.from) && context.upstream.has(edge.to)) ||
              (context.downstream.has(edge.from) && context.downstream.has(edge.to))
            return <path key={edge.id} className={`trace-edge ${edge.kind === 'join' ? 'join' : ''} ${edge.status} ${highlighted ? 'highlighted' : ''}`}
              d={`M ${x1} ${y1} V ${bend} H ${x2} V ${y2}`} markerEnd={`url(#${marker})`} />
          })}
        </svg>
        {visible.map((node, index) => {
          const point = positions.get(node.id)
          return <button key={node.id} data-node-id={node.id} data-map-index={index} className={`trace-node ${relation(node.id)} state-${node.state}`}
            style={{ left: point.x, top: point.y, width: NODE_WIDTH, height: NODE_HEIGHT }}
            aria-pressed={selected === node.id} aria-label={`${nodeTitle(node, graph.trace_id)}, ${node.agent_id ?? 'Owner unknown'}, ${readable(node.state)}`}
            onFocus={() => setFocusedStep(node.id)} onBlur={() => setFocusedStep(null)}
            onClick={() => select(node.id)} onKeyDown={event => keyboard(event, index)}>
            <span className="trace-node-type">{readable(node.kind)}{node.conflict ? ' · conflict' : ''}</span>
            <strong>{nodeTitle(node, graph.trace_id)}</strong>
            <span className="trace-node-owner">{node.agent_id ?? 'Owner unknown'}</span>
            <span className="trace-node-state">{readable(node.state)}</span>
          </button>
        })}
      </div>
    </div>}
    <div className="trace-legend"><span>Solid: recorded parent</span><span>Dashed: result join or unresolved link</span><span>Highlighted: selected context</span></div>
    {filtered.length > PAGE_SIZE && <div className="trace-pagination">
      <button disabled={page === 0} onClick={() => setPage(page - 1)}>Previous steps</button><span>Page {page + 1} of {lastPage + 1}</span>
      <button disabled={page === lastPage} onClick={() => setPage(page + 1)}>Next steps</button>
    </div>}
  </section>
}
