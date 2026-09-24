import { useEffect, useId, useRef, useState } from 'react'

// Registry membership is a current reference, never proof of a historical route.
export default function CommunicationMap({ projection, selected, onSelect, topology }) {
  const [page, setPage] = useState(0), [replay, setReplay] = useState(0)
  const marker = useId().replaceAll(':', '')
  const scroll = useRef(null)
  const [available, setAvailable] = useState(600)
  useEffect(() => {
    if (!globalThis.ResizeObserver) return
    const observer = new ResizeObserver(([entry]) => setAvailable(entry.contentRect.width))
    observer.observe(scroll.current)
    return () => observer.disconnect()
  }, [])
  const matching = projection.interactions
  const initialMessage = matching.find(row => row.message?.drawable)?.message
  const focused = selected ?? initialMessage
  const selectedIndex = focused ? matching.findIndex(row => row.message?.id === focused.id || row.task?.id === focused.id) : -1
  // Follow selections made from details or deep links, including page boundaries.
  useEffect(() => { if (selected && selectedIndex >= 0) setPage(Math.floor(selectedIndex / 50)) }, [selected, selectedIndex])
  const current = Math.min(page, Math.max(0, Math.ceil(matching.length / 50) - 1))
  const rows = matching.slice(current * 50, current * 50 + 50)
  const agents = projection.agents
  const hosts = new Map()
  for (const agent of agents) {
    const card = topology?.agents?.find(row => row.id === agent.id)
    const host = card?.nodeId ?? `unknown:${agent.id}`
    if (!hosts.has(host)) hosts.set(host, { id: host, title: card?.hostName ?? card?.nodeId ?? 'Host not recorded', leaf: card?.leaf, mode: card?.mode, address: card?.address, agents: [] })
    if (!hosts.get(host).address && card?.address) hosts.get(host).address = card.address
    hosts.get(host).agents.push(agent)
  }
  // Keep cards readable and route message arrows below them, as in the reference mock.
  const coreAddresses = [...new Set((topology?.agents ?? []).map(agent => agent.coreAddress).filter(Boolean))]
  const width = Math.max(680, available)
  const columns = Math.min(3, hosts.size || 1)
  const hostWidth = width / columns
  const agentColumns = Math.max(1, Math.floor((hostWidth - 32) / 216))
  const hostHeight = 260 + Math.max(1, ...[...hosts.values()].map(host => Math.ceil(host.agents.length / agentColumns))) * 160
  const height = 136 + Math.ceil(hosts.size / columns) * hostHeight
  const positions = new Map()
  const hostRows = [...hosts.values()].map((host, index) => {
    const x = index % columns * hostWidth, y = 136 + Math.floor(index / columns) * hostHeight
    host.agents.forEach((agent, i) => {
      const count = Math.min(agentColumns, host.agents.length - Math.floor(i / agentColumns) * agentColumns)
      positions.set(agent.id, { x: x + (i % agentColumns + .5) * hostWidth / count, y: y + 226 + Math.floor(i / agentColumns) * 160 })
    })
    return { ...host, x, y }
  })
  const message = focused?.kind === 'message' && focused.drawable ? focused : null
  const from = positions.get(message?.sender), to = positions.get(message?.recipient)
  const routeY = from && to ? Math.max(from.y, to.y) + 96 : 0
  const path = from && to ? from === to
    ? `M${from.x + 100},${from.y} C${from.x + 130},${from.y} ${from.x + 130},${routeY} ${from.x},${routeY} L${from.x},${from.y + 58}`
    : from.x === to.x ? `M${from.x + 100},${from.y} H${from.x + 112} V${to.y} H${to.x + 100}`
    : `M${from.x},${from.y + 58} V${routeY - 12} Q${from.x},${routeY} ${from.x + (to.x >= from.x ? 12 : -12)},${routeY} H${to.x - (to.x >= from.x ? 12 : -12)} Q${to.x},${routeY} ${to.x},${routeY - 12} V${to.y + 58}` : null
  const choose = (item, event) => onSelect(item, event.currentTarget)
  const navigate = (index, element) => {
    const row = matching[index]
    if (row) { setPage(Math.floor(index / 50)); onSelect(row.message ?? row.task, element) }
  }
  return <section className="communication-map agent-flow" aria-label="Agent communication map">
    <div className="flow-options"><div><h2>Agent topology</h2><p>Follow each request and result between agents.</p></div><span className="flow-counts"><strong>{agents.length}</strong> agents <strong>{projection.tasks.length}</strong> tasks</span>
      {(projection.evidence.events.length > 0 || projection.evidence.nodes.length > 0) && <button onClick={event => choose(projection.evidence, event)}>Unassociated evidence ({projection.evidence.events.length})</button>}
    </div>
    <details className="topology-context"><summary>Current topology <span>Connection reference</span></summary><p className="trace-muted">{topology?.at && `Read ${topology.at}. `}{topology?.error ?? 'Registry configuration is not historical routing evidence. A configured leaf may be offline.'}</p></details>
    <div className="communication-scroll" ref={scroll}>
      <div className="flow-canvas topology-canvas" style={{ height, width }}>
        <button className="topology-core" onClick={event => choose({ id: 'core', kind: 'topology', title: 'Core', events: [], description: 'Core provides cross-host transport and collects telemetry. Local task delivery does not require Core. Registry configuration alone does not prove a connection.', topology }, event)}><strong>Core NATS</strong><span>Configured client endpoint</span><code className="flow-network-address">{coreAddresses.join(' / ') || 'IP / port not recorded'}</code></button>
        {hostRows.map(host => <section key={host.id} className="topology-host" style={{ left: host.x + 8, top: host.y, width: hostWidth - 16, height: hostHeight - 16 }} aria-label={host.title}>
          <div className="topology-host-heading"><strong>{host.title}</strong><span>{host.agents.length} {host.agents.length === 1 ? 'agent' : 'agents'}</span></div><button className="topology-transport" onClick={event => choose({ id: host.id, kind: 'topology', title: host.title, events: [], description: host.mode === 'nats_leaf' ? `Configured NATS leaf: ${host.leaf ?? 'identity not recorded'}. Online state is not established.` : 'Leaf configuration not recorded.', topology }, event)}><strong>{host.mode === 'nats_leaf' ? 'NATS leaf' : host.mode === 'single-client' ? 'agentd · direct client' : 'Transport not recorded'}</strong><span>{host.leaf ?? (host.mode ? 'Current connection configuration' : 'No registry configuration')}</span><code className="flow-network-address">{host.address ?? 'IP / port not recorded'}</code></button>
        </section>)}
        <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`} aria-hidden="true" className="topology-lines">
          <defs><marker id={marker} markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8" fill="var(--trace-accent)" /></marker></defs>
          {hostRows.map(host => <g key={host.id}>
            {host.mode && <path className="topology-reference" d={`M${width / 2},106 V120 H${host.x + hostWidth / 2} V${host.y + 72}`} />}
            {host.mode && host.agents.map(agent => {
              const point = positions.get(agent.id)
              return <path key={agent.id} className="topology-reference" d={`M${host.x + hostWidth / 2},${host.y + 156} V${point.y - 62} H${point.x} V${point.y - 58}`} />
            })}
          </g>)}
          {path && <g><path data-arrow-id={message.id} className="communication-arrow selected" d={path} strokeDasharray={message.type === 'result' ? '6 5' : undefined} markerEnd={`url(#${marker})`} />
            <text className="selected-message-label" textAnchor="middle" x={(from.x + to.x) / 2} y={routeY - 10}>#{projection.interactions.findIndex(row => row.message?.id === message.id) + 1} · {message.type}</text>
            <circle key={`${message.id}/${replay}`} className="communication-motion" r="5" fill="currentColor"><animateMotion dur="1.4s" repeatCount="2" path={path} /></circle>
          </g>}
        </svg>
        {agents.map(agent => {
          const point = positions.get(agent.id), card = topology?.agents?.find(row => row.id === agent.id)
          const address = card?.address ?? hosts.get(card?.nodeId)?.address
          return <button key={agent.id} data-agent-id={agent.id} className={`flow-agent topology-agent${message && [message.sender, message.recipient].includes(agent.id) ? ' active' : ''}`} style={{ left: point.x, top: point.y }} aria-pressed={selected?.kind === 'agent' && selected.id === agent.id} onClick={event => choose({ ...agent, topologyCard: card, topologyAt: topology?.at }, event)}><span className="flow-agent-avatar" aria-hidden="true">{agent.title.slice(0, 2).toUpperCase()}</span><strong>{agent.title}</strong><span className="flow-agent-state">{agent.state ? `Session ${agent.state}` : 'Session not observed'}</span><code className="flow-network-address">{address ? `via NATS ${address}` : 'IP / port not recorded'}</code></button>
        })}
      </div>
    </div>
    {message && <div className="selected-direction"><strong>#{projection.interactions.findIndex(row => row.message?.id === message.id) + 1} · {message.type} · {message.sender} → {message.recipient}</strong><button onClick={() => setReplay(value => value + 1)}>Replay motion</button><span>Logical message direction · animation is a replay</span></div>}
    <div className="interaction-heading"><h3>Interaction order <span>{matching.length}</span></h3><p className="trace-muted">Select a message to follow its direction and inspect the evidence.</p></div>
    <nav className="trace-pagination" aria-label="Selected interaction"><button disabled={selectedIndex <= 0} onClick={event => navigate(selectedIndex - 1, event.currentTarget)}>Previous</button><button disabled={selectedIndex >= matching.length - 1} onClick={event => navigate(selectedIndex + 1, event.currentTarget)}>Next</button></nav>
    <ol aria-label="Communication list" className="communication-order" start={current * 50 + 1}>{rows.map((row, index) => <li key={row.id}>
      <button data-message-id={row.message?.id} data-task-id={row.task?.id} aria-pressed={(row.message ?? row.task).id === focused?.id} onClick={event => choose(row.message ?? row.task, event)} onKeyDown={event => {
        if (!['ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return
        event.preventDefault()
        navigate(event.key === 'Home' ? 0 : event.key === 'End' ? matching.length - 1 : current * 50 + index + (event.key === 'ArrowDown' ? 1 : -1), event.currentTarget)
      }}><span className="interaction-number">{projection.interactions.indexOf(row) + 1}</span><span className="interaction-content"><span className="interaction-route"><strong className="interaction-type">{row.message?.type ?? 'Task'}</strong><strong>{row.message ? `${row.message.sender ?? 'Unknown sender'} → ${row.message.recipient ?? 'Unknown recipient'}` : row.task.title}</strong></span><span className="interaction-preview">{row.task?.title ?? projection.tasks.find(task => task.id === row.message?.taskId)?.title ?? row.message?.taskId}</span><span className="interaction-status">{row.message?.state ?? row.task?.state}</span></span></button>
    </li>)}</ol>
    <p className="interaction-footnote">Numbers indicate reading order, not causality or elapsed time across hosts.</p>
    {matching.length > 50 && <div className="trace-pagination"><button disabled={!current} onClick={() => setPage(current - 1)}>Previous interactions</button><span>{current * 50 + 1}–{Math.min((current + 1) * 50, matching.length)} of {matching.length}</span><button disabled={(current + 1) * 50 >= matching.length} onClick={() => setPage(current + 1)}>Next interactions</button></div>}
    {!matching.length && <p className="trace-map-caption">No matching communication recorded.</p>}
  </section>
}
