import { useEffect, useRef, useState } from 'react'
import { decodeContent, eventKey, eventNodeId } from './communication'
import TaskCommunication from './TaskCommunication'

function Copy({ text, label = 'Copy' }) {
  const [status, setStatus] = useState('')
  return <button onClick={async () => { try { await navigator.clipboard.writeText(text); setStatus('Copied') } catch { setStatus('Copy unavailable') } }}>{status || label}</button>
}
function Content({ event }) {
  return <>{event.content && event.content.status !== 'available' && <p className="trace-note">Content {event.content.status}{event.content.reason && ` · ${event.content.reason}`}</p>}
    {Object.entries(event.content?.fields ?? {}).map(([name, raw]) => {
      const value = decodeContent(raw), text = typeof value === 'string' ? value : JSON.stringify(value, null, 2)
      const body = typeof value === 'string' && !['input', 'output', 'arguments', 'boundaries'].includes(name) ? <p className="communication-prose">{text}</p> : <pre>{text}</pre>
      return <section key={name} className="communication-content"><h4>{name}</h4>{text.length > 900 ? <details><summary>{text.slice(0, 180)}… · Expand content</summary>{body}</details> : body}<Copy text={text} label={`Copy ${name}`} /></section>
    })}</>
}
function Message({ message, selected }) {
  const ref = useRef(null)
  const contentSeen = new Set()
  const bodyEvents = message.events.filter(event => {
    if (['broker', 'infrastructure'].includes(event.kind) || !event.content) return false
    const key = JSON.stringify(event.content)
    if (contentSeen.has(key)) return false
    contentSeen.add(key)
    return true
  })
  useEffect(() => { if (selected) ref.current?.scrollIntoView?.({ block: 'nearest' }) }, [selected])
  return <section ref={ref} className="communication-summary" data-detail-message={message.id} aria-label={`${message.type ?? 'Unknown'} message`}>
    <h3>{message.type} · {message.sender ?? 'Sender unknown'} → {message.recipient ?? 'Recipient unknown'}</h3><p>{message.state}</p>
    {message.conflicts.length > 0 && <p role="alert">Conflicting {message.conflicts.join(', ')}; no direction inferred.</p>}
    {!message.confirmed && <p className="trace-muted">No acceptance recorded in loaded evidence.</p>}
    {message.events.filter(e => /failed|rejected/.test(e.phase)).map(event => <p className="trace-error" key={eventKey(event)}>{event.phase} · {event.attributes?.error_type}</p>)}
    {!bodyEvents.some(e => Object.keys(e.content?.fields ?? {}).length) && <p className="trace-muted">Message body is not recorded in this snapshot.</p>}
    {bodyEvents.map(event => <Content key={eventKey(event)} event={event} />)}
  </section>
}
export default function CommunicationDetails({ selection, projection, route, expanded, onExpand, onClose, onEvent, partial, onSelect, onNavigate }) {
  const [tab, setTab] = useState(route.event || (route.step && !route.step.startsWith('task:') && !route.step.startsWith('agent:') && !route.step.startsWith('flow:')) ? 'Evidence' : 'Overview')
  const [latest, setLatest] = useState(false)
  const panel = useRef(null), linked = useRef(null), close = useRef(null)
  const task = selection.kind === 'task' ? selection : projection.tasks.find(item => item.id === selection.taskId)
  const messages = selection.kind === 'message' ? [selection] : task?.messages ?? selection.messages ?? []
  const events = selection.kind === 'message' ? selection.events : task?.events ?? selection.events
  const nodes = task?.nodes ?? selection.nodes ?? []
  useEffect(() => { close.current?.focus({ preventScroll: true }) }, [])
  useEffect(() => {
    setLatest(false)
    setTab(route.event || (route.step && !['task:', 'agent:', 'flow:'].some(prefix => route.step.startsWith(prefix))) ? 'Evidence' : 'Overview')
  }, [selection.id, route.event, route.step])
  useEffect(() => { if (route.event) { setTab('Evidence'); linked.current?.scrollIntoView?.({ block: 'nearest' }) } }, [route.event])
  const trap = event => {
    if (event.key === 'Escape') { event.preventDefault(); onClose(); return }
    if (onNavigate && ['ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key) && event.target.getAttribute('role') !== 'tab') {
      event.preventDefault()
      onNavigate(event.key === 'Home' ? 'first' : event.key === 'End' ? 'last' : event.key === 'ArrowDown' ? 1 : -1)
      return
    }
    if (event.key !== 'Tab'  || !window.matchMedia?.('(max-width: 900px)').matches) return
    const buttons = [...panel.current.querySelectorAll('button, input, summary, [tabindex="0"]')].filter(el => el.getClientRects().length)
    if (event.shiftKey && document.activeElement === buttons[0]) { event.preventDefault(); buttons.at(-1)?.focus() }
    if (!event.shiftKey && document.activeElement === buttons.at(-1)) { event.preventDefault(); buttons[0]?.focus() }
  }
  return <aside className="communication-details" role="dialog" aria-modal={window.matchMedia?.('(max-width: 900px)').matches || undefined} ref={panel} onKeyDown={trap} aria-label="Communication details">
    <header><div className="communication-detail-actions"><button ref={close} onClick={onClose}>Close details</button><button onClick={onExpand}>{expanded ? 'Collapse panel' : 'Expand panel'}</button><Copy text={window.location.href} label="Copy link" /></div>
      <h2>{selection.title}</h2>{selection.kind !== 'agent' && <p>{['message', 'flow'].includes(selection.kind) ? `${selection.sender ?? 'Unknown sender'} → ${selection.recipient ?? 'Unknown recipient'}` : task?.agentId ?? selection.id}</p>}{selection.state && <p>{selection.state}</p>}
      <div role="tablist" aria-label="Detail sections">{['Overview', 'Execution', 'Evidence'].map((name, index) => <button key={name} id={`detail-tab-${name}`} role="tab" tabIndex={tab === name ? 0 : -1} aria-selected={tab === name} aria-controls="communication-tab-panel" onClick={() => setTab(name)} onKeyDown={event => {
        if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) { event.preventDefault(); const next = event.key === 'Home' ? 0 : event.key === 'End' ? 2 : (index + (event.key === 'ArrowRight' ? 1 : 2)) % 3; setTab(['Overview', 'Execution', 'Evidence'][next]); event.currentTarget.parentElement.children[next].focus() }
      }}>{name}</button>)}</div>
    </header>
    <div role="tabpanel" id="communication-tab-panel" aria-labelledby={`detail-tab-${tab}`} className="communication-detail-body">
      {partial && <p className="trace-note">Evidence loading is incomplete. A return or confirmation may be outside the loaded range.</p>}
      {tab === 'Overview' && <>
        {selection.kind === 'topology' && <><p>{selection.description}</p><p>Current topology · read {selection.topology?.at ?? 'time unavailable'}</p></>}
        {selection.kind === 'agent' && <section><h3>Agent identity</h3><code>{selection.id}</code><p>Snapshot session: {selection.state || 'Not observed'}</p><p>Current host: {selection.topologyCard?.hostName ?? selection.topologyCard?.nodeId ?? 'Not recorded'}</p><p>{selection.topologyCard?.description ?? 'Responsibilities not recorded'}</p><p>Current registry state: {selection.topologyCard?.state ?? 'Unknown'} · read {selection.topologyAt ?? 'time unavailable'}</p><p>Historical host routing is not established by current registry data.</p></section>}
        {!task && <div className="flow-task-links">{projection.tasks.filter(item => selection.kind === 'agent' ? item.agentId === selection.id : messages.some(message => message.taskId === item.id)).map(item => <button key={item.id} data-task-id={item.id} onClick={event => onSelect(item, event.currentTarget)}><strong>{item.title}</strong><span>{item.state}{item.failures > 0 && ` · ${item.failures} tool failures`}</span></button>)}</div>}
        {task && <><h3>{task.title}</h3><p>Task · {task.state}{task.conflict && ' · conflicting outcomes'}</p><p>{task.failures} tool failures</p>
          {!task.messages.some(message => message.type === 'result') && <p>No return recorded in loaded evidence.</p>}
          {task.events.filter(e => e.kind === 'task' && e.content).map(event => <Content key={eventKey(event)} event={event} />)}</>}
        {messages.filter(m => m.type !== 'progress' || selection.kind === 'message').map(message => <Message key={message.id} message={message} selected={route.message === message.id} />)}
        {selection.kind !== 'message' && messages.some(m => m.type === 'progress') && <details><summary>{messages.filter(m => m.type === 'progress').length} progress messages</summary>{messages.filter(m => m.type === 'progress').map(message => <Message key={message.id} message={message} />)}</details>}
        {!task && !messages.length && <>{events.filter(event => event.kind === 'run' && Object.keys(event.content?.fields ?? {}).length > 0).map(event => <section key={eventKey(event)}><h3>{event.phase}</h3><Content event={event} /></section>)}<p>{events.length} recorded observations. Open Evidence for full identities and relationships.</p></>}
        {task && !route.at && <section><button aria-expanded={latest} onClick={() => setLatest(!latest)}>Latest messages</button>{latest && <TaskCommunication key={task.id} taskId={task.id} historical={false} />}</section>}
      </>}
      {tab === 'Execution' && <>{(task?.attempts ?? selection.attempts ?? []).map(attempt => <section key={attempt.id}><h3 title={attempt.id}>Execution attempt {(task?.attempts ?? selection.attempts).indexOf(attempt) + 1}</h3>{attempt.steps.map(step => <details key={step.id}><summary>{step.title} · {step.state}{step.durations.length > 0 && ` · ${step.durations.join(' / ')} ms`}</summary>{step.events.map(event => <section key={eventKey(event)}><h4>{event.phase}</h4>{event.attributes?.error_type && <p className="trace-error">{event.attributes.error_type}</p>}<Content event={event} /><button onClick={() => onEvent(event)}>View raw evidence</button></section>)}</details>)}</section>)}{!(task?.attempts ?? selection.attempts)?.length && <p>No execution steps in this selection.</p>}</>}

      {tab === 'Evidence' && <>
        <p>Source sequence establishes local order. Source clocks do not establish communication duration.</p>
        {route.event && !events.some(e => eventKey(e) === route.event) && <p>The linked observation is not in the loaded evidence. Continue loading to search this snapshot.</p>}
        {events.map(event => <details key={eventKey(event)} ref={eventKey(event) === route.event ? linked : undefined} open={eventKey(event) === route.event || (Boolean(route.step) && eventNodeId(event) === route.step)} className="communication-evidence"><summary>{event.kind} · {event.phase} · sequence {event.source_seq}</summary><code>{eventKey(event)}</code><Content event={event} /><button onClick={() => onEvent(event)}>Link to observation</button><pre>{JSON.stringify(event, null, 2)}</pre><Copy text={JSON.stringify(event, null, 2)} label="Copy event JSON" /></details>)}
        <details><summary>Graph identities and relationships</summary><pre>{JSON.stringify({ nodes, edges: projection.graphEdges?.filter(edge => nodes.some(n => n.id === edge.from || n.id === edge.to)) }, null, 2)}</pre></details>
      </>}
    </div>
  </aside>
}
