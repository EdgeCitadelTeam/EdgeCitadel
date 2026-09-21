import { useEffect, useState } from 'react'
import { api } from '../api/client'

export default function TaskCommunication({ taskId, historical }) {
  const [state, setState] = useState({ messages: [], loading: true, failed: false })
  useEffect(() => {
    if (historical || !taskId) return
    const controller = new AbortController()
    let timer
    async function refresh() {
      try {
        const messages = await api.queryMessages({ task_id: taskId, limit: 100 }, controller.signal)
        if (controller.signal.aborted) return
        if (!Array.isArray(messages)) throw new Error('Invalid communication response')
        setState({ messages, loading: false, failed: false })
        if (!messages.some(message => message.type === 'result')) timer = setTimeout(refresh, 2000)
      } catch {
        if (controller.signal.aborted) return
        setState(old => ({ ...old, loading: false, failed: true }))
        timer = setTimeout(refresh, 5000)
      }
    }
    void refresh()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [taskId, historical])
  if (!taskId) return null
  return <section className="trace-communication" aria-label="Task communication">
    <h3>Task communication</h3>
    {historical ? <p className="trace-muted">Resume live to see the task’s retained messages. These messages are separate from the frozen trace snapshot.</p> : <>
      <p className="trace-muted">Latest 100 command, progress and result messages.</p>
      {state.loading && <p role="status">Loading communication…</p>}
      {state.failed && <p role="alert">Communication is unavailable. Retrying…</p>}
      {!state.loading && !state.failed && !state.messages.length && <p>No retained messages for this task.</p>}
      <ol>{[...state.messages].sort((a, b) => a.timestamp.localeCompare(b.timestamp)).map(message => {
        const content = message.payload?.body ?? message.payload?.message ?? message.payload?.delta ?? ''
        return <li key={message.id}>
          <strong>{message.sender_id}{message.recipient_id ? ` → ${message.recipient_id}` : ''}</strong>
          <span>{message.type}{message.task_state ? ` · ${message.task_state}` : ''}</span>
          <time>{message.timestamp}</time>
          {content !== '' && <pre>{typeof content === 'string' ? content : JSON.stringify(content, null, 2)}</pre>}
        </li>
      })}</ol>
    </>}
  </section>
}
