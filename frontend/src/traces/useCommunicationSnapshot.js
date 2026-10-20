import { useCallback, useEffect, useRef, useState } from 'react'
import { projectCommunication } from './communication'
import { TraceReadError } from './protocol'

// A graph and its first event page become visible together. Further evidence
// belongs to that exact cursor; an update never combines old events/new nodes.
export function useCommunicationSnapshot(api, graph, onDenied) {
  const [view, setView] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const owner = useRef(null)
  const load = useCallback(async (job, pages) => {
    if (job.loading) return
    job.loading = true; setLoading(true); setError(null)
    try {
      for (let i = 0; i < pages; i++) {
        const response = await api.events(job.graph.trace_id, { as_of: job.graph.at, after: job.next, limit: 200 }, job.controller.signal)
        if (job.controller.signal.aborted) return
        if (response.as_of !== job.graph.at || response.projection_generation !== job.graph.projection_generation || (response.next_cursor && job.seen.has(response.next_cursor))) throw new TraceReadError('invalid_response')
        job.events.push(...response.events)
        job.next = response.next_cursor
        job.seen.add(job.next)
        setView({ graph: job.graph, projection: projectCommunication(job.graph, job.events), next: job.next })
        if (!job.next) break
      }
    } catch (failure) {
      if (job.controller.signal.aborted) return
      setError(failure)
      if (failure.code === 'not_authorized') { setView(null); onDenied() }
    } finally {
      job.loading = false
      if (!job.controller.signal.aborted) setLoading(false)
    }
  }, [api, onDenied])
  useEffect(() => {
    if (!graph) { setView(null); setLoading(false); return }
    const job = { graph, events: [], next: null, seen: new Set(), controller: new AbortController(), loading: false }
    owner.current = job
    void load(job, 5)
    return () => { job.controller.abort(); owner.current = null }
  }, [graph, load])
  return { view, error, loading, more: () => owner.current && load(owner.current, 5) }
}
