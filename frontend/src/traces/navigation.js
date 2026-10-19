import { useSyncExternalStore } from 'react'

const subscribe = listener => {
  window.addEventListener('hashchange', listener)
  window.addEventListener('popstate', listener)
  return () => { window.removeEventListener('hashchange', listener); window.removeEventListener('popstate', listener) }
}
const currentHash = () => window.location.hash
export function parseTraceRoute(hash) {
  const params = new URLSearchParams(hash.startsWith('#execution?') ? hash.slice(11) : '')
  const run = params.get('run'), task = params.get('task'), step = params.get('step'), at = params.get('at'), event = params.get('event'), message = params.get('message')
  const valid = (value, pattern) => value !== null && pattern.test(value) ? value : null
  const route = {
    run: valid(run, /^[0-9a-f]{32}$(?![\s\S])/),
    task: valid(task, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/),
    step: valid(step, /^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$(?![\s\S])/),
    at: at && at.length <= 4096 && /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$(?![\s\S])/.test(at) ? at : null,
    message: valid(message, /^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$(?![\s\S])/),
    event: event && event.length <= 250 && /^[a-z0-9_-]+\/[0-9a-f-]+\/[0-9a-f-]+$(?![\s\S])/.test(event) ? event : null,
  }
  route.invalid = ['run', 'task', 'step', 'at', 'event', 'message'].some(key => params.has(key) && route[key] === null)
  return route
}
export const useTraceRoute = () => parseTraceRoute(useSyncExternalStore(subscribe, currentHash, () => ''))
export function navigateTrace(route = {}, { replace = false } = {}) {
  const params = new URLSearchParams()
  for (const key of ['run', 'task', 'step', 'at', 'event', 'message']) if (route[key]) params.set(key, route[key])
  const hash = '#execution' + (params.size ? '?' + params.toString() : '')
  if (hash === window.location.hash) return
  window.history[replace ? 'replaceState' : 'pushState']({}, '', hash)
  window.dispatchEvent(new Event('hashchange'))
}
