/**
 * Returns the heartbeat age in seconds for a given ISO timestamp.
 * Returns Infinity when no timestamp is available.
 */
export function ageSec(lastHeartbeat) {
  if (!lastHeartbeat) return Infinity
  return Math.max(0, (Date.now() - new Date(lastHeartbeat).getTime()) / 1000)
}
