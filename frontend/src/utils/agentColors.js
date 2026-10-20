const PALETTE = [
  '#6366f1', // indigo
  '#22c55e', // green
  '#f59e0b', // amber
  '#ef4444', // red
  '#06b6d4', // cyan
  '#ec4899', // pink
  '#8b5cf6', // violet
  '#14b8a6', // teal
  '#f97316', // orange
  '#3b82f6', // blue
  '#a855f7', // purple
  '#84cc16', // lime
]

// Fixed colors for well-known senders to guarantee contrast
const FIXED_COLORS = {
  dashboard: '#3b82f6', // blue
  system: '#6366f1',    // indigo
}

export function getAgentColor(agentName) {
  if (!agentName) return PALETTE[0]
  if (FIXED_COLORS[agentName]) return FIXED_COLORS[agentName]
  let hash = 0
  for (let i = 0; i < agentName.length; i++) {
    hash = agentName.charCodeAt(i) + ((hash << 5) - hash)
  }
  // Skip blue/indigo slots (0,9) since those are reserved for dashboard/system
  const agentPalette = PALETTE.filter((_, i) => i !== 0 && i !== 9)
  return agentPalette[Math.abs(hash) % agentPalette.length]
}
