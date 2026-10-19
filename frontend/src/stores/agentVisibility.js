export const isTestAgent = (agent) =>
  (agent.card?.metadata?.['runtime.deployment'] || agent.deployment) === 'test'

export const visibleAgents = (agents, showTestAgents) => agents.filter((agent) =>
  !(agent.card?.metadata?.['runtime.roles'] || []).includes('aggregator') &&
  (showTestAgents || !isTestAgent(agent)))
