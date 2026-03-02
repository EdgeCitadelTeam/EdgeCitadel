const { TestAPIClient } = require('./api-client');

/**
 * Delete all agents from the database.
 * Useful for resetting state between test files.
 */
async function deleteAllAgents() {
  const api = new TestAPIClient();
  try {
    const res = await api.listAgents();
    const agents = res.data;
    for (const agent of agents) {
      try {
        await api.deleteAgent(agent.id);
      } catch {
        // ignore 404 if already gone
      }
    }
  } catch {
    // ignore if API is unreachable during cleanup
  }
}

/**
 * Delete a specific agent by ID.
 */
async function deleteAgent(agentId) {
  const api = new TestAPIClient();
  try {
    await api.deleteAgent(agentId);
  } catch {
    // ignore
  }
}

module.exports = { deleteAllAgents, deleteAgent };
