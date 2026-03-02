let counter = 0;

/**
 * Generate a unique ID for test isolation.
 * @param {string} prefix
 * @returns {string}
 */
function uniqueId(prefix = 'test') {
  counter++;
  return `${prefix}-${Date.now()}-${counter}-${Math.random().toString(36).slice(2, 6)}`;
}

/**
 * Factory: create an agent registration payload.
 */
function makeAgent(overrides = {}) {
  const name = overrides.name || uniqueId('agent');
  return {
    name,
    display_name: overrides.display_name || name,
    role: overrides.role || 'worker',
    device_type: overrides.device_type || 'raspberry_pi',
    model: overrides.model || 'llama-3.2-1b',
    capabilities: overrides.capabilities || ['text_generation', 'summarization'],
    ip_address: overrides.ip_address || '192.168.1.100',
  };
}

/**
 * Factory: create a task payload.
 */
function makeTask(overrides = {}) {
  const id = overrides.id || uniqueId('task');
  return {
    title: overrides.title || `Task ${id}`,
    description: overrides.description || 'Automated test task',
    assigned_agent: overrides.assigned_agent || null,
    priority: overrides.priority || 'normal',
    id,
  };
}

/**
 * Factory: create a message payload.
 */
function makeMessage(overrides = {}) {
  return {
    sender: overrides.sender || uniqueId('sender'),
    receiver: overrides.receiver || uniqueId('receiver'),
    type: overrides.type || 'command',
    correlation_id: overrides.correlation_id || uniqueId('corr'),
    timestamp: overrides.timestamp || new Date().toISOString(),
    payload: overrides.payload || { message: 'test message' },
  };
}

/**
 * Factory: create a log entry payload.
 */
function makeLog(overrides = {}) {
  return {
    level: overrides.level || 'INFO',
    message: overrides.message || 'Test log entry',
    source: overrides.source || 'test-runner',
    metadata: overrides.metadata || {},
  };
}

module.exports = { uniqueId, makeAgent, makeTask, makeMessage, makeLog };
