const mqtt = require('mqtt');

const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:11883';

class TestMQTTClient {
  constructor() {
    this.client = null;
    this._messageHandlers = [];
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.client = mqtt.connect(MQTT_URL, {
        reconnectPeriod: 1000,
        connectTimeout: 10_000,
      });
      this.client.on('connect', () => resolve());
      this.client.on('error', (err) => reject(err));

      // Central message dispatcher for waitForMessage
      this.client.on('message', (topic, message) => {
        let parsed;
        try {
          parsed = JSON.parse(message.toString());
        } catch {
          parsed = message.toString();
        }
        for (const handler of this._messageHandlers) {
          handler(topic, parsed);
        }
      });
    });
  }

  async disconnect() {
    if (this.client) {
      return new Promise((resolve) => {
        this.client.end(false, {}, () => resolve());
      });
    }
  }

  async publish(topic, payload) {
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    return new Promise((resolve, reject) => {
      this.client.publish(topic, data, { qos: 1 }, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async subscribe(topic) {
    return new Promise((resolve, reject) => {
      this.client.subscribe(topic, { qos: 1 }, (err, granted) => {
        if (err) reject(err);
        else resolve(granted);
      });
    });
  }

  waitForMessage(topic, predicate = () => true, timeout = 10_000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        // Clean up handler
        const idx = this._messageHandlers.indexOf(handler);
        if (idx !== -1) this._messageHandlers.splice(idx, 1);
        reject(new Error(`Timed out waiting for message on ${topic}`));
      }, timeout);

      const handler = (receivedTopic, parsed) => {
        // Match topic: support MQTT wildcards + and #
        if (!topicMatches(topic, receivedTopic)) return;
        if (predicate(parsed, receivedTopic)) {
          clearTimeout(timer);
          const idx = this._messageHandlers.indexOf(handler);
          if (idx !== -1) this._messageHandlers.splice(idx, 1);
          resolve(parsed);
        }
      };

      this._messageHandlers.push(handler);
    });
  }

  // --- Convenience methods ---

  async registerAgent(name, opts = {}) {
    const payload = {
      sender: name,
      sender_id: name,
      type: 'register',
      timestamp: new Date().toISOString(),
      display_name: opts.display_name || name,
      role: opts.role || 'worker',
      device_type: opts.device_type || 'raspberry_pi',
      model: opts.model || 'llama-3.2-1b',
      capabilities: opts.capabilities || ['text_generation', 'summarization'],
      ip_address: opts.ip_address || '192.168.1.100',
      payload: {
        display_name: opts.display_name || name,
        role: opts.role || 'worker',
        device_type: opts.device_type || 'raspberry_pi',
        model: opts.model || 'llama-3.2-1b',
        capabilities: opts.capabilities || ['text_generation', 'summarization'],
        ip_address: opts.ip_address || '192.168.1.100',
      },
    };
    await this.publish(`agents/${name}/register`, payload);
  }

  async sendHeartbeat(name, opts = {}) {
    const payload = {
      sender: name,
      sender_id: name,
      type: 'heartbeat',
      timestamp: new Date().toISOString(),
      cpu_percent: opts.cpu_percent ?? 45.2,
      memory_percent: opts.memory_percent ?? 62.8,
      status: 'online',
      payload: {
        cpu_percent: opts.cpu_percent ?? 45.2,
        memory_percent: opts.memory_percent ?? 62.8,
        status: 'online',
      },
    };
    await this.publish(`agents/${name}/heartbeat`, payload);
  }

  async changeAgentStatus(name, status) {
    const payload = {
      sender: name,
      sender_id: name,
      type: 'status',
      timestamp: new Date().toISOString(),
      status,
      payload: { status },
    };
    await this.publish(`agents/${name}/status`, payload);
  }

  async sendLog(name, level, message, opts = {}) {
    const payload = {
      sender: name,
      sender_id: name,
      type: 'log',
      timestamp: new Date().toISOString(),
      level,
      message,
      source: opts.source || name,
      payload: {
        level,
        message,
        source: opts.source || name,
        metadata: opts.metadata || {},
      },
    };
    await this.publish(`agents/${name}/log`, payload);
  }

  async assignTask(agentName, taskId, opts = {}) {
    const payload = {
      sender: 'orchestrator',
      sender_id: 'orchestrator',
      receiver: agentName,
      receiver_id: agentName,
      type: 'task_assign',
      correlation_id: opts.correlation_id || taskId,
      timestamp: new Date().toISOString(),
      task_id: taskId,
      title: opts.title || `Task ${taskId}`,
      description: opts.description || 'Test task',
      priority: opts.priority || 'normal',
      assigned_agent: agentName,
    };
    await this.publish(`tasks/${taskId}/assign`, payload);
  }

  async reportTaskProgress(agentName, taskId, opts = {}) {
    const payload = {
      sender: agentName,
      sender_id: agentName,
      type: 'task_progress',
      correlation_id: opts.correlation_id || taskId,
      timestamp: new Date().toISOString(),
      task_id: taskId,
      progress: opts.progress || 50,
      message: opts.message || 'In progress',
    };
    await this.publish(`tasks/${taskId}/progress`, payload);
  }

  async completeTask(agentName, taskId, result = {}) {
    const payload = {
      sender: agentName,
      sender_id: agentName,
      type: 'task_complete',
      correlation_id: taskId,
      timestamp: new Date().toISOString(),
      task_id: taskId,
      result: result,
    };
    await this.publish(`tasks/${taskId}/complete`, payload);
  }

  async failTask(agentName, taskId, error = 'Task failed') {
    const payload = {
      sender: agentName,
      sender_id: agentName,
      type: 'task_failed',
      correlation_id: taskId,
      timestamp: new Date().toISOString(),
      task_id: taskId,
      error_message: error,
      error: error,
    };
    await this.publish(`tasks/${taskId}/failed`, payload);
  }

  async sendMessage(sender, receiver, type, payload = {}) {
    const correlationId = `corr-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const msg = {
      sender,
      sender_id: sender,
      receiver,
      receiver_id: receiver,
      type,
      message_type: type,
      correlation_id: correlationId,
      timestamp: new Date().toISOString(),
      ...payload,
      payload,
    };
    await this.publish(`agents/${receiver}/inbox`, msg);
    return correlationId;
  }

  async sendBroadcast(sender, type, payload = {}) {
    const msg = {
      sender,
      sender_id: sender,
      type,
      timestamp: new Date().toISOString(),
      ...payload,
      payload,
    };
    await this.publish('system/broadcast', msg);
  }
}

/**
 * Check if an MQTT topic matches a subscription pattern.
 * Supports + (single-level) and # (multi-level) wildcards.
 */
function topicMatches(pattern, topic) {
  const patternParts = pattern.split('/');
  const topicParts = topic.split('/');

  for (let i = 0; i < patternParts.length; i++) {
    if (patternParts[i] === '#') {
      return true; // # matches everything from here on
    }
    if (patternParts[i] === '+') {
      if (i >= topicParts.length) return false;
      continue; // + matches any single level
    }
    if (i >= topicParts.length || patternParts[i] !== topicParts[i]) {
      return false;
    }
  }
  return patternParts.length === topicParts.length;
}

module.exports = { TestMQTTClient };
