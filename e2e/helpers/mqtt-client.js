const mqtt = require('mqtt');

const MQTT_URL = `mqtt://${process.env.MQTT_HOST || 'localhost'}:${process.env.MQTT_PORT || 11883}`;
const MQTT_USER = process.env.MQTT_USER || 'iot_agent';
const MQTT_PASS = process.env.MQTT_PASS || 'openclaw_secret';

class TestMQTTClient {
  constructor() {
    this.client = null;
    this._subscriptions = new Map();
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.client = mqtt.connect(MQTT_URL, {
        username: MQTT_USER,
        password: MQTT_PASS,
        connectTimeout: 10_000,
        clean: true,
      });
      this.client.on('connect', () => resolve());
      this.client.on('error', (err) => reject(err));
      this.client.on('message', (topic, message) => {
        const handlers = this._subscriptions.get(topic) || [];
        let parsed;
        try {
          parsed = JSON.parse(message.toString());
        } catch {
          parsed = message.toString();
        }
        for (const handler of handlers) {
          handler(parsed, topic);
        }
      });
    });
  }

  async disconnect() {
    if (this.client) {
      this._subscriptions.clear();
      return new Promise((resolve) => {
        this.client.end(false, () => resolve());
      });
    }
  }

  async publish(topic, payload) {
    return new Promise((resolve, reject) => {
      const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
      this.client.publish(topic, data, { qos: 1 }, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async subscribe(topic) {
    return new Promise((resolve, reject) => {
      this.client.subscribe(topic, { qos: 1 }, (err) => {
        if (err) reject(err);
        else {
          if (!this._subscriptions.has(topic)) {
            this._subscriptions.set(topic, []);
          }
          resolve();
        }
      });
    });
  }

  waitForMessage(topic, predicate = () => true, timeout = 10_000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out waiting for message on ${topic}`));
      }, timeout);

      const handler = (parsed, msgTopic) => {
        if (predicate(parsed, msgTopic)) {
          cleanup();
          resolve(parsed);
        }
      };

      const cleanup = () => {
        clearTimeout(timer);
        const handlers = this._subscriptions.get(topic) || [];
        const idx = handlers.indexOf(handler);
        if (idx >= 0) handlers.splice(idx, 1);
      };

      if (!this._subscriptions.has(topic)) {
        this._subscriptions.set(topic, []);
      }
      this._subscriptions.get(topic).push(handler);
    });
  }

  // --- Convenience methods ---

  async registerAgent(name, opts = {}) {
    const payload = {
      sender: name,
      type: 'register',
      timestamp: new Date().toISOString(),
      payload: {
        display_name: opts.display_name || name,
        role: opts.role || 'worker',
        device_type: opts.device_type || 'raspberry_pi',
        model: opts.model || 'llama-3.2-1b',
        capabilities: opts.capabilities || ['text_generation', 'summarization'],
        ip_address: opts.ip_address || '192.168.1.100',
      },
    };
    await this.publish(`agents/register/${name}`, payload);
  }

  async sendHeartbeat(name, opts = {}) {
    const payload = {
      sender: name,
      type: 'heartbeat',
      timestamp: new Date().toISOString(),
      payload: {
        cpu_percent: opts.cpu_percent ?? 45.2,
        memory_percent: opts.memory_percent ?? 62.8,
        status: 'online',
      },
    };
    await this.publish(`agents/heartbeat/${name}`, payload);
  }

  async changeAgentStatus(name, status) {
    const payload = {
      sender: name,
      type: 'status',
      timestamp: new Date().toISOString(),
      payload: { status },
    };
    await this.publish(`agents/status/${name}`, payload);
  }

  async sendLog(name, level, message, opts = {}) {
    const payload = {
      sender: name,
      type: 'log',
      timestamp: new Date().toISOString(),
      payload: {
        level,
        message,
        source: opts.source || name,
        metadata: opts.metadata || {},
      },
    };
    await this.publish(`agents/logs/${name}`, payload);
  }

  async assignTask(agentName, taskId, opts = {}) {
    const payload = {
      sender: 'orchestrator',
      receiver: agentName,
      type: 'task_assign',
      correlation_id: opts.correlation_id || taskId,
      timestamp: new Date().toISOString(),
      payload: {
        task_id: taskId,
        title: opts.title || `Task ${taskId}`,
        description: opts.description || 'Test task',
        priority: opts.priority || 'normal',
      },
    };
    await this.publish(`agents/task/${agentName}/assign`, payload);
  }

  async reportTaskProgress(agentName, taskId, opts = {}) {
    const payload = {
      sender: agentName,
      type: 'task_progress',
      correlation_id: opts.correlation_id || taskId,
      timestamp: new Date().toISOString(),
      payload: {
        task_id: taskId,
        progress: opts.progress || 50,
        message: opts.message || 'In progress',
      },
    };
    await this.publish(`agents/task/${agentName}/progress`, payload);
  }

  async completeTask(agentName, taskId, result = {}) {
    const payload = {
      sender: agentName,
      type: 'task_complete',
      correlation_id: taskId,
      timestamp: new Date().toISOString(),
      payload: {
        task_id: taskId,
        result: result,
      },
    };
    await this.publish(`agents/task/${agentName}/complete`, payload);
  }

  async failTask(agentName, taskId, error = 'Task failed') {
    const payload = {
      sender: agentName,
      type: 'task_failed',
      correlation_id: taskId,
      timestamp: new Date().toISOString(),
      payload: {
        task_id: taskId,
        error_message: error,
      },
    };
    await this.publish(`agents/task/${agentName}/failed`, payload);
  }

  async sendMessage(sender, receiver, type, payload = {}) {
    const correlationId = `corr-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const msg = {
      sender,
      receiver,
      type,
      correlation_id: correlationId,
      timestamp: new Date().toISOString(),
      payload,
    };
    await this.publish(`agents/inbox/${receiver}`, msg);
    return correlationId;
  }

  async sendBroadcast(sender, type, payload = {}) {
    const msg = {
      sender,
      type,
      timestamp: new Date().toISOString(),
      payload,
    };
    await this.publish('agents/broadcast', msg);
  }
}

module.exports = { TestMQTTClient };
