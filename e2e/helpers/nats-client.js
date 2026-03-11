const { connect, StringCodec } = require('nats');

const NATS_URL = process.env.NATS_URL || 'localhost:14222';
const sc = StringCodec();

class TestNATSClient {
  constructor() {
    this.nc = null;
    this._subscriptions = [];
  }

  async connect() {
    this.nc = await connect({ servers: NATS_URL });
  }

  async disconnect() {
    if (this.nc) {
      await this.nc.drain();
    }
  }

  async publish(subject, payload) {
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    this.nc.publish(subject, sc.encode(data));
    await this.nc.flush();
  }

  async subscribe(subject) {
    const sub = this.nc.subscribe(subject);
    this._subscriptions.push(sub);
    return sub;
  }

  waitForMessage(subject, predicate = () => true, timeout = 10_000) {
    return new Promise(async (resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Timed out waiting for message on ${subject}`));
      }, timeout);

      const sub = this.nc.subscribe(subject);
      this._subscriptions.push(sub);

      (async () => {
        for await (const msg of sub) {
          let parsed;
          try {
            parsed = JSON.parse(sc.decode(msg.data));
          } catch {
            parsed = sc.decode(msg.data);
          }
          if (predicate(parsed, msg.subject)) {
            clearTimeout(timer);
            sub.unsubscribe();
            resolve(parsed);
            return;
          }
        }
      })();
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
    await this.publish(`agents.${name}.register`, payload);
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
    await this.publish(`agents.${name}.heartbeat`, payload);
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
    await this.publish(`agents.${name}.status`, payload);
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
    await this.publish(`agents.${name}.log`, payload);
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
    await this.publish(`tasks.${taskId}.assign`, payload);
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
    await this.publish(`tasks.${taskId}.progress`, payload);
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
    await this.publish(`tasks.${taskId}.complete`, payload);
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
    await this.publish(`tasks.${taskId}.failed`, payload);
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
    await this.publish(`agents.${receiver}.inbox`, msg);
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
    await this.publish('system.broadcast', msg);
  }
}

module.exports = { TestNATSClient };
