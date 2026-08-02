const WebSocket = require('ws');

if (!process.env.WS_BASE_URL) {
  throw new Error('WS_BASE_URL is required');
}

const WS_BASE_URL = process.env.WS_BASE_URL;

class TestWSClient {
  constructor() {
    this.ws = null;
    this.events = [];
    this._listeners = [];
  }

  async connect(path = '/stream') {
    const url = `${WS_BASE_URL}${path}`;
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(url);
      const timeout = setTimeout(() => {
        reject(new Error(`WebSocket connection timed out: ${url}`));
      }, 10_000);

      this.ws.on('open', () => {
        clearTimeout(timeout);
        resolve();
      });

      this.ws.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });

      this.ws.on('message', (data) => {
        let parsed;
        try {
          parsed = JSON.parse(data.toString());
        } catch {
          parsed = data.toString();
        }
        this.events.push(parsed);
        for (const listener of this._listeners) {
          listener(parsed);
        }
      });
    });
  }

  waitForEvent(predicate, timeout = 15_000) {
    // Check already received events
    for (const event of this.events) {
      if (predicate(event)) {
        return Promise.resolve(event);
      }
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timed out waiting for WebSocket event'));
      }, timeout);

      const listener = (event) => {
        if (predicate(event)) {
          cleanup();
          resolve(event);
        }
      };

      const cleanup = () => {
        clearTimeout(timer);
        const idx = this._listeners.indexOf(listener);
        if (idx >= 0) this._listeners.splice(idx, 1);
      };

      this._listeners.push(listener);
    });
  }

  clearEvents() {
    this.events = [];
  }

  async disconnect() {
    this._listeners = [];
    if (this.ws) {
      return new Promise((resolve) => {
        this.ws.on('close', () => resolve());
        this.ws.close();
        // Force resolve if close doesn't fire quickly
        setTimeout(resolve, 2000);
      });
    }
  }
}

module.exports = { TestWSClient };
