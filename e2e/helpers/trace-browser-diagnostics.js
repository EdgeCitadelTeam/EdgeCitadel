// Fixed-size counters only: never retain URLs, headers, payloads or error text.
function monitorPage(page) {
  const counters = {
    requests: 0, failed_requests: 0, error_responses: 0, last_error_status: null,
    sockets: 0, closed_sockets: 0, socket_errors: 0, received_frames: 0,
    last_frame_ms: null, crashed: false,
  };
  page.on('request', () => counters.requests++);
  page.on('requestfailed', () => counters.failed_requests++);
  page.on('response', response => {
    if (response.status() >= 400) {
      counters.error_responses++;
      counters.last_error_status = response.status();
    }
  });
  page.on('crash', () => { counters.crashed = true; });
  page.on('websocket', socket => {
    counters.sockets++;
    socket.on('close', () => counters.closed_sockets++);
    socket.on('socketerror', () => counters.socket_errors++);
    socket.on('framereceived', () => {
      counters.received_frames++;
      counters.last_frame_ms = performance.now();
    });
  });
  return counters;
}

async function captureFailure(lanes, context, timeoutMs = 2000) {
  const snapshots = [];
  for (const lane of lanes) {
    let view = null;
    let timer;
    try {
      // Locator timeout alone does not bound an unresponsive renderer's evaluate.
      const capture = lane.page.locator('body').evaluate((body, nodeId) => {
        const modes = ['loading', 'live', 'reconnecting', 'historical', 'error',
          'inaccessible', 'resnapshot required', 'closed'];
        const mode = body.querySelector('.trace-run-heading strong')?.textContent?.trim().toLowerCase();
        const node = Array.from(body.querySelectorAll('[data-node-id]'))
          .find(element => element.getAttribute('data-node-id') === nodeId);
        const states = ['running', 'finished', 'failed', 'cancelled', 'unknown'];
        const state = states.find(value => node?.classList.contains('state-' + value)) || null;
        const filter = Array.from(body.querySelectorAll('input')).find(input =>
          Array.from(input.labels || []).some(label => label.textContent.trim() === 'Find a step'));
        return {
          session_mode: modes.includes(mode) ? mode : 'other',
          target_present: Boolean(node), target_state: state,
          filter_matches_target: filter ? filter.value === nodeId : null,
          alert_count: body.querySelectorAll('[role="alert"]').length,
          document_visibility: document.visibilityState,
        };
      }, context.sample?.node_id || null, { timeout: timeoutMs });
      view = await Promise.race([capture, new Promise(resolve => {
        timer = setTimeout(() => resolve(null), timeoutMs);
      })]);
    } catch { /* Unavailable view is explicit; transport counters remain useful. */ }
    finally { clearTimeout(timer); }
    snapshots.push({ samples: lane.samples, transport: { ...lane.diagnostics }, view });
  }
  return { ...context, captured_ms: performance.now(), lanes: snapshots };
}

module.exports = { monitorPage, captureFailure };
