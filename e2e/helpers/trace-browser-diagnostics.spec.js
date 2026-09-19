const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { monitorPage, captureFailure } = require('./trace-browser-diagnostics');

test('transport diagnostics retain bounded counters without sensitive payloads', () => {
  const page = new EventEmitter();
  const counters = monitorPage(page);
  const initialKeys = Object.keys(counters);
  const socket = new EventEmitter();
  page.emit('websocket', socket);
  for (let i = 0; i < 10000; i++) {
    page.emit('request', { url: 'secret-token' });
    socket.emit('framereceived', { payload: 'private-event' });
  }
  page.emit('requestfailed', { failure: 'private-url' });
  page.emit('response', { status: () => 503 });
  page.emit('response', { status: () => 200 });
  socket.emit('socketerror', 'secret-token');
  socket.emit('close');
  page.emit('crash');
  assert.deepEqual(Object.keys(counters), initialKeys);
  assert.equal(counters.requests, 10000);
  assert.equal(counters.received_frames, 10000);
  assert.equal(counters.last_error_status, 503);
  assert.equal(counters.error_responses, 1);
  assert.equal(counters.failed_requests, 1);
  assert.equal(counters.closed_sockets, 1);
  assert.equal(counters.socket_errors, 1);
  assert.equal(counters.crashed, true);
  assert.ok(Number.isFinite(counters.last_frame_ms));
  assert.doesNotMatch(JSON.stringify(counters), /secret|private/);
});

test('a lost page preserves sample, stage and counters without masking failure', async () => {
  const page = new EventEmitter();
  page.locator = () => ({ evaluate: async () => { throw new Error('private-error'); } });
  const diagnostics = monitorPage(page);
  page.emit('requestfailed');
  const result = await captureFailure([{ page, diagnostics, samples: 7 }], {
    stage: 'visible', sample: { event_id: 'event', lane: 0 },
  });
  assert.equal(result.stage, 'visible');
  assert.equal(result.sample.event_id, 'event');
  assert.equal(result.lanes[0].samples, 7);
  assert.equal(result.lanes[0].view, null);
  assert.equal(result.lanes[0].transport.failed_requests, 1);
  page.emit('requestfailed');
  assert.equal(result.lanes[0].transport.failed_requests, 1);
  assert.doesNotMatch(JSON.stringify(result), /private-error/);
});

test('a hung renderer cannot prevent the failure report', async () => {
  const page = new EventEmitter();
  page.locator = () => ({ evaluate: () => new Promise(() => {}) });
  const diagnostics = monitorPage(page);
  const result = await captureFailure([{ page, diagnostics, samples: 0 }], { stage: 'visible' }, 10);
  assert.equal(result.lanes[0].view, null);
  assert.equal(result.stage, 'visible');
});
