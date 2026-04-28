import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildRegisterEnvelope, buildHeartbeatEnvelope, validateEnvelope } from '../src/nats-session.js';

test('register envelope has canonical shape', () => {
  const env = buildRegisterEnvelope({
    agentId: 'openclaw-abc',
    sessionId: 'abc',
    heartbeatIntervalSec: 30
  });
  assert.equal(env.type, 'register');
  assert.equal(env.sender_id, 'openclaw-abc');
  assert.equal(env.v, 1);
  assert.equal(env.payload.name, 'openclaw-abc');
  assert.equal(env.payload.metadata['runtime.kind'], 'native');
  assert.equal(env.payload.metadata['runtime.heartbeat_interval_sec'], 30);
});

test('heartbeat envelope has canonical shape', () => {
  const env = buildHeartbeatEnvelope('openclaw-abc');
  assert.equal(env.type, 'heartbeat');
  assert.equal(env.sender_id, 'openclaw-abc');
  assert.ok(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(env.timestamp));
});

test('rejects legacy envelope with receiver_id', () => {
  const result = validateEnvelope({ v: 1, id: 'x', type: 'heartbeat',
                                    sender_id: 's', timestamp: '2026-04-23T10:00:00.000Z',
                                    payload: {}, receiver_id: 'legacy' });
  assert.equal(result.ok, false);
  assert.match(result.error, /receiver_id|additional/i);
});

test('accepts valid command envelope', () => {
  const result = validateEnvelope({
    v: 1, id: '11111111-2222-4333-8444-555555555555',
    type: 'command', sender_id: 'openclaw-abc', recipient_id: 'shell-1',
    task_id: '22222222-3333-4444-8555-666666666666',
    timestamp: '2026-04-23T10:00:00.000Z',
    payload: { body: 'echo hi' }
  });
  assert.equal(result.ok, true, result.error);
});
