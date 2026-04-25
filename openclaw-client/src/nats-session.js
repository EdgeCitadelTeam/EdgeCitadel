import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { v4 as uuid } from 'uuid';
import Ajv from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

const HERE = dirname(fileURLToPath(import.meta.url));
const SCHEMA_PATH = resolve(HERE, '../../schemas/envelope.v1.json');
const schema = JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const envelopeValidator = ajv.compile(schema);

export function nowIso() {
  return new Date().toISOString().replace(/Z$/, 'Z');   // already .sssZ
}

export function validateEnvelope(env) {
  const ok = envelopeValidator(env);
  if (ok) return { ok: true };
  return { ok: false,
           error: (envelopeValidator.errors || [])
             .map(e => `${e.instancePath || '(root)'} ${e.message}`)
             .join('; ') };
}

export function buildRegisterEnvelope({ agentId, sessionId,
                                        heartbeatIntervalSec = 30,
                                        description = 'Browser-side openclaw client.' }) {
  const card = {
    name: agentId,
    description,
    version: '0.1.0',
    url: `nats://edgecitadel/agents.${agentId}.inbox`,
    provider: { organization: 'EdgeCitadel', url: 'https://edgecitadel.local' },
    capabilities: {
      streaming: false,
      extensions: [{
        uri: 'https://edgecitadel.local/ext/nats-binding/v1',
        description: 'NATS binding via aggregator-mediated publish.',
        required: false,
        params: { subject_prefix: `agents.${agentId}` }
      }]
    },
    securitySchemes: {},
    skills: [{ id: 'openclaw.chat', name: 'chat',
               description: 'Send commands to fleet via aggregator.',
               tags: ['browser'] }],
    defaultInputModes: ['text/plain'],
    defaultOutputModes: ['text/plain'],
    metadata: {
      'runtime.kind': 'native',
      'runtime.roles': ['orchestrator'],
      'runtime.tags': ['openclaw', 'browser', `session:${sessionId}`],
      'runtime.heartbeat_interval_sec': heartbeatIntervalSec
    }
  };
  return {
    v: 1, id: uuid(), type: 'register',
    sender_id: agentId, timestamp: nowIso(), payload: card
  };
}

export function buildHeartbeatEnvelope(agentId) {
  return {
    v: 1, id: uuid(), type: 'heartbeat',
    sender_id: agentId, timestamp: nowIso(), payload: {}
  };
}

export function buildStatusEnvelope(agentId, state, reason) {
  return {
    v: 1, id: uuid(), type: 'status',
    sender_id: agentId, agent_state: state, timestamp: nowIso(),
    payload: reason ? { reason } : {}
  };
}

export function buildCommandEnvelope({ senderId, recipientId, body, args }) {
  return {
    v: 1, id: uuid(), type: 'command',
    sender_id: senderId, recipient_id: recipientId,
    task_id: uuid(), timestamp: nowIso(),
    payload: { body, ...(args ? { args } : {}) }
  };
}
