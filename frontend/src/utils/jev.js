// Coordinator status is a snapshot with child references, not an LLM text delta.
export const isJevProgress = message => message.type === 'task.progress'
  && message.sender_id === 'jev' && typeof message.payload?.run_id === 'string'

export function createJevRequestId() {
  // getRandomValues is available on the private HTTP dashboard as well as HTTPS.
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  bytes[6] = (bytes[6] & 0x0f) | 0x40
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
