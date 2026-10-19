import { useEffect, useRef, useState } from 'react'
import { X, Copy, Check, Hash } from 'lucide-react'
import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import { fullTimestamp } from '../utils/formatTime'

const FIELD_ORDER = [
  'v',
  'id',
  'type',
  'sender_id',
  'recipient_id',
  'task_id',
  'context_id',
  'task_state',
  'agent_state',
  'hop_count',
  'timestamp',
  'deployment',
]

function PrettyJson({ value }) {
  const [copied, setCopied] = useState(false)
  const text =
    typeof value === 'string' ? value : JSON.stringify(value, null, 2)

  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    } catch {
      // browser restricted clipboard; ignore
    }
  }

  return (
    <div className="relative group">
      <button
        onClick={onCopy}
        className={clsx(
          'absolute top-1.5 right-1.5 z-10',
          'flex items-center gap-1 text-[10px] font-mono uppercase tracking-wider',
          'px-1.5 py-0.5 rounded',
          'bg-surface-200/70 hover:bg-surface-200 border border-surface-300/60',
          'text-gray-400 hover:text-gray-200',
          'transition-colors',
          'opacity-70 group-hover:opacity-100'
        )}
        title="Copy as JSON"
      >
        {copied ? <Check size={10} /> : <Copy size={10} />}
        {copied ? 'copied' : 'copy'}
      </button>
      <pre
        className={clsx(
          'text-[11.5px] leading-relaxed',
          'font-mono text-gray-300',
          'bg-surface-50/80 border border-surface-200/60 rounded-md',
          'px-3 py-2.5',
          'whitespace-pre-wrap break-words',
          'max-h-[60vh] overflow-y-auto'
        )}
      >
        {text}
      </pre>
    </div>
  )
}

function FieldRow({ label, value, mono = false, accent }) {
  if (value === undefined || value === null || value === '') return null
  return (
    <div className="grid grid-cols-[112px_1fr] items-baseline gap-3 py-1 border-b border-surface-100/80 last:border-0">
      <span className="text-[10px] uppercase tracking-[0.14em] text-gray-500 font-medium">
        {label}
      </span>
      <span
        className={clsx(
          'text-[12px] text-gray-200 break-all',
          mono && 'font-mono text-[11.5px]'
        )}
        style={accent ? { color: accent } : undefined}
      >
        {String(value)}
      </span>
    </div>
  )
}

export default function MessageInspector({ message, onClose }) {
  const panelRef = useRef(null)

  // Esc to close
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  // Body scroll lock + focus on open
  useEffect(() => {
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    panelRef.current?.focus()
    return () => {
      document.body.style.overflow = prev
    }
  }, [])

  const senderColor = getAgentColor(message.sender_id)
  const recipientColor = message.recipient_id
    ? getAgentColor(message.recipient_id)
    : null

  // Pull body / response / message preview to a top-of-panel block
  const payload = message.payload || {}
  const bodyText =
    payload.body ?? payload.response ?? payload.message ?? null

  // Compose the "raw envelope" without the deployment column quirks
  const rawEnvelope = { ...message }
  // Friendly key order: known fields first, payload last
  const ordered = {}
  for (const k of FIELD_ORDER) {
    if (k in rawEnvelope) ordered[k] = rawEnvelope[k]
  }
  for (const k of Object.keys(rawEnvelope)) {
    if (!(k in ordered) && k !== 'payload') ordered[k] = rawEnvelope[k]
  }
  ordered.payload = rawEnvelope.payload

  return (
    <div
      onClick={onClose}
      className="fixed inset-0 z-50 flex justify-end bg-black/55 backdrop-blur-[2px] animate-[fadeIn_140ms_ease-out]"
      style={{
        // local @keyframes via Tailwind arbitrary props would be cleaner;
        // inline below as a small JS-driven fallback.
      }}
    >
      <div
        ref={panelRef}
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
        className={clsx(
          'h-full w-full sm:w-[min(640px,90vw)]',
          'bg-surface-100/95 border-l border-surface-300/70',
          'shadow-[0_0_60px_-10px_rgba(0,0,0,0.6)]',
          'flex flex-col',
          'animate-[slideIn_180ms_cubic-bezier(0.22,1,0.36,1)]',
          'focus:outline-none'
        )}
        style={{
          // 3px sender-colored stripe down the inner edge for visual continuity
          boxShadow: `inset 3px 0 0 0 ${senderColor}`,
        }}
      >
        {/* Header */}
        <div className="flex items-center gap-2.5 px-4 py-3 border-b border-surface-200/80 shrink-0">
          <Hash size={13} className="text-gray-500 shrink-0" />
          <span
            className="font-mono text-[11.5px] tracking-tight truncate"
            style={{ color: senderColor }}
            title={message.sender_id}
          >
            {message.sender_id || 'unknown'}
          </span>
          {recipientColor && (
            <>
              <span className="text-gray-600 text-[10px]">→</span>
              <span
                className="font-mono text-[11.5px] tracking-tight truncate"
                style={{ color: recipientColor }}
                title={message.recipient_id}
              >
                {message.recipient_id}
              </span>
            </>
          )}
          <span className="text-[10px] uppercase tracking-[0.18em] text-gray-500 ml-1">
            {message.type}
          </span>
          <button
            onClick={onClose}
            className="ml-auto text-gray-500 hover:text-gray-200 transition-colors p-1 -mr-1"
            title="Close (Esc)"
            aria-label="Close inspector"
          >
            <X size={16} />
          </button>
        </div>

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto">
          {/* Body / response section */}
          {bodyText && (
            <section className="px-4 pt-4 pb-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500 mb-1.5">
                Content
              </div>
              <div className="text-[13px] text-gray-200 leading-relaxed whitespace-pre-wrap break-words font-mono bg-surface-50/60 border border-surface-200/60 rounded-md px-3 py-2.5">
                {String(bodyText)}
              </div>
            </section>
          )}

          {/* Metadata grid */}
          <section className="px-4 pt-3 pb-3">
            <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500 mb-1.5">
              Envelope
            </div>
            <div className="bg-surface-50/40 border border-surface-200/60 rounded-md px-3 py-1.5">
              <FieldRow label="id" value={message.id} mono />
              <FieldRow label="type" value={message.type} mono />
              <FieldRow
                label="sender"
                value={message.sender_id}
                mono
                accent={senderColor}
              />
              <FieldRow
                label="recipient"
                value={message.recipient_id}
                mono
                accent={recipientColor}
              />
              <FieldRow label="task_id" value={message.task_id} mono />
              <FieldRow label="context_id" value={message.context_id} mono />
              <FieldRow label="task_state" value={message.task_state} />
              <FieldRow label="agent_state" value={message.agent_state} />
              <FieldRow label="hop_count" value={message.hop_count} mono />
              <FieldRow
                label="timestamp"
                value={fullTimestamp(message.timestamp)}
                mono
              />
              <FieldRow label="deployment" value={message.deployment} />
              <FieldRow label="v" value={message.v} mono />
            </div>
          </section>

          {/* Raw envelope JSON */}
          <section className="px-4 pt-3 pb-5">
            <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500 mb-1.5">
              Raw envelope
            </div>
            <PrettyJson value={ordered} />
          </section>

          {/* Standalone payload preview when not already shown above */}
          {!bodyText && payload && Object.keys(payload).length > 0 && (
            <section className="px-4 pt-1 pb-5">
              <div className="text-[10px] uppercase tracking-[0.18em] text-gray-500 mb-1.5">
                Payload
              </div>
              <PrettyJson value={payload} />
            </section>
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2.5 border-t border-surface-200/70 text-[10px] uppercase tracking-[0.18em] text-gray-500 shrink-0 flex items-center justify-between">
          <span className="font-mono normal-case tracking-tight">
            esc to close
          </span>
          {message.task_id && (
            <span
              className="font-mono normal-case tracking-tight text-gray-600"
              title={`task_id: ${message.task_id}`}
            >
              t:{message.task_id.slice(0, 12)}
            </span>
          )}
        </div>
      </div>

      {/* Local keyframes — Tailwind doesn't ship these by default */}
      <style>{`
        @keyframes fadeIn { from { opacity: 0 } to { opacity: 1 } }
        @keyframes slideIn {
          from { transform: translateX(24px); opacity: 0 }
          to   { transform: translateX(0);    opacity: 1 }
        }
      `}</style>
    </div>
  )
}
