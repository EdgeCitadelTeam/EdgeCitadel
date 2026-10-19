import { useState } from 'react'
import clsx from 'clsx'
import {
  ArrowRight,
  Terminal,
  CheckCircle,
  AlertTriangle,
  Info,
  Radio,
  Zap,
  Activity,
  XCircle,
  ChevronDown,
  ChevronUp,
  Maximize2,
} from 'lucide-react'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'
import MessageInspector from './MessageInspector'

const typeConfig = {
  command: { icon: Terminal, color: 'text-blue-400' },
  result: { icon: CheckCircle, color: 'text-emerald-400' },
  status: { icon: Info, color: 'text-amber-400' },
  log: { icon: Info, color: 'text-amber-400' },
  delegation: { icon: Zap, color: 'text-indigo-400' },
  cancel: { icon: XCircle, color: 'text-red-400' },
  broadcast: { icon: Radio, color: 'text-cyan-400' },
  'task.progress': { icon: Activity, color: 'text-yellow-400' },
  // Synthetic streaming bubble (appStore.appendStreamDelta) — distinct
  // visual treatment from RESULT so the in-flight state is unambiguous.
  streaming: { icon: Activity, color: 'text-amber-300' },
  register: { icon: Info, color: 'text-gray-400' },
  heartbeat: { icon: Info, color: 'text-gray-400' },
}

// A2A task-state badge colors
const taskStateColors = {
  submitted: 'bg-gray-500/20 text-gray-300',
  working: 'bg-yellow-500/20 text-yellow-300',
  'input-required': 'bg-purple-500/20 text-purple-300',
  completed: 'bg-green-500/20 text-green-300',
  failed: 'bg-red-500/20 text-red-300',
  canceled: 'bg-gray-500/20 text-gray-400',
  rejected: 'bg-red-500/20 text-red-400',
  'auth-required': 'bg-orange-500/20 text-orange-300',
}

function skillBadgeClass(skillId) {
  switch (skillId) {
    case 'reasoning.chat':   return 'bg-skill-chat-bg text-skill-chat-text'
    case 'text.summarize':   return 'bg-skill-summarize-bg text-skill-summarize-text'
    case 'text.classify':    return 'bg-skill-classify-bg text-skill-classify-text'
    case 'code.explain':     return 'bg-skill-explain-bg text-skill-explain-text'
    default:                 return 'bg-surface-200 text-gray-400'
  }
}

// Inline preview thresholds. Generous enough that short LLM replies don't
// trigger an expander; tight enough that 12b paragraphs do.
const PREVIEW_LINE_LIMIT = 6
const PREVIEW_CHAR_LIMIT = 480

// Extract human-readable content from the message payload
function extractContent(payload) {
  if (!payload || typeof payload !== 'object') return null

  if (payload.message) return payload.message
  if (payload.content) return payload.content
  if (payload.response) return payload.response
  if (payload.body) return payload.body

  if (payload.result) {
    if (typeof payload.result === 'string') return payload.result
    if (typeof payload.result === 'object') {
      if (payload.result.response) return payload.result.response
      return Object.entries(payload.result)
        .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${v}`)
        .join('\n')
    }
  }

  if (payload.command) {
    const parts = [payload.command]
    if (payload.payload) {
      if (typeof payload.payload === 'string') {
        parts.push(payload.payload)
      } else if (payload.payload.message) {
        parts.push(payload.payload.message)
      } else {
        const details = Object.entries(payload.payload)
          .map(([k, v]) => `${k.replace(/_/g, ' ')}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
          .join(', ')
        if (details) parts.push(details)
      }
    }
    return parts.join('\n')
  }

  if (payload.status && payload.status !== 'success') return `Status: ${payload.status}`

  return null
}

// Build a clipped preview when content is long. Returns either the full
// content (when short) or a truncated string + a one-line description of
// what was hidden.
function buildPreview(content) {
  if (!content) return { preview: null, isClipped: false, hiddenLabel: null }
  const text = String(content)
  const lines = text.split('\n')
  const overLines = lines.length > PREVIEW_LINE_LIMIT
  const overChars = text.length > PREVIEW_CHAR_LIMIT
  if (!overLines && !overChars) {
    return { preview: text, isClipped: false, hiddenLabel: null }
  }
  let preview
  if (overLines) {
    preview = lines.slice(0, PREVIEW_LINE_LIMIT).join('\n')
  } else {
    preview = text.slice(0, PREVIEW_CHAR_LIMIT)
  }
  const hiddenLines = lines.length - preview.split('\n').length
  const hiddenChars = text.length - preview.length
  // Prefer the more meaningful unit; mention chars when the content is mostly
  // one wrapped paragraph rather than many short lines.
  const hiddenLabel =
    hiddenLines >= 2
      ? `+${hiddenLines} lines`
      : `+${hiddenChars.toLocaleString()} chars`
  return { preview, isClipped: true, hiddenLabel }
}

export default function MessageBubble({ message, highlighted, onClick, commandSkillId }) {
  const [expanded, setExpanded] = useState(false)
  const [inspecting, setInspecting] = useState(false)

  const type = message.type || 'unknown'
  // While the synthetic streaming bubble is live (appStore.appendStreamDelta
  // builds it with type='result' so finalizeStream can swap-in-place), wear
  // a STREAMING badge instead of RESULT — the underlying type is structural,
  // but the user-facing label should reflect the in-flight state.
  const displayType = message.streaming && !message.stalled ? 'streaming' : type
  const config = typeConfig[displayType] || typeConfig[type] || { icon: Info, color: 'text-gray-400' }
  const Icon = config.icon
  const senderColor = getAgentColor(message.sender_id)
  const content = extractContent(message.payload) ?? message.content ?? null
  const taskState = message.task_state

  const { preview, isClipped, hiddenLabel } = buildPreview(content)
  const showFull = expanded || !isClipped
  const displayed = showFull ? content : preview

  const stop = (e) => {
    e.stopPropagation()
  }

  return (
    <>
      <div
        onClick={onClick}
        data-task-id={message.task_id || ''}
        data-message-type={type}
        data-task-state={taskState || ''}
        className={clsx(
          'rounded-lg border px-3 py-2 transition-all cursor-pointer border-l-[3px] relative',
          highlighted && 'ring-1 ring-accent/40 shadow-sm shadow-accent/10',
          'hover:brightness-125'
        )}
        style={{
          borderLeftColor: senderColor,
          backgroundColor: `${senderColor}18`,
          borderColor: highlighted ? undefined : `${senderColor}35`,
        }}
      >
        {/* Header: sender -> recipient, type icon, timestamp, inspect */}
        <div className="flex items-center gap-2 min-w-0">
          <Icon size={13} className={clsx(config.color, 'shrink-0')} />
          <span
            className="font-medium text-xs truncate"
            style={{ color: senderColor }}
          >
            {message.sender_id || 'unknown'}
          </span>
          {message.recipient_id && (
            <>
              <ArrowRight size={10} className="text-gray-600 shrink-0" />
              <span
                className="text-xs truncate"
                style={{ color: getAgentColor(message.recipient_id) }}
              >
                {message.recipient_id}
              </span>
            </>
          )}
          <span
            className={clsx(
              'text-[10px] font-medium uppercase shrink-0 ml-1',
              config.color
            )}
          >
            {displayType}
          </span>
          {taskState && (
            <span
              className={clsx(
                'text-[10px] font-medium px-1.5 py-0.5 rounded shrink-0',
                taskStateColors[taskState] || 'bg-surface-200 text-gray-400'
              )}
            >
              {taskState}
            </span>
          )}
          {(message.skill_id || message.payload?.skill_id || commandSkillId) && (
            <span
              className={clsx(
                'ml-2 inline-block px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0',
                skillBadgeClass(message.skill_id || message.payload?.skill_id || commandSkillId)
              )}
            >
              {message.skill_id || message.payload?.skill_id || commandSkillId}
            </span>
          )}
          <button
            onClick={(e) => {
              stop(e)
              setInspecting(true)
            }}
            className={clsx(
              'ml-auto shrink-0 p-1 -mr-1 rounded',
              'text-gray-600 hover:text-gray-200 hover:bg-surface-200/40',
              'opacity-60 hover:opacity-100',
              'transition-all'
            )}
            title="Inspect full envelope"
            aria-label="Inspect full envelope"
          >
            <Maximize2 size={11} />
          </button>
          <span
            className="text-[10px] text-gray-600 shrink-0"
            title={fullTimestamp(message.timestamp)}
          >
            {relativeTime(message.timestamp)}
          </span>
        </div>

        {/* Content with truncation affordance */}
        {displayed && (
          <div
            className={clsx(
              'mt-1.5 text-[13px] text-gray-300 leading-relaxed',
              'whitespace-pre-wrap break-words',
              'relative'
            )}
          >
            {displayed}

            {message.streaming && !message.stalled && (
              <span className="ml-1 inline-block animate-pulse text-gray-400">▍</span>
            )}
            {message.stalled && (
              <span className="ml-1 text-xs text-amber-400">(stream interrupted)</span>
            )}

            {/* Bottom fade — bubble-tinted so it blends with the
                sender-colored background; only when collapsed. */}
            {isClipped && !expanded && (
              <div
                className="pointer-events-none absolute inset-x-0 bottom-0 h-7"
                style={{
                  background: `linear-gradient(to top, ${senderColor}26, transparent)`,
                }}
              />
            )}
          </div>
        )}

        {/* Truncation toolbar — only when content is long */}
        {isClipped && (
          <div className="mt-1.5 flex items-center gap-2 relative">
            <button
              onClick={(e) => {
                stop(e)
                setExpanded((v) => !v)
              }}
              className={clsx(
                'inline-flex items-center gap-1',
                'text-[10px] font-mono uppercase tracking-[0.12em]',
                'px-1.5 py-0.5 rounded',
                'border border-surface-300/70',
                'bg-surface-200/50 hover:bg-surface-200',
                'text-gray-300 hover:text-gray-100',
                'transition-colors'
              )}
              title={expanded ? 'Collapse' : 'Show full message'}
            >
              {expanded ? (
                <>
                  <ChevronUp size={10} />
                  collapse
                </>
              ) : (
                <>
                  <ChevronDown size={10} />
                  show more · {hiddenLabel}
                </>
              )}
            </button>
            <button
              onClick={(e) => {
                stop(e)
                setInspecting(true)
              }}
              className={clsx(
                'inline-flex items-center gap-1',
                'text-[10px] font-mono uppercase tracking-[0.12em]',
                'px-1.5 py-0.5 rounded',
                'text-gray-500 hover:text-gray-200',
                'transition-colors'
              )}
              title="Open inspector with raw envelope"
            >
              <Maximize2 size={10} />
              inspect
            </button>
          </div>
        )}

        {/* Task / context badges */}
        {(message.task_id || message.context_id) && (
          <div className="mt-1.5 flex items-center gap-1.5 flex-wrap">
            {message.task_id && (
              <span
                className="text-[10px] text-gray-600 bg-surface-200/50 px-1.5 py-0.5 rounded font-mono"
                title={`task_id: ${message.task_id}`}
              >
                t:{message.task_id.slice(0, 8)}
              </span>
            )}
            {message.context_id && (
              <span
                className="text-[10px] text-gray-600 bg-surface-200/30 px-1.5 py-0.5 rounded font-mono"
                title={`context_id: ${message.context_id}`}
              >
                c:{message.context_id.slice(0, 8)}
              </span>
            )}
          </div>
        )}
      </div>

      {inspecting && (
        <MessageInspector
          message={message}
          onClose={() => setInspecting(false)}
        />
      )}
    </>
  )
}
