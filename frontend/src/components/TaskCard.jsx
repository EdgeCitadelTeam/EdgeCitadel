import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime } from '../utils/formatTime'

// Border accent per A2A task_state.
const stateBorder = {
  submitted: 'border-l-gray-500',
  working: 'border-l-yellow-500',
  'input-required': 'border-l-purple-500',
  completed: 'border-l-green-500',
  failed: 'border-l-red-500',
  canceled: 'border-l-gray-600',
  rejected: 'border-l-red-600',
  'auth-required': 'border-l-orange-500',
}

// task shape (derived in TaskBoard.deriveTasks):
//   { task_id, context_id, sender_id, recipient_id, first_ts, last_ts,
//     task_state, body, result }
export default function TaskCard({ task, onClick }) {
  const recipient = task.recipient_id
  const body = task.body
  return (
    <div
      onClick={onClick}
      className={clsx(
        'bg-surface-50 border border-surface-200 rounded-lg p-3 cursor-pointer',
        'hover:border-surface-300 transition-colors border-l-2',
        stateBorder[task.task_state] || stateBorder.submitted
      )}
    >
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-[10px] text-gray-500 font-mono">
          {task.task_id?.slice(0, 8)}
        </span>
        {task.context_id && (
          <span
            className="text-[10px] text-gray-600 font-mono"
            title={`context: ${task.context_id}`}
          >
            · c:{task.context_id.slice(0, 6)}
          </span>
        )}
      </div>
      {body && (
        <p className="text-xs text-gray-300 mb-2 line-clamp-2">{body}</p>
      )}
      <div className="flex items-center gap-2 flex-wrap">
        {recipient && (
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-medium text-white"
            style={{ backgroundColor: getAgentColor(recipient) }}
          >
            {recipient}
          </span>
        )}
        <span className="text-[10px] text-gray-500 ml-auto">
          {relativeTime(task.last_ts)}
        </span>
      </div>
    </div>
  )
}
