import clsx from 'clsx'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime } from '../utils/formatTime'

const priorityColors = {
  critical: 'border-l-red-500',
  high: 'border-l-orange-500',
  normal: 'border-l-blue-500',
  low: 'border-l-gray-500',
}

export default function TaskCard({ task, onClick }) {
  return (
    <div
      onClick={onClick}
      className={clsx(
        'bg-surface-50 border border-surface-200 rounded-lg p-3 cursor-pointer',
        'hover:border-surface-300 transition-colors border-l-2',
        priorityColors[task.priority] || priorityColors.normal
      )}
    >
      <h4 className="text-sm font-medium text-gray-200 mb-1 truncate">
        {task.title || 'Untitled'}
      </h4>
      {task.description && (
        <p className="text-xs text-gray-500 mb-2 line-clamp-2">
          {task.description}
        </p>
      )}
      <div className="flex items-center gap-2 flex-wrap">
        {task.assigned_agent && (
          <span
            className="text-[10px] px-1.5 py-0.5 rounded font-medium text-white"
            style={{ backgroundColor: getAgentColor(task.assigned_agent) }}
          >
            {task.assigned_agent}
          </span>
        )}
        <span className="text-[10px] text-gray-500 ml-auto">
          {relativeTime(task.created_at)}
        </span>
      </div>
    </div>
  )
}
