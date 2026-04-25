import { useState, useEffect, useCallback } from 'react'
import { X, RefreshCw } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import TaskCard from './TaskCard'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

// A2A task-state enum from the v0.1 envelope schema. Items without task_state
// (e.g. a freshly accepted command not yet observed working) bucket into
// 'submitted' for display.
const COLUMNS = [
  { key: 'submitted', label: 'Submitted', color: 'text-gray-400' },
  { key: 'working', label: 'Working', color: 'text-yellow-400' },
  { key: 'input-required', label: 'Input Required', color: 'text-purple-400' },
  { key: 'completed', label: 'Completed', color: 'text-green-400' },
  { key: 'failed', label: 'Failed', color: 'text-red-400' },
  { key: 'canceled', label: 'Canceled', color: 'text-gray-500' },
  { key: 'rejected', label: 'Rejected', color: 'text-red-500' },
  { key: 'auth-required', label: 'Auth Required', color: 'text-orange-400' },
]

// Roll a list of envelopes (messages) into tasks keyed by task_id. The
// most-recent task_state (preferring result > task.progress > command)
// becomes the displayed state.
function deriveTasks(messages) {
  const byTask = new Map()
  for (const m of messages) {
    if (!m.task_id) continue
    if (!byTask.has(m.task_id)) {
      byTask.set(m.task_id, {
        task_id: m.task_id,
        context_id: m.context_id || null,
        sender_id: m.sender_id,
        recipient_id: m.recipient_id || null,
        first_ts: m.timestamp,
        last_ts: m.timestamp,
        task_state: 'submitted',
        body: null,
        result: null,
        last_payload: null,
      })
    }
    const t = byTask.get(m.task_id)
    if (new Date(m.timestamp) < new Date(t.first_ts)) t.first_ts = m.timestamp
    if (new Date(m.timestamp) > new Date(t.last_ts)) t.last_ts = m.timestamp
    if (m.type === 'command') {
      t.body = m.payload?.body || t.body
      t.recipient_id = m.recipient_id || t.recipient_id
      t.sender_id = m.sender_id || t.sender_id
    }
    if (m.task_state) t.task_state = m.task_state
    if (m.type === 'result') {
      t.result = m.payload
      // result envelope's task_state is authoritative if present
      if (m.task_state) t.task_state = m.task_state
    }
    t.last_payload = m.payload
  }
  return [...byTask.values()].sort(
    (a, b) => new Date(b.last_ts) - new Date(a.last_ts)
  )
}

export default function TaskBoard() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const [tasks, setTasks] = useState([])
  const [selectedTask, setSelectedTask] = useState(null)
  const [trace, setTrace] = useState([])

  const fetchTasks = useCallback(async () => {
    try {
      const params = { limit: 500 }
      if (selectedAgent) params.agent_id = selectedAgent
      const items = (await api.queryMessages(params)) || []
      setTasks(deriveTasks(items))
    } catch {
      // ignore
    }
  }, [selectedAgent])

  useEffect(() => {
    fetchTasks()
    const interval = setInterval(fetchTasks, 5000)
    return () => clearInterval(interval)
  }, [fetchTasks])

  const handleSelectTask = async (task) => {
    setSelectedTask(task)
    try {
      const items = await api.queryMessages({ task_id: task.task_id, limit: 200 })
      setTrace(items || [])
    } catch {
      setTrace([])
    }
  }

  const grouped = {}
  for (const col of COLUMNS) {
    grouped[col.key] = tasks.filter((t) => t.task_state === col.key)
  }

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 p-2 border-b border-surface-200">
        <button
          onClick={fetchTasks}
          className="bg-surface-100 hover:bg-surface-200 text-gray-300 px-3 py-1 rounded text-xs flex items-center gap-1"
        >
          <RefreshCw size={12} />
          Refresh
        </button>
        <span className="text-xs text-gray-500 ml-2">
          {tasks.length} task{tasks.length === 1 ? '' : 's'}
        </span>
      </div>

      {/* Kanban */}
      <div className="flex-1 overflow-x-auto p-3">
        <div className="flex gap-3 min-h-full">
          {COLUMNS.map((col) => (
            <div
              key={col.key}
              className="w-44 md:w-56 shrink-0 bg-surface/50 rounded-lg"
            >
              <div className="flex items-center gap-2 p-2 border-b border-surface-200">
                <span className={clsx('text-xs font-medium', col.color)}>
                  {col.label}
                </span>
                <span className="bg-surface-200 text-gray-400 px-1.5 py-0.5 rounded-full text-[10px]">
                  {grouped[col.key].length}
                </span>
              </div>
              <div className="p-2 space-y-2">
                {grouped[col.key].map((task) => (
                  <TaskCard
                    key={task.task_id}
                    task={task}
                    onClick={() => handleSelectTask(task)}
                  />
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Task detail panel */}
      {selectedTask && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-surface-50 border border-surface-200 rounded-lg p-4 w-full max-w-lg max-h-[80vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-200 truncate mr-2 font-mono">
                {selectedTask.task_id.slice(0, 16)}...
              </h3>
              <button onClick={() => setSelectedTask(null)} className="shrink-0">
                <X size={16} className="text-gray-500" />
              </button>
            </div>
            <div className="space-y-2 text-xs">
              <div className="flex flex-wrap gap-3">
                <div>
                  <span className="text-gray-500">State:</span>{' '}
                  <span className="text-gray-300">{selectedTask.task_state}</span>
                </div>
                <div>
                  <span className="text-gray-500">From:</span>{' '}
                  <span className="text-gray-300">{selectedTask.sender_id}</span>
                </div>
                <div>
                  <span className="text-gray-500">To:</span>{' '}
                  <span className="text-gray-300">
                    {selectedTask.recipient_id || '—'}
                  </span>
                </div>
              </div>
              {selectedTask.context_id && (
                <div>
                  <span className="text-gray-500">Context:</span>{' '}
                  <span className="text-gray-300 font-mono">
                    {selectedTask.context_id}
                  </span>
                </div>
              )}
              {selectedTask.body && (
                <div>
                  <span className="text-gray-500">Body:</span>
                  <pre className="text-gray-400 mt-1 bg-surface-100 p-2 rounded whitespace-pre-wrap break-all">
                    {selectedTask.body}
                  </pre>
                </div>
              )}
              <div className="flex flex-wrap gap-3 text-gray-500">
                <span title={fullTimestamp(selectedTask.first_ts)}>
                  Created: {relativeTime(selectedTask.first_ts)}
                </span>
                <span title={fullTimestamp(selectedTask.last_ts)}>
                  Updated: {relativeTime(selectedTask.last_ts)}
                </span>
              </div>
              {selectedTask.result && (
                <div>
                  <span className="text-gray-500">Result:</span>
                  <pre className="text-gray-400 mt-1 bg-surface-100 p-2 rounded whitespace-pre-wrap break-all max-h-40 overflow-y-auto">
                    {JSON.stringify(selectedTask.result, null, 2)}
                  </pre>
                </div>
              )}

              {/* Message trace */}
              {trace.length > 0 && (
                <div className="mt-3 pt-3 border-t border-surface-200">
                  <h4 className="text-gray-400 font-medium mb-2">
                    Message Trace ({trace.length})
                  </h4>
                  <div className="space-y-1">
                    {trace.map((msg) => (
                      <div
                        key={msg.id}
                        className="bg-surface-100 p-2 rounded text-gray-400"
                      >
                        <span className="text-gray-300">{msg.sender_id}</span>
                        {msg.recipient_id && ` → ${msg.recipient_id}`}
                        <span className="text-gray-500 ml-2">[{msg.type}]</span>
                        {msg.task_state && (
                          <span className="text-gray-500 ml-2">
                            {msg.task_state}
                          </span>
                        )}
                        <span className="text-gray-600 ml-2">
                          {relativeTime(msg.timestamp)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
