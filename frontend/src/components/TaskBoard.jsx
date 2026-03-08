import { useState, useEffect, useCallback } from 'react'
import { Plus, X } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { taskApi } from '../api/client'
import TaskCard from './TaskCard'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

const COLUMNS = [
  { key: 'pending', label: 'Pending', color: 'text-gray-400' },
  { key: 'assigned', label: 'Assigned', color: 'text-blue-400' },
  { key: 'running', label: 'Running', color: 'text-yellow-400' },
  { key: 'completed', label: 'Completed', color: 'text-green-400' },
  { key: 'failed', label: 'Failed', color: 'text-red-400' },
]

export default function TaskBoard() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const agents = useAppStore((s) => s.agents)
  const [tasks, setTasks] = useState([])
  const [showCreate, setShowCreate] = useState(false)
  const [selectedTask, setSelectedTask] = useState(null)
  const [trace, setTrace] = useState([])
  const [form, setForm] = useState({
    title: '',
    description: '',
    assigned_agent: '',
    priority: 'normal',
  })

  const fetchTasks = useCallback(async () => {
    try {
      const params = { limit: 200 }
      if (selectedAgent) params.agent = selectedAgent
      const { data } = await taskApi.list(params)
      setTasks(data.items || [])
    } catch {
      // ignore
    }
  }, [selectedAgent])

  useEffect(() => {
    fetchTasks()
    const interval = setInterval(fetchTasks, 5000)
    return () => clearInterval(interval)
  }, [fetchTasks])

  const handleCreate = async () => {
    if (!form.title.trim()) return
    try {
      await taskApi.create({
        ...form,
        assigned_agent: form.assigned_agent || undefined,
      })
      setForm({ title: '', description: '', assigned_agent: '', priority: 'normal' })
      setShowCreate(false)
      fetchTasks()
    } catch {
      // ignore
    }
  }

  const handleSelectTask = async (task) => {
    setSelectedTask(task)
    try {
      const { data } = await taskApi.trace(task.id)
      setTrace(data || [])
    } catch {
      setTrace([])
    }
  }

  const grouped = {}
  for (const col of COLUMNS) {
    grouped[col.key] = tasks.filter((t) => t.status === col.key)
  }

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 p-2 border-b border-surface-200">
        <button
          onClick={() => setShowCreate(true)}
          className="bg-accent hover:bg-accent-dark text-white px-3 py-1 rounded text-xs flex items-center gap-1"
        >
          <Plus size={12} />
          Create Task
        </button>
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
                    key={task.id}
                    task={task}
                    onClick={() => handleSelectTask(task)}
                  />
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Create modal */}
      {showCreate && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-surface-50 border border-surface-200 rounded-lg p-4 w-full max-w-sm">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-200">Create Task</h3>
              <button onClick={() => setShowCreate(false)}>
                <X size={16} className="text-gray-500" />
              </button>
            </div>
            <div className="space-y-2">
              <input
                value={form.title}
                onChange={(e) => setForm({ ...form, title: e.target.value })}
                placeholder="Title"
                className="w-full bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200"
              />
              <textarea
                value={form.description}
                onChange={(e) =>
                  setForm({ ...form, description: e.target.value })
                }
                placeholder="Description"
                rows={3}
                className="w-full bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200 resize-none"
              />
              <select
                value={form.assigned_agent}
                onChange={(e) =>
                  setForm({ ...form, assigned_agent: e.target.value })
                }
                className="w-full bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-300"
              >
                <option value="">Unassigned</option>
                {agents.map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.display_name || a.id}
                  </option>
                ))}
              </select>
              <select
                value={form.priority}
                onChange={(e) =>
                  setForm({ ...form, priority: e.target.value })
                }
                className="w-full bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-300"
              >
                <option value="low">Low</option>
                <option value="normal">Normal</option>
                <option value="high">High</option>
                <option value="critical">Critical</option>
              </select>
              <button
                onClick={handleCreate}
                className="w-full bg-accent hover:bg-accent-dark text-white py-1.5 rounded text-sm"
              >
                Create
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Task detail panel */}
      {selectedTask && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-surface-50 border border-surface-200 rounded-lg p-4 w-full max-w-lg max-h-[80vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-200 truncate mr-2">
                {selectedTask.title}
              </h3>
              <button onClick={() => setSelectedTask(null)} className="shrink-0">
                <X size={16} className="text-gray-500" />
              </button>
            </div>
            <div className="space-y-2 text-xs">
              <div className="flex flex-wrap gap-3">
                <div>
                  <span className="text-gray-500">Status:</span>{' '}
                  <span className="text-gray-300">{selectedTask.status}</span>
                </div>
                <div>
                  <span className="text-gray-500">Priority:</span>{' '}
                  <span className="text-gray-300">{selectedTask.priority}</span>
                </div>
                <div>
                  <span className="text-gray-500">Agent:</span>{' '}
                  <span className="text-gray-300">
                    {selectedTask.assigned_agent || 'Unassigned'}
                  </span>
                </div>
              </div>
              {selectedTask.description && (
                <p className="text-gray-400">{selectedTask.description}</p>
              )}
              <div className="flex flex-wrap gap-3 text-gray-500">
                <span title={fullTimestamp(selectedTask.created_at)}>
                  Created: {relativeTime(selectedTask.created_at)}
                </span>
                {selectedTask.started_at && (
                  <span>Started: {relativeTime(selectedTask.started_at)}</span>
                )}
                {selectedTask.completed_at && (
                  <span>
                    Completed: {relativeTime(selectedTask.completed_at)}
                  </span>
                )}
              </div>
              {selectedTask.result && (
                <div>
                  <span className="text-gray-500">Result:</span>
                  <pre className="text-gray-400 mt-1 bg-surface-100 p-2 rounded whitespace-pre-wrap break-all">
                    {JSON.stringify(selectedTask.result, null, 2)}
                  </pre>
                </div>
              )}
              {selectedTask.error_message && (
                <div>
                  <span className="text-red-400">Error:</span>
                  <p className="text-red-300 mt-1">{selectedTask.error_message}</p>
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
                        <span className="text-gray-300">
                          {msg.sender_id}
                        </span>
                        {msg.receiver_id && ` → ${msg.receiver_id}`}
                        <span className="text-gray-500 ml-2">
                          [{msg.message_type}]
                        </span>
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
