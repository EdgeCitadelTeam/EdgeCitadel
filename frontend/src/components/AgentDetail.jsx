import { useState, useEffect } from 'react'
import { ArrowLeft, Send } from 'lucide-react'
import useAppStore from '../stores/appStore'
import { agentApi, messageApi, taskApi, commandApi } from '../api/client'
import { getAgentColor } from '../utils/agentColors'
import { relativeTime, fullTimestamp } from '../utils/formatTime'
import StatusBadge from './StatusBadge'
import toast from 'react-hot-toast'

export default function AgentDetail({ agentId, onBack }) {
  const [agent, setAgent] = useState(null)
  const [stats, setStats] = useState(null)
  const [messages, setMessages] = useState([])
  const [tasks, setTasks] = useState([])
  const [command, setCommand] = useState('')

  useEffect(() => {
    const fetch = async () => {
      try {
        const [agentRes, statsRes, msgRes, taskRes] = await Promise.all([
          agentApi.get(agentId),
          agentApi.stats(agentId),
          messageApi.list({ agent: agentId, limit: 50 }),
          taskApi.list({ agent: agentId, limit: 20 }),
        ])
        setAgent(agentRes.data)
        setStats(statsRes.data)
        setMessages(msgRes.data.items || [])
        setTasks(taskRes.data.items || [])
      } catch {
        // ignore
      }
    }
    fetch()
  }, [agentId])

  const handleSendCommand = async () => {
    if (!command.trim()) return
    try {
      await commandApi.send(agentId, {
        message_type: 'command',
        payload: { message: command.trim() },
      })
      setCommand('')
      toast.success(`Sent to ${agentId}`)
    } catch {
      toast.error('Failed to send command')
    }
  }

  if (!agent) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-sm text-gray-500">Loading agent details...</p>
      </div>
    )
  }

  const color = getAgentColor(agent.id)

  return (
    <div className="h-full overflow-y-auto p-3 md:p-4">
      {/* Back button */}
      <button
        onClick={onBack}
        className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-200 mb-4"
      >
        <ArrowLeft size={14} />
        Back to all
      </button>

      {/* Profile header */}
      <div className="flex items-start gap-3 md:gap-4 mb-6">
        <div
          className="w-10 h-10 md:w-14 md:h-14 rounded-full flex items-center justify-center text-sm md:text-lg font-bold text-white shrink-0"
          style={{ backgroundColor: color }}
        >
          {agent.id.slice(0, 2).toUpperCase()}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-base md:text-lg font-semibold text-gray-100 truncate">
              {agent.display_name || agent.id}
            </h2>
            <StatusBadge status={agent.status} size="md" />
          </div>
          <div className="text-sm text-gray-400 mt-0.5">
            {agent.role && <span>{agent.role}</span>}
            {agent.device_type && <span> &middot; {agent.device_type}</span>}
            {agent.model && <span> &middot; {agent.model}</span>}
          </div>
          <div className="text-xs text-gray-500 mt-1 break-all">
            {agent.ip_address && <span>IP: {agent.ip_address}</span>}
            {agent.last_heartbeat && (
              <span
                className="ml-3"
                title={fullTimestamp(agent.last_heartbeat)}
              >
                Last heartbeat: {relativeTime(agent.last_heartbeat)}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Capabilities */}
      {agent.capabilities && (
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-400 mb-1">
            Capabilities
          </h3>
          <div className="flex flex-wrap gap-1">
            {(Array.isArray(agent.capabilities)
              ? agent.capabilities
              : Object.keys(agent.capabilities)
            ).map((cap) => (
              <span
                key={cap}
                className="bg-surface-100 text-gray-300 px-2 py-0.5 rounded text-xs"
              >
                {cap}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Metrics */}
      <div className="grid grid-cols-2 gap-2 md:gap-3 mb-4">
        <div className="bg-surface-50 border border-surface-200 rounded-lg p-3">
          <div className="text-xs text-gray-500 mb-1">CPU</div>
          <div className="text-xl md:text-2xl font-mono text-gray-200">
            {agent.cpu_percent != null
              ? `${agent.cpu_percent.toFixed(1)}%`
              : 'N/A'}
          </div>
          {agent.cpu_percent != null && (
            <div className="mt-2 h-1 bg-surface-200 rounded-full">
              <div
                className="h-full bg-accent rounded-full"
                style={{ width: `${Math.min(agent.cpu_percent, 100)}%` }}
              />
            </div>
          )}
        </div>
        <div className="bg-surface-50 border border-surface-200 rounded-lg p-3">
          <div className="text-xs text-gray-500 mb-1">Memory</div>
          <div className="text-xl md:text-2xl font-mono text-gray-200">
            {agent.memory_percent != null
              ? `${agent.memory_percent.toFixed(1)}%`
              : 'N/A'}
          </div>
          {agent.memory_percent != null && (
            <div className="mt-2 h-1 bg-surface-200 rounded-full">
              <div
                className="h-full bg-green-500 rounded-full"
                style={{ width: `${Math.min(agent.memory_percent, 100)}%` }}
              />
            </div>
          )}
        </div>
      </div>

      {/* Stats */}
      {stats && (
        <div className="grid grid-cols-2 gap-2 md:gap-3 mb-4">
          <div className="bg-surface-50 border border-surface-200 rounded-lg p-3">
            <div className="text-xs text-gray-500">Messages</div>
            <div className="text-lg font-mono text-gray-200">
              {stats.message_count}
            </div>
          </div>
          <div className="bg-surface-50 border border-surface-200 rounded-lg p-3">
            <div className="text-xs text-gray-500">Tasks</div>
            <div className="text-lg font-mono text-gray-200">
              {stats.task_count}
            </div>
          </div>
        </div>
      )}

      {/* Send command */}
      <div className="mb-4">
        <h3 className="text-xs font-medium text-gray-400 mb-1">
          Send Command
        </h3>
        <div className="flex gap-2">
          <input
            value={command}
            onChange={(e) => setCommand(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSendCommand()}
            placeholder="Enter command..."
            className="flex-1 min-w-0 bg-surface-100 border border-surface-200 rounded px-3 py-1.5 text-sm text-gray-200"
          />
          <button
            onClick={handleSendCommand}
            disabled={!command.trim()}
            className="bg-accent hover:bg-accent-dark disabled:opacity-40 text-white px-3 py-1.5 rounded shrink-0"
          >
            <Send size={14} />
          </button>
        </div>
      </div>

      {/* Recent messages */}
      <div className="mb-4">
        <h3 className="text-xs font-medium text-gray-400 mb-2">
          Recent Messages ({messages.length})
        </h3>
        <div className="space-y-1 max-h-60 overflow-y-auto">
          {messages.map((msg) => (
            <div
              key={msg.id}
              className="bg-surface-50 border border-surface-200 rounded p-2 text-xs"
            >
              <span className="text-gray-300">{msg.sender_id}</span>
              {msg.receiver_id && (
                <span className="text-gray-500"> &rarr; {msg.receiver_id}</span>
              )}
              <span className="text-gray-600 ml-2">[{msg.message_type}]</span>
              <span className="text-gray-600 ml-2">
                {relativeTime(msg.timestamp)}
              </span>
            </div>
          ))}
          {messages.length === 0 && (
            <p className="text-gray-500 text-xs">No messages</p>
          )}
        </div>
      </div>

      {/* Task history */}
      <div>
        <h3 className="text-xs font-medium text-gray-400 mb-2">
          Task History ({tasks.length})
        </h3>
        <div className="space-y-1 max-h-60 overflow-y-auto">
          {tasks.map((task) => (
            <div
              key={task.id}
              className="bg-surface-50 border border-surface-200 rounded p-2 text-xs flex items-center gap-2"
            >
              <span className="text-gray-300 flex-1 truncate">
                {task.title}
              </span>
              <span
                className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${
                  task.status === 'completed'
                    ? 'bg-green-500/20 text-green-400'
                    : task.status === 'failed'
                      ? 'bg-red-500/20 text-red-400'
                      : 'bg-gray-500/20 text-gray-400'
                }`}
              >
                {task.status}
              </span>
            </div>
          ))}
          {tasks.length === 0 && (
            <p className="text-gray-500 text-xs">No tasks</p>
          )}
        </div>
      </div>
    </div>
  )
}
