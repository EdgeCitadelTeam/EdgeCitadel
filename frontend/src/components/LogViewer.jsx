import { useState, useEffect, useRef, useCallback } from 'react'
import { Filter } from 'lucide-react'
import clsx from 'clsx'
import useAppStore from '../stores/appStore'
import { api } from '../api/client'
import { relativeTime, fullTimestamp } from '../utils/formatTime'

const LEVEL_COLORS = {
  INFO: 'text-blue-400',
  WARN: 'text-yellow-400',
  WARNING: 'text-yellow-400',
  ERROR: 'text-red-400',
  MQTT: 'text-purple-400',
  DEBUG: 'text-gray-400',
}

const LEVEL_BG = {
  ERROR: 'bg-red-500/5',
  WARN: 'bg-yellow-500/5',
  WARNING: 'bg-yellow-500/5',
}

const LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG', 'MQTT']

export default function LogViewer() {
  const selectedAgent = useAppStore((s) => s.selectedAgent)
  const [logs, setLogs] = useState([])
  const [loading, setLoading] = useState(false)
  const [levelFilter, setLevelFilter] = useState(null)
  const [sourceFilter, setSourceFilter] = useState('')
  const [search, setSearch] = useState('')
  const [expandedLog, setExpandedLog] = useState(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const scrollRef = useRef(null)
  const bottomRef = useRef(null)

  const fetchLogs = useCallback(async () => {
    setLoading(true)
    try {
      const params = { type: 'log', limit: 200 }
      if (selectedAgent) params.agent_id = selectedAgent

      let items = (await api.queryMessages(params)) || []
      // Map log envelopes to a flat row shape; payload is free-form per agent.
      let rows = items.map((m) => ({
        id: m.id,
        level: (m.payload?.level || 'INFO').toUpperCase(),
        timestamp: m.timestamp,
        agent_id: m.sender_id,
        source: m.payload?.source || m.payload?.logger || '',
        message: m.payload?.message || m.payload?.body || JSON.stringify(m.payload),
        metadata: m.payload,
      }))
      if (levelFilter) rows = rows.filter((r) => r.level === levelFilter)
      if (sourceFilter) rows = rows.filter((r) => (r.source || '').includes(sourceFilter))
      if (search) {
        const needle = search.toLowerCase()
        rows = rows.filter((r) => (r.message || '').toLowerCase().includes(needle))
      }
      setLogs(rows.slice().reverse())
    } catch {
      // ignore
    }
    setLoading(false)
  }, [levelFilter, selectedAgent, sourceFilter, search])

  useEffect(() => {
    fetchLogs()
    const interval = setInterval(fetchLogs, 5000)
    return () => clearInterval(interval)
  }, [fetchLogs])

  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [logs, autoScroll])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    setAutoScroll(el.scrollHeight - el.scrollTop - el.clientHeight < 100)
  }

  return (
    <div className="flex flex-col h-full">
      {/* Filters */}
      <div className="flex items-center gap-2 p-2 border-b border-surface-200 flex-wrap">
        <Filter size={14} className="text-gray-500" />
        {LEVELS.map((level) => (
          <button
            key={level}
            onClick={() =>
              setLevelFilter(levelFilter === level ? null : level)
            }
            className={clsx(
              'px-2 py-0.5 text-xs rounded transition-colors',
              levelFilter === level
                ? 'bg-accent text-white'
                : 'bg-surface-100 text-gray-400 hover:bg-surface-200'
            )}
          >
            {level}
          </button>
        ))}
        <input
          value={sourceFilter}
          onChange={(e) => setSourceFilter(e.target.value)}
          placeholder="Source..."
          className="bg-surface-100 border border-surface-200 rounded px-2 py-0.5 text-xs text-gray-300 w-20 md:w-24 focus:outline-none"
        />
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search logs..."
          className="bg-surface-100 border border-surface-200 rounded px-2 py-0.5 text-xs text-gray-300 w-28 md:w-40 focus:outline-none"
        />
      </div>

      {/* Log table */}
      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="flex-1 overflow-auto font-mono text-xs"
      >
        {/* Desktop: table view */}
        <table className="w-full hidden md:table">
          <thead className="sticky top-0 bg-surface-50">
            <tr className="text-left text-gray-500 border-b border-surface-200">
              <th className="px-2 py-1.5 w-16">Level</th>
              <th className="px-2 py-1.5 w-28">Time</th>
              <th className="px-2 py-1.5 w-24">Agent</th>
              <th className="px-2 py-1.5 w-20">Source</th>
              <th className="px-2 py-1.5">Message</th>
            </tr>
          </thead>
          <tbody>
            {logs.map((log) => (
              <tr
                key={log.id}
                onClick={() =>
                  setExpandedLog(expandedLog === log.id ? null : log.id)
                }
                className={clsx(
                  'border-b border-surface-200/50 cursor-pointer hover:bg-surface-100',
                  LEVEL_BG[log.level]
                )}
              >
                <td
                  className={clsx(
                    'px-2 py-1 font-medium',
                    LEVEL_COLORS[log.level] || 'text-gray-400'
                  )}
                >
                  {log.level}
                </td>
                <td
                  className="px-2 py-1 text-gray-500"
                  title={fullTimestamp(log.timestamp)}
                >
                  {relativeTime(log.timestamp)}
                </td>
                <td className="px-2 py-1 text-gray-300">
                  {log.agent_id || '-'}
                </td>
                <td className="px-2 py-1 text-gray-400">{log.source || '-'}</td>
                <td className="px-2 py-1 text-gray-300 truncate max-w-md">
                  {log.message}
                </td>
              </tr>
            ))}
            {logs.length === 0 && !loading && (
              <tr>
                <td
                  colSpan={5}
                  className="text-center py-8 text-gray-500"
                >
                  No logs found
                </td>
              </tr>
            )}
          </tbody>
        </table>

        {/* Mobile: card view */}
        <div className="md:hidden space-y-1 p-2">
          {logs.map((log) => (
            <div
              key={log.id}
              onClick={() =>
                setExpandedLog(expandedLog === log.id ? null : log.id)
              }
              className={clsx(
                'p-2 rounded border border-surface-200/50 cursor-pointer',
                LEVEL_BG[log.level],
                'hover:bg-surface-100'
              )}
            >
              <div className="flex items-center gap-2 mb-1">
                <span
                  className={clsx(
                    'text-[10px] font-medium px-1.5 py-0.5 rounded',
                    LEVEL_COLORS[log.level] || 'text-gray-400',
                    'bg-surface-200/50'
                  )}
                >
                  {log.level}
                </span>
                <span className="text-[10px] text-gray-500" title={fullTimestamp(log.timestamp)}>
                  {relativeTime(log.timestamp)}
                </span>
                {log.agent_id && (
                  <span className="text-[10px] text-gray-400 ml-auto truncate max-w-[100px]">
                    {log.agent_id}
                  </span>
                )}
              </div>
              <p className="text-gray-300 text-[11px] line-clamp-2">
                {log.message}
              </p>
            </div>
          ))}
          {logs.length === 0 && !loading && (
            <p className="text-center py-8 text-gray-500 text-xs">
              No logs found
            </p>
          )}
        </div>

        {/* Expanded metadata */}
        {expandedLog && (
          <div className="bg-surface-100 border border-surface-200 m-2 p-2 rounded">
            <pre className="text-xs text-gray-400 whitespace-pre-wrap break-all">
              {JSON.stringify(
                logs.find((l) => l.id === expandedLog)?.metadata || {},
                null,
                2
              )}
            </pre>
          </div>
        )}

        <div ref={bottomRef} />
      </div>
    </div>
  )
}
