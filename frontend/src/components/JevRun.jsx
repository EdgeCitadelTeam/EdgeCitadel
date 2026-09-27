import { useState } from 'react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import useAppStore from '../stores/appStore'
import { navigateTrace } from '../traces/navigation'

export default function JevRun({ run }) {
  const [sending, setSending] = useState(false)
  const online = useAppStore(s => s.agents.some(a => a.agent_id === 'jev' && a.agent_state === 'online'))
  const track = useAppStore(s => s.setTrackedTaskId)
  const pending = useAppStore(s => s.addPendingCommand)
  async function resume(event) {
    event.stopPropagation()
    if (sending) return
    setSending(true)
    try {
      const response = await api.sendCommand('jev', '继续已有运行', { run_id: run.run_id }, 'jev.resume')
      track(response.task_id)
      pending(response.task_id, 'jev')
      toast.success('已提交继续请求')
    } catch {
      toast.error('继续请求未确认；可再次查询同一运行')
    } finally {
      setSending(false)
    }
  }
  return <div className="mt-2 space-y-1 text-xs" aria-label="JEV run">
    <div className="text-gray-400">JEV · {run.outcome} · {run.run_id}</div>
    {(run.steps || []).map((step, i) => <div key={step.task_id}>
      <button className="text-accent-light underline" onClick={event => {
        event.stopPropagation()
        navigateTrace({ task: step.task_id, step: 'task:' + step.task_id })
      }}>{i + 1}. {step.executor} · {step.observed_state || '尚未派发'} · {step.task_id.slice(0, 8)}</button>
    </div>)}
    {run.resumable && <div className="flex items-center gap-2">
      <button className="rounded border border-accent/50 px-2 py-1 text-accent-light disabled:opacity-40"
        disabled={sending || !online} onClick={resume}>继续</button>
      <span className="text-gray-500">{online ? '查询原任务；不会重复已完成步骤' : 'JEV offline'}</span>
    </div>}
  </div>
}
