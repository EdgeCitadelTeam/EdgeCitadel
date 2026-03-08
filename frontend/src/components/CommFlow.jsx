import { useState, useEffect, useRef, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { messageApi } from '../api/client'
import { getAgentColor } from '../utils/agentColors'
import useAppStore from '../stores/appStore'

const TIME_OPTIONS = [
  { label: '1h', hours: 1 },
  { label: '6h', hours: 6 },
  { label: '24h', hours: 24 },
  { label: '7d', hours: 168 },
]

export default function CommFlow() {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] })
  const [selectedHours, setSelectedHours] = useState(24)
  const [selectedNode, setSelectedNode] = useState(null)
  const graphRef = useRef()
  const realtimeMessages = useAppStore((s) => s.realtimeMessages)

  const fetchFlow = useCallback(async () => {
    try {
      const { data } = await messageApi.flow({ hours: selectedHours })
      const nodes = (data.nodes || []).map((n) => ({
        id: n.id,
        color: getAgentColor(n.id),
        val: 1,
      }))

      // Add MQTT broker hub node
      const hasNodes = nodes.length > 0
      if (hasNodes) {
        nodes.push({
          id: 'mqtt-broker',
          color: '#6366f1',
          val: 3,
          isBroker: true,
        })
      }

      const links = (data.edges || []).map((e) => ({
        source: e.source,
        target: e.target,
        value: e.count || 1,
      }))

      // Add synthetic edges from each agent to the MQTT broker hub
      if (hasNodes) {
        for (const node of nodes) {
          if (!node.isBroker) {
            links.push({
              source: node.id,
              target: 'mqtt-broker',
              value: 1,
              isSynthetic: true,
            })
          }
        }
      }

      // Scale node size by message volume
      const volumes = {}
      for (const link of links) {
        volumes[link.source] = (volumes[link.source] || 0) + link.value
        volumes[link.target] = (volumes[link.target] || 0) + link.value
      }
      for (const node of nodes) {
        if (!node.isBroker) {
          node.val = Math.max(1, Math.log2((volumes[node.id] || 1) + 1))
        }
      }

      setGraphData({ nodes, links })
    } catch {
      // ignore
    }
  }, [selectedHours])

  useEffect(() => {
    fetchFlow()
  }, [fetchFlow])

  // Refresh on new realtime messages (debounced)
  useEffect(() => {
    const timer = setTimeout(fetchFlow, 2000)
    return () => clearTimeout(timer)
  }, [realtimeMessages.length, fetchFlow])

  const paintNode = useCallback((node, ctx) => {
    const size = node.isBroker ? 10 : 6 * (node.val || 1)
    ctx.beginPath()
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI)
    ctx.fillStyle = node.color
    ctx.fill()

    // Label
    ctx.font = node.isBroker ? 'bold 4px sans-serif' : '3px sans-serif'
    ctx.textAlign = 'center'
    ctx.fillStyle = '#e5e7eb'
    ctx.fillText(
      node.isBroker ? 'MQTT Broker' : node.id,
      node.x,
      node.y + size + 5
    )
  }, [])

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2 p-2 border-b border-surface-200">
        <span className="text-xs text-gray-400">Time range:</span>
        {TIME_OPTIONS.map((opt) => (
          <button
            key={opt.hours}
            onClick={() => setSelectedHours(opt.hours)}
            className={`px-2 py-1 text-xs rounded transition-colors ${
              selectedHours === opt.hours
                ? 'bg-accent text-white'
                : 'bg-surface-100 text-gray-400 hover:bg-surface-200'
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      <div className="flex-1 relative">
        {graphData.nodes.length > 0 ? (
          <ForceGraph2D
            ref={graphRef}
            graphData={graphData}
            nodeCanvasObject={paintNode}
            nodePointerAreaPaint={(node, color, ctx) => {
              const size = node.isBroker ? 10 : 6 * (node.val || 1)
              ctx.beginPath()
              ctx.arc(node.x, node.y, size, 0, 2 * Math.PI)
              ctx.fillStyle = color
              ctx.fill()
            }}
            linkColor={() => 'rgba(99, 102, 241, 0.3)'}
            linkWidth={(link) => Math.max(1, Math.log2(link.value + 1))}
            linkDirectionalArrowLength={4}
            linkDirectionalArrowRelPos={0.8}
            backgroundColor="#0f1117"
            onNodeClick={(node) =>
              setSelectedNode(selectedNode === node.id ? null : node.id)
            }
          />
        ) : (
          <div className="flex items-center justify-center h-full">
            <p className="text-xs text-gray-500">
              No communication flow data. Send messages between agents to see
              the graph.
            </p>
          </div>
        )}

        {/* Selected node stats */}
        {selectedNode && (
          <div className="absolute top-2 right-2 bg-surface-50 border border-surface-200 rounded-lg p-3 w-48">
            <h4 className="text-sm font-medium text-gray-200 mb-1">
              {selectedNode}
            </h4>
            <div className="text-xs text-gray-400 space-y-0.5">
              <p>
                Connections:{' '}
                {
                  graphData.links.filter(
                    (l) =>
                      (l.source?.id || l.source) === selectedNode ||
                      (l.target?.id || l.target) === selectedNode
                  ).length
                }
              </p>
              <p>
                Messages:{' '}
                {graphData.links
                  .filter(
                    (l) =>
                      (l.source?.id || l.source) === selectedNode ||
                      (l.target?.id || l.target) === selectedNode
                  )
                  .reduce((sum, l) => sum + l.value, 0)}
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
