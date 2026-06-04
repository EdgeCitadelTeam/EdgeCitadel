import { useEffect, useMemo, useState } from 'react'
import {
  Area,
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { Activity, Newspaper, Star, TriangleAlert } from 'lucide-react'
import clsx from 'clsx'
import { api } from '../api/client'

const TIMEFRAMES = ['1D', '5D', '1M', '6M', 'YTD', '1Y', '5Y']
const DMA_KEYS = ['dma20', 'dma50', 'dma200']
const EVENT_TYPES = {
  earnings: { label: 'Earnings', color: '#a78bfa', icon: Star },
  analyst: { label: 'Rating', color: '#60a5fa', icon: Activity },
  news: { label: 'News', color: '#f59e0b', icon: Newspaper },
  alert: { label: 'Agent alert', color: '#fb7185', icon: TriangleAlert },
}

const RANGE_CONFIG = {
  '1D': { points: 48, stepMs: 30 * 60 * 1000, interval: '30m' },
  '5D': { points: 80, stepMs: 90 * 60 * 1000, interval: '90m' },
  '1M': { points: 30, stepMs: 24 * 60 * 60 * 1000, interval: '1d' },
  '6M': { points: 126, stepMs: 24 * 60 * 60 * 1000, interval: '1d' },
  YTD: { points: 110, stepMs: 24 * 60 * 60 * 1000, interval: '1d' },
  '1Y': { points: 252, stepMs: 24 * 60 * 60 * 1000, interval: '1d' },
  '5Y': { points: 260, stepMs: 7 * 24 * 60 * 60 * 1000, interval: '1w' },
}

function fmtDate(value) {
  return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(new Date(value))
}

function movingAverage(values, period, index) {
  if (index < period - 1) return null
  const slice = values.slice(index - period + 1, index + 1)
  return Number((slice.reduce((sum, item) => sum + item.close, 0) / period).toFixed(2))
}

function makeMockCandles(range) {
  const config = RANGE_CONFIG[range] || RANGE_CONFIG['1M']
  const now = Date.now()
  let lastClose = 204
  const candles = Array.from({ length: config.points }, (_, idx) => {
    const drift = Math.sin(idx / 7) * 1.6 + Math.cos(idx / 17) * 2.4
    const open = lastClose + Math.sin(idx / 3) * 0.9
    const close = open + drift / 2 + (idx % 9 === 0 ? 1.8 : -0.15)
    const high = Math.max(open, close) + 1.2 + Math.abs(Math.sin(idx))
    const low = Math.min(open, close) - 1.1 - Math.abs(Math.cos(idx / 2))
    lastClose = close
    return {
      time: new Date(now - (config.points - idx - 1) * config.stepMs).toISOString(),
      open: Number(open.toFixed(2)),
      high: Number(high.toFixed(2)),
      low: Number(low.toFixed(2)),
      close: Number(close.toFixed(2)),
      volume: Math.round(32_000_000 + Math.abs(Math.sin(idx / 5)) * 25_000_000 + idx * 75_000),
    }
  })

  const eventIndexes = [Math.floor(config.points * 0.18), Math.floor(config.points * 0.38), Math.floor(config.points * 0.64), Math.floor(config.points * 0.82)]
  const eventTypes = ['earnings', 'analyst', 'news', 'alert']
  return candles.map((candle, idx) => ({
    ...candle,
    events: eventIndexes.includes(idx)
      ? [{ type: eventTypes[eventIndexes.indexOf(idx)], label: EVENT_TYPES[eventTypes[eventIndexes.indexOf(idx)]].label }]
      : [],
  }))
}

function enrichCandles(candles, indicators = {}) {
  const normalized = candles.map((item) => ({
    ...item,
    timestamp: item.timestamp || item.time || item.date,
    time: item.time || item.timestamp || item.date,
    open: Number(item.open),
    high: Number(item.high),
    low: Number(item.low),
    close: Number(item.close ?? item.price),
    volume: Number(item.volume || 0),
  }))

  let cumulativePv = 0
  let cumulativeVolume = 0
  return normalized.map((item, idx) => {
    cumulativePv += item.close * item.volume
    cumulativeVolume += item.volume
    return {
      ...item,
      label: fmtDate(item.time),
      price: item.close,
      dma20: indicators.dma20?.[idx] ?? indicators.ma20?.[idx] ?? movingAverage(normalized, 20, idx),
      dma50: indicators.dma50?.[idx] ?? indicators.ma50?.[idx] ?? movingAverage(normalized, 50, idx),
      dma200: indicators.dma200?.[idx] ?? indicators.ma200?.[idx] ?? movingAverage(normalized, 200, idx),
      vwap: indicators.vwap?.[idx] ?? Number((cumulativePv / Math.max(cumulativeVolume, 1)).toFixed(2)),
      rsi: indicators.rsi?.[idx] ?? Number((50 + Math.sin(idx / 6) * 18 + Math.cos(idx / 15) * 9).toFixed(2)),
      macd: indicators.macd?.[idx]?.macd ?? indicators.macd?.[idx] ?? Number((Math.sin(idx / 9) * 2.2).toFixed(2)),
      macdSignal: indicators.macd?.[idx]?.signal ?? Number((Math.sin((idx - 3) / 9) * 1.8).toFixed(2)),
    }
  })
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const row = payload[0].payload
  return (
    <div className="rounded-lg border border-surface-200 bg-surface/95 p-3 text-xs shadow-xl">
      <div className="font-medium text-gray-200">{label}</div>
      <div className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-gray-400">
        <span>Open</span><span className="text-right tabular-nums text-gray-200">${row.open?.toFixed(2)}</span>
        <span>High</span><span className="text-right tabular-nums text-gray-200">${row.high?.toFixed(2)}</span>
        <span>Low</span><span className="text-right tabular-nums text-gray-200">${row.low?.toFixed(2)}</span>
        <span>Close</span><span className="text-right tabular-nums text-gray-200">${row.close?.toFixed(2)}</span>
        <span>Volume</span><span className="text-right tabular-nums text-gray-200">{Intl.NumberFormat('en', { notation: 'compact' }).format(row.volume || 0)}</span>
      </div>
    </div>
  )
}

function EventDot({ cx, cy, payload }) {
  const event = payload.events?.[0]
  if (!event) return null
  const eventMeta = EVENT_TYPES[event.type] || EVENT_TYPES.news
  return <circle cx={cx} cy={cy} r={5} fill={eventMeta.color} stroke="#111827" strokeWidth={2} />
}

function EventLegend({ events }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-[11px] text-gray-400">
      {Object.entries(EVENT_TYPES).map(([key, event]) => {
        const Icon = event.icon
        const active = events.some((item) => item.events?.some((marker) => marker.type === key))
        return (
          <span key={key} className={clsx('inline-flex items-center gap-1 rounded-full border px-2 py-1', active ? 'border-surface-200 bg-surface' : 'border-surface-200/40 opacity-50')}>
            <Icon size={11} style={{ color: event.color }} /> {event.label}
          </span>
        )
      })}
    </div>
  )
}

export default function PriceChartPanel({ symbol = 'AAPL' }) {
  const [timeframe, setTimeframe] = useState('1M')
  const [chartType, setChartType] = useState('line')
  const [oscillator, setOscillator] = useState('rsi')
  const [enabledOverlays, setEnabledOverlays] = useState({ dma20: true, dma50: true, dma200: false, vwap: true })
  const [candles, setCandles] = useState([])
  const [indicators, setIndicators] = useState({})
  const [status, setStatus] = useState('loading')

  const interval = RANGE_CONFIG[timeframe]?.interval || '1d'

  useEffect(() => {
    let cancelled = false
    setStatus('loading')
    Promise.all([
      api.getCandles(symbol, timeframe, interval),
      api.getTechnicalIndicators(symbol, timeframe, ['dma20', 'dma50', 'dma200', 'vwap', 'rsi', 'macd']),
    ])
      .then(([candlePayload, indicatorPayload]) => {
        if (cancelled) return
        setCandles(candlePayload?.candles || candlePayload || [])
        setIndicators(indicatorPayload?.indicators || indicatorPayload || {})
        setStatus('live')
      })
      .catch(() => {
        if (cancelled) return
        setCandles(makeMockCandles(timeframe))
        setIndicators({})
        setStatus('prototype')
      })
    return () => { cancelled = true }
  }, [symbol, timeframe, interval])

  const data = useMemo(() => enrichCandles(candles, indicators), [candles, indicators])
  const events = data.filter((item) => item.events?.length)
  const latest = data[data.length - 1]

  const toggleOverlay = (key) => setEnabledOverlays((prev) => ({ ...prev, [key]: !prev[key] }))

  return (
    <section className="rounded-xl border border-surface-200 bg-surface-50 p-4 shadow-sm">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <h3 className="text-base font-semibold text-gray-100">Price chart</h3>
            <span className={clsx('rounded-full px-2 py-0.5 text-[11px] uppercase tracking-wide', status === 'live' ? 'bg-emerald-500/15 text-emerald-300' : status === 'loading' ? 'bg-blue-500/15 text-blue-300' : 'bg-yellow-500/15 text-yellow-300')}>
              {status === 'live' ? 'API data' : status === 'loading' ? 'Loading' : 'Prototype data'}
            </span>
          </div>
          <p className="mt-1 text-xs text-gray-500">
            {symbol} · {timeframe} · {latest ? `last $${latest.close.toFixed(2)}` : 'waiting for market data'}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="flex rounded-lg border border-surface-200 bg-surface p-0.5">
            {TIMEFRAMES.map((item) => (
              <button key={item} onClick={() => setTimeframe(item)} className={clsx('rounded-md px-2 py-1 text-xs transition-colors', timeframe === item ? 'bg-accent text-white' : 'text-gray-400 hover:text-gray-200')}>
                {item}
              </button>
            ))}
          </div>
          <div className="flex rounded-lg border border-surface-200 bg-surface p-0.5">
            {['line', 'candles'].map((item) => (
              <button key={item} onClick={() => setChartType(item)} className={clsx('rounded-md px-2 py-1 text-xs capitalize transition-colors', chartType === item ? 'bg-surface-200 text-gray-100' : 'text-gray-500 hover:text-gray-300')}>
                {item}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          {[...DMA_KEYS, 'vwap'].map((key) => (
            <button key={key} onClick={() => toggleOverlay(key)} className={clsx('rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-wide transition-colors', enabledOverlays[key] ? 'border-accent/60 bg-accent/10 text-accent-light' : 'border-surface-200 text-gray-500 hover:text-gray-300')}>
              {key.replace('dma', '')}{key.startsWith('dma') ? ' DMA' : ''}
            </button>
          ))}
        </div>
        <EventLegend events={events} />
      </div>

      <div className="mt-4 h-[420px] min-w-0">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
            <defs>
              <linearGradient id="priceFill" x1="0" x2="0" y1="0" y2="1">
                <stop offset="5%" stopColor="#14b8a6" stopOpacity={0.28} />
                <stop offset="95%" stopColor="#14b8a6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
            <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 11 }} minTickGap={28} tickLine={false} axisLine={false} />
            <YAxis yAxisId="price" domain={['dataMin - 5', 'dataMax + 5']} orientation="right" tick={{ fill: '#94a3b8', fontSize: 11 }} tickFormatter={(v) => `$${Number(v).toFixed(0)}`} tickLine={false} axisLine={false} width={48} />
            <YAxis yAxisId="volume" orientation="left" hide domain={[0, 'dataMax * 5']} />
            <Tooltip content={<CustomTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            <Bar yAxisId="volume" dataKey="volume" name="Volume" fill="#334155" opacity={0.55} barSize={chartType === 'candles' ? 4 : 6} />
            {chartType === 'line' ? (
              <Area yAxisId="price" type="monotone" dataKey="price" name="Close" stroke="#14b8a6" strokeWidth={2} fill="url(#priceFill)" dot={false} />
            ) : (
              <>
                <Bar yAxisId="price" dataKey={(row) => [row.low, row.high]} name="High/Low" fill="#64748b" barSize={2} />
                <Line yAxisId="price" type="monotone" dataKey="open" name="Open" stroke="#f59e0b" strokeWidth={1.5} dot={false} strokeDasharray="3 3" />
                <Line yAxisId="price" type="monotone" dataKey="close" name="Close" stroke="#14b8a6" strokeWidth={1.8} dot={false} />
              </>
            )}
            {enabledOverlays.dma20 && <Line yAxisId="price" type="monotone" dataKey="dma20" name="20 DMA" stroke="#fde047" strokeWidth={1.5} dot={false} connectNulls />}
            {enabledOverlays.dma50 && <Line yAxisId="price" type="monotone" dataKey="dma50" name="50 DMA" stroke="#60a5fa" strokeWidth={1.5} dot={false} connectNulls />}
            {enabledOverlays.dma200 && <Line yAxisId="price" type="monotone" dataKey="dma200" name="200 DMA" stroke="#c084fc" strokeWidth={1.5} dot={false} connectNulls />}
            {enabledOverlays.vwap && <Line yAxisId="price" type="monotone" dataKey="vwap" name="VWAP" stroke="#fb7185" strokeWidth={1.5} dot={false} strokeDasharray="4 4" />}
            {events.map((item) => (
              <ReferenceDot key={`${item.time}-${item.events[0].type}`} yAxisId="price" x={item.label} y={item.close} shape={<EventDot payload={item} />} ifOverflow="extendDomain" />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className="mt-4 rounded-lg border border-surface-200 bg-surface p-3">
        <div className="mb-2 flex items-center justify-between gap-3">
          <div className="text-xs font-medium uppercase tracking-wide text-gray-500">Momentum</div>
          <div className="flex rounded-lg border border-surface-200 bg-surface-50 p-0.5">
            {['rsi', 'macd'].map((item) => (
              <button key={item} onClick={() => setOscillator(item)} className={clsx('rounded-md px-2 py-1 text-xs uppercase transition-colors', oscillator === item ? 'bg-surface-200 text-gray-100' : 'text-gray-500 hover:text-gray-300')}>
                {item}
              </button>
            ))}
          </div>
        </div>
        <div className="h-28">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid stroke="#253041" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="label" hide />
              <YAxis orientation="right" tick={{ fill: '#94a3b8', fontSize: 10 }} tickLine={false} axisLine={false} width={36} />
              <Tooltip content={<CustomTooltip />} />
              {oscillator === 'rsi' ? (
                <Line type="monotone" dataKey="rsi" name="RSI" stroke="#a78bfa" strokeWidth={1.8} dot={false} />
              ) : (
                <>
                  <Bar dataKey="macd" name="MACD" fill="#14b8a6" opacity={0.55} barSize={5} />
                  <Line type="monotone" dataKey="macdSignal" name="Signal" stroke="#fb7185" strokeWidth={1.5} dot={false} />
                </>
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  )
}
