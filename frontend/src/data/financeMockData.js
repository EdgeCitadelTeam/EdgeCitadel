export const mockQuotes = {
  NVDA: {
    symbol: 'NVDA', name: 'NVIDIA Corp.', price: 142.63, change: 3.18, changePercent: 2.28,
    marketCap: '3.51T', volume: '176.2M', averageVolume: '214.7M', week52Low: 82.33, week52High: 153.13,
    preMarket: 143.21, afterHours: 142.91, updatedAt: '2026-06-07T20:00:00Z', source: 'Demo market feed',
    rating: 'Buy', alertCount: 2,
  },
  AAPL: {
    symbol: 'AAPL', name: 'Apple Inc.', price: 203.92, change: -1.34, changePercent: -0.65,
    marketCap: '3.06T', volume: '58.4M', averageVolume: '61.1M', week52Low: 169.21, week52High: 237.49,
    preMarket: 204.10, afterHours: 203.77, updatedAt: '2026-06-07T20:00:00Z', source: 'Demo market feed',
    rating: 'Hold', alertCount: 1,
  },
  MSFT: {
    symbol: 'MSFT', name: 'Microsoft Corp.', price: 474.18, change: 4.72, changePercent: 1.01,
    marketCap: '3.52T', volume: '27.5M', averageVolume: '29.2M', week52Low: 359.11, week52High: 481.00,
    preMarket: 475.20, afterHours: 474.88, updatedAt: '2026-06-07T20:00:00Z', source: 'Demo market feed',
    rating: 'Buy', alertCount: 0,
  },
  TSLA: {
    symbol: 'TSLA', name: 'Tesla Inc.', price: 178.44, change: -5.82, changePercent: -3.16,
    marketCap: '568.7B', volume: '121.6M', averageVolume: '104.9M', week52Low: 138.80, week52High: 299.29,
    preMarket: 176.80, afterHours: 177.12, updatedAt: '2026-06-07T20:00:00Z', source: 'Demo market feed',
    rating: 'Watch', alertCount: 3,
  },
}

export const mockCandles = Array.from({ length: 48 }, (_, idx) => {
  const base = 118 + idx * 0.48 + Math.sin(idx / 4) * 4
  return {
    date: `W${idx + 1}`,
    close: Number(base.toFixed(2)),
    ma20: Number((base - 1.8 + Math.sin(idx / 6)).toFixed(2)),
    ma50: Number((base - 4.1 + Math.cos(idx / 8)).toFixed(2)),
    volume: Math.round(110 + Math.sin(idx / 3) * 38 + idx * 1.3),
  }
})

export const marketIndexes = [
  { symbol: 'SPY', label: 'S&P 500 ETF', price: 612.42, changePercent: 0.84 },
  { symbol: 'QQQ', label: 'Nasdaq 100 ETF', price: 542.18, changePercent: 1.26 },
  { symbol: 'DIA', label: 'Dow ETF', price: 427.05, changePercent: 0.31 },
  { symbol: 'IWM', label: 'Russell 2000 ETF', price: 221.73, changePercent: -0.18 },
]

export const sectorPerformance = [
  { name: 'Technology', changePercent: 1.72 },
  { name: 'Communication', changePercent: 0.96 },
  { name: 'Financials', changePercent: 0.42 },
  { name: 'Industrials', changePercent: 0.28 },
  { name: 'Healthcare', changePercent: -0.21 },
  { name: 'Energy', changePercent: -0.63 },
  { name: 'Utilities', changePercent: -0.77 },
  { name: 'Consumer Disc.', changePercent: 0.13 },
]

export const stockAnalysis = {
  type: 'stock_analysis', symbol: 'NVDA', company_name: 'NVIDIA Corp.', rating: 'Buy', target_price: 168,
  current_price: 142.63, upside_percent: 17.8, confidence: 74, horizon: '6-12 months',
  summary: 'AI accelerator demand remains the dominant driver, but the setup requires monitoring hyperscaler capex concentration and gross-margin normalization.',
  thesis: 'Base case assumes continued data-center growth, networking attach-rate expansion, and software monetization offsetting slower gaming recovery.',
  key_drivers: ['Data-center revenue growth', 'Blackwell supply ramp', 'Networking attach rates', 'Enterprise inference adoption'],
  risks: ['Hyperscaler capex pause', 'Export restrictions', 'ASIC substitution', 'Gross-margin compression'],
  catalysts: ['Next earnings call', 'Blackwell availability update', 'Large enterprise AI deployments'],
  sources: [
    { title: 'Latest company filings', publisher: 'SEC', type: 'filing', published_at: '2026-05-29' },
    { title: 'Demo quote snapshot', publisher: 'EdgeCitadel demo feed', type: 'quote', published_at: '2026-06-07' },
  ],
}

export const fundamentals = [
  { period: 'FY22', revenue: 27, eps: 3.85, margin: 25, fcf: 8.1 },
  { period: 'FY23', revenue: 27, eps: 3.34, margin: 16, fcf: 3.8 },
  { period: 'FY24', revenue: 61, eps: 12.96, margin: 49, fcf: 27.0 },
  { period: 'FY25', revenue: 130, eps: 29.75, margin: 56, fcf: 60.8 },
]

export const peers = [
  { symbol: 'NVDA', growth: 88, grossMargin: 75, forwardPE: 32, evSales: 18.4, rating: 'Buy' },
  { symbol: 'AMD', growth: 28, grossMargin: 52, forwardPE: 29, evSales: 7.8, rating: 'Hold' },
  { symbol: 'AVGO', growth: 22, grossMargin: 69, forwardPE: 27, evSales: 12.3, rating: 'Buy' },
  { symbol: 'INTC', growth: 5, grossMargin: 41, forwardPE: 21, evSales: 2.1, rating: 'Watch' },
]

export const newsEvents = [
  { type: 'earnings', title: 'Next earnings window expected in late August', source: 'Calendar', sentiment: 'neutral', impact: 'high', time: '2026-08-27' },
  { type: 'news', title: 'Cloud AI infrastructure demand remains elevated', source: 'Demo News', sentiment: 'positive', impact: 'medium', time: '2026-06-06' },
  { type: 'filing', title: 'Quarterly filing reviewed by research agent', source: 'SEC', sentiment: 'neutral', impact: 'medium', time: '2026-05-29' },
]

export const positions = [
  { symbol: 'NVDA', weight: 12.5, marketValue: '$125,400', pnl: 18.2, risk: 'High' },
  { symbol: 'MSFT', weight: 9.8, marketValue: '$98,100', pnl: 7.4, risk: 'Medium' },
  { symbol: 'AAPL', weight: 7.6, marketValue: '$76,200', pnl: -1.3, risk: 'Medium' },
  { symbol: 'SPY', weight: 22.0, marketValue: '$220,000', pnl: 5.1, risk: 'Low' },
]

export const alerts = [
  { id: 'a1', symbol: 'NVDA', severity: 'high', rule: 'Price above target watch band', trigger: 'Crossed $142 with volume above average', status: 'open', time: '2026-06-07T19:42:00Z' },
  { id: 'a2', symbol: 'TSLA', severity: 'medium', rule: 'Volume spike', trigger: 'Volume 1.4x 30-day average', status: 'open', time: '2026-06-07T18:10:00Z' },
  { id: 'a3', symbol: 'AAPL', severity: 'low', rule: 'News sentiment drift', trigger: 'Negative product-cycle headlines increased', status: 'acknowledged', time: '2026-06-06T21:15:00Z' },
]
