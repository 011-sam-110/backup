import { useState } from 'react'
import {
  ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, ResponsiveContainer,
} from 'recharts'
import * as XLSX from 'xlsx'

const TABS = [
  { label: '7D',  days: 7 },
  { label: '30D', days: 30 },
  { label: '90D', days: 90 },
]

function movingAverage(data, window = 7) {
  return data.map((d, i) => {
    const slice = data.slice(Math.max(0, i - window + 1), i + 1)
    const avg = slice.reduce((s, x) => s + (x.avg_utilisation_pct || 0), 0) / slice.length
    return { ...d, ma: Math.round(avg * 10) / 10 }
  })
}

function fmtDate(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #E5E7EB', borderRadius: 4, padding: '10px 14px', fontSize: 13, fontFamily: 'Inter, sans-serif' }}>
      <div style={{ color: '#6B7280', marginBottom: 4 }}>{label}</div>
      {payload.map(p => (
        <div key={p.dataKey} style={{ color: p.color, fontWeight: 600 }}>
          {p.name}: {p.value}{p.dataKey !== 'total_charging' ? '%' : ''}
        </div>
      ))}
    </div>
  )
}

function exportToExcel(data, days) {
  const rows = data.map(d => ({
    'Date': d.date,
    'Avg Utilisation %': d.avg_utilisation_pct,
    '7-Day MA %': d.ma,
    'Total Charging': d.total_charging,
    'Hub Count': d.hub_count,
  }))
  const ws = XLSX.utils.json_to_sheet(rows)
  const wb = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(wb, ws, `Trend ${days}D`)
  XLSX.writeFile(wb, `utilisation_trend_${days}d_${new Date().toISOString().slice(0, 10)}.xlsx`)
}

export default function WeeklyTrend({ onDaysChange, data = [], activeDays = 30 }) {
  const chartData = movingAverage(data.map(d => ({ ...d, label: fmtDate(d.date) })))

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <div className="trend-tabs">
          {TABS.map(t => (
            <button
              key={t.days}
              className={`trend-tab${activeDays === t.days ? ' active' : ''}`}
              onClick={() => onDaysChange(t.days)}
            >
              {t.label}
            </button>
          ))}
        </div>
        <button className="btn btn-outline" style={{ fontSize: 12, padding: '4px 12px' }} onClick={() => exportToExcel(chartData, activeDays)}>
          ↓ Export
        </button>
      </div>

      {chartData.length < 2 ? (
        <div className="empty">Need 2+ days of data to show this chart.</div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <ComposedChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
            <defs>
              <linearGradient id="utilGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#2563EB" stopOpacity={0.18} />
                <stop offset="95%" stopColor="#2563EB" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
            <XAxis dataKey="label" tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }} axisLine={{ stroke: '#E5E7EB' }} tickLine={false} interval="preserveStartEnd" />
            <YAxis domain={[0, 100]} tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine y={50} stroke="#f59e0b" strokeDasharray="4 4" opacity={0.4} />
            <ReferenceLine y={80} stroke="#ef4444" strokeDasharray="4 4" opacity={0.4} />
            <Area type="monotone" dataKey="avg_utilisation_pct" name="Avg Util" fill="url(#utilGrad)" stroke="#2563EB" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="ma" name="7-Day MA" stroke="#93C5FD" strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
