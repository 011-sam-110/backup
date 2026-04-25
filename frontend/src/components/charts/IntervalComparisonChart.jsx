import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Brush, Legend,
} from 'recharts'

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  const util     = payload.find(p => p.dataKey === 'util')
  const charging = payload.find(p => p.dataKey === 'charging')
  return (
    <div style={{
      background: '#FFFFFF', border: '1px solid #E5E7EB',
      borderRadius: 4, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Inter, sans-serif',
    }}>
      <div style={{ color: '#6B7280', marginBottom: 4 }}>{label}</div>
      {util && (
        <div style={{ color: '#2563EB', fontWeight: 600 }}>
          Utilisation: {util.value}%
        </div>
      )}
      {charging && (
        <div style={{ color: '#10b981' }}>
          Charging: {charging.value}
        </div>
      )}
    </div>
  )
}

function IntervalChart({ intervalMin, rows }) {
  const data = rows
    .filter(r => r[`c_${intervalMin}`] != null)
    .map(r => ({ ts: r.ts, util: r[`u_${intervalMin}`], charging: r[`c_${intervalMin}`] }))

  if (!data.length) {
    return (
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 8 }}>
          {intervalMin}-Minute Interval
        </div>
        <div className="empty">No data for this interval in this window yet.</div>
      </div>
    )
  }

  return (
    <div style={{ marginBottom: 36 }}>
      <div style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 8 }}>
        {intervalMin}-Minute Interval — {data.length} data points
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data} margin={{ top: 5, right: 20, bottom: 30, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
          <XAxis
            dataKey="ts"
            tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }}
            axisLine={{ stroke: '#E5E7EB' }}
            tickLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="pct"
            domain={[0, 100]}
            tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }}
            axisLine={false}
            tickLine={false}
            tickFormatter={v => `${v}%`}
          />
          <YAxis
            yAxisId="count"
            orientation="right"
            tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }}
            axisLine={false}
            tickLine={false}
            allowDecimals={false}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: 11, fontFamily: 'Inter, sans-serif', color: '#6B7280', paddingBottom: 4 }} />
          <ReferenceLine yAxisId="pct" y={50} stroke="#f59e0b" strokeDasharray="4 4" opacity={0.4} />
          <ReferenceLine yAxisId="pct" y={80} stroke="#ef4444" strokeDasharray="4 4" opacity={0.4} />
          <Line
            yAxisId="pct"
            type="monotone"
            dataKey="util"
            stroke="#2563EB"
            strokeWidth={2}
            dot={false}
            name="Utilisation %"
            isAnimationActive={false}
          />
          <Line
            yAxisId="count"
            type="monotone"
            dataKey="charging"
            stroke="#10b981"
            strokeWidth={1.5}
            dot={false}
            strokeDasharray="4 2"
            name="Charging count"
            isAnimationActive={false}
          />
          <Brush
            dataKey="ts"
            height={24}
            stroke="#E5E7EB"
            fill="#F9FAFB"
            travellerWidth={8}
            travellerStyle={{ fill: '#0056b3' }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function IntervalComparisonChart({ data }) {
  const { hub_name, intervals = [], rows = [] } = data

  if (!rows.length) {
    return <div className="empty">No targeted snapshot data for this hub in this window.</div>
  }

  return (
    <div>
      <div style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 24 }}>
        {hub_name} — {rows.length} base snapshots · {intervals.length} interval{intervals.length !== 1 ? 's' : ''} configured
      </div>
      {intervals.map(iv => (
        <IntervalChart key={iv} intervalMin={iv} rows={rows} />
      ))}
    </div>
  )
}
