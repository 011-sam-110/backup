import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, Legend,
} from 'recharts'
import { utilColor, fmtKw, hubEstKw } from '../../utils/status'

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div style={{
      background: '#FFFFFF', border: '1px solid #E5E7EB',
      borderRadius: 4, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Inter, sans-serif',
    }}>
      <div style={{ color: '#212529', fontWeight: 600, marginBottom: 4 }}>{d.hub_name || d.uuid}</div>
      <div style={{ fontFamily: 'monospace', fontSize: 10, color: '#9CA3AF', marginBottom: 5 }}>{d.uuid}</div>
      <div style={{ color: utilColor(d.utilisation_pct), fontWeight: 600 }}>{d.utilisation_pct}% utilised</div>
      <div style={{ color: '#6B7280', marginTop: 2 }}>{d.charging_count} / {d.total_evses} charging</div>
      {d.est_kw > 0 && (
        <div style={{ color: '#f59e0b', marginTop: 2 }}>Est. load: {fmtKw(d.est_kw)}</div>
      )}
    </div>
  )
}

function makeCustomYTick(chartData) {
  return function CustomYTick({ x, y, payload }) {
    const hub = chartData.find(h => h.uuid === payload.value)
    const name = hub?.hub_name
      ? (hub.hub_name.length > 20 ? hub.hub_name.slice(0, 18) + '…' : hub.hub_name)
      : null

    return (
      <g transform={`translate(${x},${y})`}>
        <text
          x={-6} y={name ? -4 : 4}
          textAnchor="end"
          fill={name ? '#212529' : '#6B7280'}
          fontSize={11}
          fontFamily="Inter, sans-serif"
        >
          {name || payload.value.slice(0, 8) + '…'}
        </text>
        <text
          x={-6} y={name ? 8 : 18}
          textAnchor="end"
          fill="#9CA3AF"
          fontSize={9}
          fontFamily="monospace"
        >
          {payload.value}
        </text>
      </g>
    )
  }
}

export default function HubBarChart({ hubs, onHubClick, label }) {
  const chartData = [...hubs]
    .filter(h => h.utilisation_pct != null)
    .sort((a, b) => b.utilisation_pct - a.utilisation_pct)
    .map(h => ({ ...h, est_kw: hubEstKw(h) }))

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 12, marginBottom: 8 }}>
        {label ?? `All ${chartData.length} hubs · click a bar to see details`}
      </p>
      <div style={{ overflowY: 'auto', maxHeight: 560 }}>
        <ResponsiveContainer width="100%" height={Math.max(300, chartData.length * 60)}>
          <BarChart
            layout="vertical"
            data={chartData}
            margin={{ top: 20, right: 60, bottom: 5, left: 120 }}
            style={{ cursor: 'pointer' }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" horizontal={false} />
            <XAxis
              xAxisId="pct"
              type="number"
              domain={[0, 100]}
              tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => `${v}%`}
              orientation="bottom"
            />
            <XAxis
              xAxisId="kw"
              type="number"
              domain={[0, 'auto']}
              tick={{ fill: '#f59e0b', fontSize: 10, fontFamily: 'Inter, sans-serif' }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => fmtKw(v)}
              orientation="top"
            />
            <YAxis
              type="category"
              dataKey="uuid"
              tick={makeCustomYTick(chartData)}
              axisLine={false}
              tickLine={false}
              width={160}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(0,0,0,0.03)' }} />
            <Legend
              wrapperStyle={{ fontSize: 11, fontFamily: 'Inter, sans-serif', paddingTop: 8 }}
              formatter={(value) => value === 'utilisation_pct' ? 'Utilisation %' : 'Est. Load (kW)'}
            />
            <Bar
              xAxisId="pct"
              dataKey="utilisation_pct"
              radius={[0, 4, 4, 0]}
              maxBarSize={14}
              onClick={(data) => onHubClick?.(data)}
            >
              {chartData.map((h) => (
                <Cell key={h.uuid} fill="#2563EB" />
              ))}
            </Bar>
            <Bar
              xAxisId="kw"
              dataKey="est_kw"
              fill="#f59e0b"
              radius={[0, 4, 4, 0]}
              maxBarSize={14}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
