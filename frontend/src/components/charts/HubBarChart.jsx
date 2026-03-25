import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell,
} from 'recharts'
import { utilColor } from '../../utils/status'

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div style={{
      background: '#0d1220', border: '1px solid #1c2840',
      borderRadius: 8, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Outfit, sans-serif',
    }}>
      <div style={{ color: '#5c7a99', marginBottom: 4 }}>{d.hub_name || d.uuid}</div>
      <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 10, color: '#2a3d52', marginBottom: 5 }}>{d.uuid}</div>
      <div style={{ color: utilColor(d.utilisation_pct), fontWeight: 600 }}>{d.utilisation_pct}% utilised</div>
      <div style={{ color: '#dae5f5', marginTop: 2 }}>{d.charging_count} / {d.total_evses} charging</div>
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
          fill={name ? '#dae5f5' : '#5c7a99'}
          fontSize={11}
          fontFamily="Outfit, sans-serif"
        >
          {name || payload.value.slice(0, 8) + '…'}
        </text>
        <text
          x={-6} y={name ? 8 : 18}
          textAnchor="end"
          fill="#2a3d52"
          fontSize={9}
          fontFamily="JetBrains Mono, monospace"
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

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 12, marginBottom: 8 }}>
        {label ?? `All ${chartData.length} hubs · click a bar to see details`}
      </p>
      <div style={{ overflowY: 'auto', maxHeight: 560 }}>
        <ResponsiveContainer width="100%" height={Math.max(300, chartData.length * 46)}>
          <BarChart
            layout="vertical"
            data={chartData}
            margin={{ top: 5, right: 60, bottom: 5, left: 100 }}
            style={{ cursor: 'pointer' }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#1c2840" horizontal={false} />
            <XAxis
              type="number"
              domain={[0, 100]}
              tick={{ fill: '#5c7a99', fontSize: 11, fontFamily: 'Outfit, sans-serif' }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => `${v}%`}
            />
            <YAxis
              type="category"
              dataKey="uuid"
              tick={makeCustomYTick(chartData)}
              axisLine={false}
              tickLine={false}
              width={160}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
            <Bar
              dataKey="utilisation_pct"
              radius={[0, 4, 4, 0]}
              maxBarSize={20}
              onClick={(data) => onHubClick?.(data)}
            >
              {chartData.map((h) => (
                <Cell key={h.uuid} fill={utilColor(h.utilisation_pct)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
