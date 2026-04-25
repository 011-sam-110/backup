import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts'

const INTERVAL_COLORS = ['#4472C4', '#10b981', '#f59e0b', '#e11d48', '#8b5cf6']
const INTERVAL_DASH   = ['', '5 5', '2 4', '8 3', '3 6']

const CustomTooltip = ({ active, payload, label, intervals }) => {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#fff', border: '1px solid #E5E7EB',
      borderRadius: 4, padding: '10px 14px', fontSize: 13,
    }}>
      <div style={{ color: '#6B7280', marginBottom: 6 }}>{label}</div>
      {intervals.map((iv, i) => {
        const c = payload.find(p => p.dataKey === `c_${iv}`)
        const u = payload.find(p => p.dataKey === `u_${iv}`)
        if (!c && !u) return null
        return (
          <div key={iv} style={{ color: INTERVAL_COLORS[i % INTERVAL_COLORS.length], marginBottom: 2 }}>
            <strong>{iv}m:</strong>{' '}
            {c?.value != null ? `${c.value} charging` : '—'}
            {u?.value != null ? ` · ${u.value}%` : ''}
          </div>
        )
      })}
    </div>
  )
}

export default function IntervalComparisonChart({ data }) {
  const { hub_name, intervals = [], rows = [] } = data

  if (!rows.length) {
    return <div className="empty">No targeted snapshot data for this hub in this window.</div>
  }

  // Thin the X-axis ticks — show one every ~15 rows to avoid crowding
  const tickEvery = Math.max(1, Math.floor(rows.length / 20))
  const xTicks = rows.filter((_, i) => i % tickEvery === 0).map(r => r.ts)

  return (
    <div>
      <div style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>
        {hub_name} — {rows.length} base snapshots
      </div>

      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 8 }}>
          Charging Count
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={rows} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" />
            <XAxis
              dataKey="ts"
              ticks={xTicks}
              tick={{ fontSize: 11, fill: '#9CA3AF' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 11, fill: '#9CA3AF' }}
              tickLine={false}
              axisLine={false}
              allowDecimals={false}
              width={32}
            />
            <Tooltip content={<CustomTooltip intervals={intervals} />} />
            <Legend
              formatter={(val) => {
                const m = val.match(/^c_(\d+)$/)
                return m ? `${m[1]}m interval` : val
              }}
              wrapperStyle={{ fontSize: 12 }}
            />
            {intervals.map((iv, i) => (
              <Line
                key={`c_${iv}`}
                dataKey={`c_${iv}`}
                name={`c_${iv}`}
                stroke={INTERVAL_COLORS[i % INTERVAL_COLORS.length]}
                strokeWidth={i === 0 ? 2 : 1.5}
                strokeDasharray={INTERVAL_DASH[i % INTERVAL_DASH.length]}
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div>
        <div style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text-muted)', marginBottom: 8 }}>
          Utilisation %
        </div>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={rows} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" />
            <XAxis
              dataKey="ts"
              ticks={xTicks}
              tick={{ fontSize: 11, fill: '#9CA3AF' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 11, fill: '#9CA3AF' }}
              tickLine={false}
              axisLine={false}
              width={36}
              tickFormatter={v => `${v}%`}
            />
            <Tooltip
              formatter={(v, name) => {
                const m = name.match(/^u_(\d+)$/)
                return [v != null ? `${v}%` : '—', m ? `${m[1]}m` : name]
              }}
              labelStyle={{ color: '#6B7280' }}
            />
            {intervals.map((iv, i) => (
              <Line
                key={`u_${iv}`}
                dataKey={`u_${iv}`}
                name={`u_${iv}`}
                stroke={INTERVAL_COLORS[i % INTERVAL_COLORS.length]}
                strokeWidth={i === 0 ? 2 : 1.5}
                strokeDasharray={INTERVAL_DASH[i % INTERVAL_DASH.length]}
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
