import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, Legend,
} from 'recharts'
import { utilColor, fmtKw } from '../../utils/status'

// Fill in missing hours with 0
function fillHours(data) {
  const map = Object.fromEntries(data.map(d => [d.hour, d]))
  return Array.from({ length: 24 }, (_, h) => map[h] ?? { hour: h, avg_utilisation_pct: 0, avg_est_kw: 0, avg_visit_starts: 0, data_points: 0 })
}

function fmtHour(h) {
  if (h === 0) return '12am'
  if (h === 12) return '12pm'
  return h < 12 ? `${h}am` : `${h - 12}pm`
}

function barFill(pct) {
  if (pct === 0) return '#E5E7EB'
  return utilColor(pct)
}

const CustomTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null
  const d = payload[0].payload
  return (
    <div style={{
      background: '#FFFFFF', border: '1px solid #E5E7EB',
      borderRadius: 4, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Inter, sans-serif',
    }}>
      <div style={{ color: '#6B7280', marginBottom: 4 }}>{fmtHour(d.hour)}</div>
      <div style={{ color: barFill(d.avg_utilisation_pct), fontWeight: 600 }}>
        {d.avg_utilisation_pct}% avg utilisation
      </div>
      {d.avg_est_kw > 0 && (
        <div style={{ color: '#f59e0b', marginTop: 2 }}>Est. {fmtKw(d.avg_est_kw)}</div>
      )}
      {d.avg_visit_starts > 0 && (
        <div style={{ color: '#818CF8', marginTop: 2 }}>{d.avg_visit_starts} avg visit starts</div>
      )}
      <div style={{ color: '#6B7280', fontSize: 11, marginTop: 2 }}>
        {d.data_points} data points
      </div>
    </div>
  )
}

function EmptyHourly() {
  return (
    <div style={{ textAlign: 'center', padding: '32px 20px', color: 'var(--text-muted)' }}>
      <div style={{ fontSize: 32, marginBottom: 12 }}>🕐</div>
      <div style={{ fontWeight: 600, marginBottom: 6, color: 'var(--text)' }}>Not enough data yet</div>
      <div style={{ fontSize: 13 }}>
        This chart builds up over 24+ hours of polling.<br />
        Come back after the scraper has run throughout a full day.
      </div>
    </div>
  )
}

export default function HourlyPattern({ data }) {
  const chartData = fillHours(data)
  const hasData = chartData.some(d => d.avg_utilisation_pct > 0)

  if (!hasData) return <EmptyHourly />

  return (
    <ResponsiveContainer width="100%" height={240}>
      <BarChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
        <XAxis
          dataKey="hour"
          tickFormatter={fmtHour}
          tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }}
          axisLine={false}
          tickLine={false}
          interval={2}
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
          yAxisId="kw"
          orientation="right"
          tick={false}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="visits"
          orientation="right"
          tick={{ fill: '#818CF8', fontSize: 10, fontFamily: 'Inter, sans-serif' }}
          axisLine={false}
          tickLine={false}
          tickFormatter={v => v > 0 ? v : ''}
          width={28}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(0,0,0,0.03)' }} />
        <Legend
          wrapperStyle={{ fontSize: 11, fontFamily: 'Inter, sans-serif', paddingTop: 4 }}
          formatter={(value) => {
            if (value === 'avg_utilisation_pct') return 'Utilisation %'
            if (value === 'avg_est_kw') return 'Est. Load (kW)'
            if (value === 'avg_visit_starts') return 'Avg Visit Starts'
            return value
          }}
        />
        <Bar yAxisId="pct" dataKey="avg_utilisation_pct" radius={[4, 4, 0, 0]} maxBarSize={10}>
          {chartData.map((d) => (
            <Cell key={d.hour} fill={barFill(d.avg_utilisation_pct)} />
          ))}
        </Bar>
        <Bar yAxisId="kw" dataKey="avg_est_kw" fill="#f59e0b" radius={[4, 4, 0, 0]} maxBarSize={10} />
        <Bar yAxisId="visits" dataKey="avg_visit_starts" fill="#818CF8" radius={[4, 4, 0, 0]} maxBarSize={10} />
      </BarChart>
    </ResponsiveContainer>
  )
}
