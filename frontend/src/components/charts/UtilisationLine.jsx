import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Brush,
} from 'recharts'

function fmtTime(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  const hh = d.getUTCHours().toString().padStart(2, '0')
  const mm = d.getUTCMinutes().toString().padStart(2, '0')
  return `${hh}:${mm} UTC`
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#0d1220', border: '1px solid #1c2840',
      borderRadius: 8, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Outfit, sans-serif',
    }}>
      <div style={{ color: '#5c7a99', marginBottom: 4 }}>{label}</div>
      <div style={{ color: '#22d3ee', fontWeight: 600 }}>
        Avg utilisation: {payload[0].value}%
      </div>
      {payload[1] && (
        <div style={{ color: '#10b981' }}>
          Charging: {payload[1].value}
        </div>
      )}
    </div>
  )
}

export default function UtilisationLine({ data }) {
  const chartData = data.map(d => ({
    ...d,
    time: fmtTime(d.scraped_at),
  }))

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={chartData} margin={{ top: 5, right: 20, bottom: 30, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1c2840" />
        <XAxis
          dataKey="time"
          tick={{ fill: '#5c7a99', fontSize: 11, fontFamily: 'Outfit, sans-serif' }}
          axisLine={{ stroke: '#1c2840' }}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          yAxisId="pct"
          domain={[0, 100]}
          tick={{ fill: '#5c7a99', fontSize: 11, fontFamily: 'Outfit, sans-serif' }}
          axisLine={false}
          tickLine={false}
          tickFormatter={v => `${v}%`}
        />
        <YAxis
          yAxisId="count"
          orientation="right"
          tick={{ fill: '#5c7a99', fontSize: 11, fontFamily: 'Outfit, sans-serif' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip content={<CustomTooltip />} />
        <ReferenceLine yAxisId="pct" y={50} stroke="#f59e0b" strokeDasharray="4 4" opacity={0.4} />
        <ReferenceLine yAxisId="pct" y={80} stroke="#ef4444" strokeDasharray="4 4" opacity={0.4} />
        <Line
          yAxisId="pct"
          type="monotone"
          dataKey="avg_utilisation_pct"
          stroke="#22d3ee"
          strokeWidth={2}
          dot={false}
          name="Avg utilisation %"
        />
        <Line
          yAxisId="count"
          type="monotone"
          dataKey="total_charging"
          stroke="#10b981"
          strokeWidth={1.5}
          dot={false}
          strokeDasharray="4 2"
          name="Total charging"
        />
        <Brush
          dataKey="time"
          height={24}
          stroke="#1c2840"
          fill="#0d1220"
          travellerWidth={8}
          startIndex={0}
          travellerStyle={{ fill: '#22d3ee' }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
