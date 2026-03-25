import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Brush, Legend,
} from 'recharts'

function fmtTime(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  const hh = d.getUTCHours().toString().padStart(2, '0')
  const mm = d.getUTCMinutes().toString().padStart(2, '0')
  return `${hh}:${mm} UTC`
}

function fmtKwh(v) {
  if (v == null) return null
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)} GWh`
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)} MWh`
  return `${Math.round(v)} kWh`
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  const util = payload.find(p => p.dataKey === 'avg_utilisation_pct')
  const charging = payload.find(p => p.dataKey === 'total_charging')
  const kwh = payload.find(p => p.dataKey === 'total_estimated_kwh')
  return (
    <div style={{
      background: '#0d1220', border: '1px solid #1c2840',
      borderRadius: 8, padding: '10px 14px', fontSize: 13,
      fontFamily: 'Outfit, sans-serif',
    }}>
      <div style={{ color: '#5c7a99', marginBottom: 4 }}>{label}</div>
      {util && (
        <div style={{ color: '#22d3ee', fontWeight: 600 }}>
          Avg utilisation: {util.value}%
        </div>
      )}
      {charging && (
        <div style={{ color: '#10b981' }}>
          Charging: {charging.value}
        </div>
      )}
      {kwh && kwh.value != null && (
        <div style={{ color: '#a78bfa' }}>
          Est. energy: {fmtKwh(kwh.value)}
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
        <YAxis yAxisId="kwh" orientation="right" hide />
        <Tooltip content={<CustomTooltip />} />
        <Legend
          wrapperStyle={{ fontSize: 11, fontFamily: 'Outfit, sans-serif', color: '#5c7a99', paddingBottom: 4 }}
          formatter={(value) => value === 'Est. kWh' ? `${value} (±30–50%)` : value}
        />
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
        <Line
          yAxisId="kwh"
          type="monotone"
          dataKey="total_estimated_kwh"
          stroke="#a78bfa"
          strokeWidth={1.5}
          dot={false}
          strokeDasharray="2 3"
          name="Est. kWh"
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
