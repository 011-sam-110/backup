import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
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
    <div style={{ background: '#FFFFFF', border: '1px solid #E5E7EB', borderRadius: 4, padding: '10px 14px', fontSize: 12, fontFamily: 'Inter, sans-serif' }}>
      <div style={{ color: '#6B7280', marginBottom: 6 }}>{label}</div>
      {payload.map(p => (
        <div key={p.dataKey} style={{ color: p.fill, marginBottom: 2 }}>
          {p.name}: {p.value?.toFixed(1)}%
        </div>
      ))}
    </div>
  )
}

export default function ReliabilityChart({ data = [] }) {
  if (data.length < 2) {
    return <div className="empty" style={{ padding: '20px 0' }}>Need more data to show network composition.</div>
  }

  const chartData = data.map(d => ({ ...d, time: fmtTime(d.scraped_at) }))

  return (
    <ResponsiveContainer width="100%" height={240}>
      <AreaChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
        <defs>
          <linearGradient id="gCharging"  x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#10b981" stopOpacity={0.9}/><stop offset="100%" stopColor="#10b981" stopOpacity={0.6}/></linearGradient>
          <linearGradient id="gAvailable" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#60a5fa" stopOpacity={0.9}/><stop offset="100%" stopColor="#60a5fa" stopOpacity={0.6}/></linearGradient>
          <linearGradient id="gInop"      x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f59e0b" stopOpacity={0.9}/><stop offset="100%" stopColor="#f59e0b" stopOpacity={0.6}/></linearGradient>
          <linearGradient id="gOos"       x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#ef4444" stopOpacity={0.9}/><stop offset="100%" stopColor="#ef4444" stopOpacity={0.6}/></linearGradient>
          <linearGradient id="gUnknown"   x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#6b7280" stopOpacity={0.9}/>
            <stop offset="100%" stopColor="#6b7280" stopOpacity={0.6}/>
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
        <XAxis dataKey="time" tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
        <YAxis domain={[0, 100]} tick={{ fill: '#6B7280', fontSize: 11, fontFamily: 'Inter, sans-serif' }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
        <Tooltip content={<CustomTooltip />} />
        <Legend iconType="square" iconSize={10} wrapperStyle={{ fontSize: 12, color: '#6B7280', fontFamily: 'Inter, sans-serif' }} />
        <Area stackId="a" type="monotone" dataKey="charging_pct"    name="Charging"     stroke="#10b981" fill="url(#gCharging)"  />
        <Area stackId="a" type="monotone" dataKey="available_pct"   name="Available"    stroke="#60a5fa" fill="url(#gAvailable)" />
        <Area stackId="a" type="monotone" dataKey="inoperative_pct" name="Inoperative"  stroke="#f59e0b" fill="url(#gInop)"      />
        <Area stackId="a" type="monotone" dataKey="oos_pct"         name="Out of Order" stroke="#ef4444" fill="url(#gOos)"       />
        <Area stackId="a" type="monotone" dataKey="unknown_pct"     name="Unknown"      stroke="#6b7280" fill="url(#gUnknown)"   />
      </AreaChart>
    </ResponsiveContainer>
  )
}
