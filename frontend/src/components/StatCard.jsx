function Sparkline({ data, valueClass }) {
  if (!data || data.length < 2) return null
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const W = 64, H = 26, PAD = 2
  const pts = data.map((v, i) => {
    const x = PAD + (i / (data.length - 1)) * (W - PAD * 2)
    const y = H - PAD - ((v - min) / range) * (H - PAD * 2)
    return `${x},${y}`
  }).join(' ')
  const colorMap = { green: '#10b981', lime: '#84cc16', amber: '#f59e0b', orange: '#f97316', red: '#ef4444' }
  const stroke = colorMap[valueClass] || '#0056b3'
  return (
    <svg width={W} height={H} style={{ display: 'block', marginTop: 8, opacity: 0.85 }}>
      <polyline points={pts} fill="none" stroke={stroke} strokeWidth="1.8" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  )
}

function SplitValue({ value }) {
  if (typeof value !== 'string') return <>{value}</>
  const m = value.match(/^([\d.,—]+)\s*(.*)$/)
  if (!m || !m[2]) return <>{value}</>
  return <><span className="value-number">{m[1]}</span><span className="value-unit">{m[2]}</span></>
}

export default function StatCard({ label, value, valueClass = '', sub, icon, delta, sparkData }) {
  return (
    <div className="stat-card">
      {icon && <div className="stat-icon" aria-hidden="true">{icon}</div>}
      <div className="label">{label}</div>
      <div className={`value ${valueClass}`}><SplitValue value={value} /></div>
      {sub && <div className="sub">{sub}</div>}
      {delta && (
        <div className={`stat-delta ${delta.positive ? 'up' : 'down'}`}>
          {delta.positive ? '↑' : '↓'} {delta.text} vs last week
        </div>
      )}
      <Sparkline data={sparkData} valueClass={valueClass} />
    </div>
  )
}
