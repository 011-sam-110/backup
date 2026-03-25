import { useState } from 'react'

function fmt(n) {
  return Math.round(n).toLocaleString('en-GB')
}

function fmtGbp(n) {
  return '£' + Math.round(n).toLocaleString('en-GB')
}

export default function ProjectionsPanel({ avgUtil = 0 }) {
  const [open, setOpen] = useState(false)
  const [hubs, setHubs] = useState(100)
  const [bays, setBays] = useState(24)
  const [gbpPerSession, setGbpPerSession] = useState(8)
  const [sessionMins, setSessionMins] = useState(45)

  const sessionsPerBayPerHour = 60 / sessionMins
  const operatingHours = 16
  const dailySessions = Math.max(0,
    hubs * bays * (avgUtil / 100) * sessionsPerBayPerHour * operatingHours
  )
  const dailyRevenue = dailySessions * gbpPerSession
  const annualRevenue = dailyRevenue * 365

  function Slider({ label, value, min, max, step = 1, onChange, fmt: fmtFn }) {
    return (
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>{label}</span>
          <span style={{ fontWeight: 600, fontSize: 13 }}>{fmtFn ? fmtFn(value) : value}</span>
        </div>
        <input
          type="range"
          min={min} max={max} step={step}
          value={value}
          onChange={e => onChange(Number(e.target.value))}
          style={{ width: '100%', accentColor: 'var(--amber)' }}
        />
      </div>
    )
  }

  return (
    <div className="projection-panel">
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', userSelect: 'none' }}
        onClick={() => setOpen(o => !o)}
      >
        <span style={{ fontSize: 13, fontWeight: 600 }}>
          {open ? '▾' : '▸'} Scenario Projection
        </span>
        <span className="projection-badge">⚠ Projection</span>
      </div>

      {open && (
        <div style={{ marginTop: 20 }}>
          <div className="projection-grid">
            <Slider label="Hubs planned" value={hubs} min={1} max={500} onChange={setHubs} />
            <Slider label="Bays per hub (EVSEs)" value={bays} min={1} max={50} onChange={setBays} />
            <Slider label="£ per session" value={gbpPerSession} min={1} max={30} onChange={setGbpPerSession} fmtFn={v => `£${v}`} />
            <Slider label="Session duration (min)" value={sessionMins} min={15} max={120} step={5} onChange={setSessionMins} fmtFn={v => `${v} min`} />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginTop: 4 }}>
            <div>
              <div style={{ color: 'var(--text-muted)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
                Daily Sessions
              </div>
              <div className="projection-output">{fmt(dailySessions)}</div>
            </div>
            <div>
              <div style={{ color: 'var(--text-muted)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
                Daily Revenue
              </div>
              <div className="projection-output">{fmtGbp(dailyRevenue)}</div>
            </div>
            <div>
              <div style={{ color: 'var(--text-muted)', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>
                Annual Revenue
              </div>
              <div className="projection-output">{fmtGbp(annualRevenue)}</div>
            </div>
          </div>

          <div style={{ marginTop: 14, color: 'var(--text-muted)', fontSize: 11 }}>
            Based on observed avg utilisation of <strong>{avgUtil}%</strong>. Assumes {operatingHours} operating hours/day and {sessionMins}-min sessions.
          </div>
        </div>
      )}
    </div>
  )
}
