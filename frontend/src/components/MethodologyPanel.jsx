import { useState } from 'react'

const ROW = ({ label, children }) => (
  <div style={{ display: 'grid', gridTemplateColumns: '160px 1fr', gap: '6px 16px', marginBottom: 8 }}>
    <span style={{ color: 'var(--text-muted)', fontSize: 12, paddingTop: 1 }}>{label}</span>
    <span style={{ color: 'var(--text)', fontSize: 12, lineHeight: 1.55 }}>{children}</span>
  </div>
)

export default function MethodologyPanel() {
  const [open, setOpen] = useState(false)

  return (
    <div style={{ marginBottom: 18 }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          background: 'none', border: 'none', cursor: 'pointer',
          color: 'var(--text-muted)', fontSize: 12, padding: '4px 0',
          display: 'flex', alignItems: 'center', gap: 6,
        }}
      >
        <span style={{
          display: 'inline-block',
          transform: open ? 'rotate(90deg)' : 'none',
          transition: 'transform 0.15s',
          fontSize: 10,
        }}>▶</span>
        How is this data calculated?
      </button>

      {open && (
        <div style={{
          marginTop: 10,
          background: 'var(--card-bg)',
          border: '1px solid var(--border)',
          borderRadius: 10,
          padding: '16px 20px',
        }}>
          <div style={{
            color: 'var(--accent)', fontWeight: 700, fontSize: 11,
            textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 14,
          }}>
            Data Methodology
          </div>

          <ROW label="Utilisation %">
            Charging EVSEs ÷ total EVSEs (available + charging + inoperative + out-of-order + unknown) × 100.
          </ROW>

          <ROW label="Est. Load (kW)">
            <span>
              <code style={{ background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 4, fontSize: 11 }}>
                charging_count × hub_max_kW × 0.7
              </code>
            </span>
          </ROW>

          <ROW label="Known limitations">
            <span>
              <b>hub_max_kW</b> is the site's peak rating, not per-EVSE power. A 350 kW hub with 4 ports
              shares that capacity — individual sessions may draw far less. Actual load may be
              <b> 30–50% lower</b> than shown.
            </span>
          </ROW>

          <ROW label="Visits & dwell time">
            <span>
              A visit is inferred when <code style={{ background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 4, fontSize: 11 }}>charging_count</code> increases
              between two consecutive 5-minute snapshots — each unit of increase is counted as one visit start.
              When the count decreases, the oldest open visits are closed first (FIFO) and dwell time is recorded.
              Only <b>completed</b> visits are included in the average dwell; active sessions are excluded.{' '}
              <b>Limitation:</b> any session that both starts and ends within a single 5-minute polling window
              is invisible to this method. Dwell times represent the time between when a start and end were
              detected, so they are lower bounds for very short sessions and may over- or under-count when
              multiple arrivals and departures coincide in the same window.
            </span>
          </ROW>
        </div>
      )}
    </div>
  )
}
