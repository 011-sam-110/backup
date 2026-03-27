import { useState } from 'react'

const ROW = ({ label, children }) => (
  <div style={{ display: 'grid', gridTemplateColumns: '170px 1fr', gap: '6px 16px', marginBottom: 10 }}>
    <span style={{ color: 'var(--text-muted)', fontSize: 12, paddingTop: 1 }}>{label}</span>
    <span style={{ color: 'var(--text)', fontSize: 12, lineHeight: 1.55 }}>{children}</span>
  </div>
)

const Code = ({ children }) => (
  <code style={{ background: 'rgba(127,127,127,0.12)', padding: '1px 5px', borderRadius: 4, fontSize: 11 }}>
    {children}
  </code>
)

const Divider = ({ title }) => (
  <div style={{
    color: 'var(--text-muted)', fontWeight: 700, fontSize: 10,
    textTransform: 'uppercase', letterSpacing: '0.08em',
    marginTop: 16, marginBottom: 10, borderTop: '1px solid var(--border)', paddingTop: 12,
  }}>
    {title}
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

          <Divider title="Data Source" />

          <ROW label="Where data comes from">
            Hub status is scraped directly from the Zapmap/Electroverse public API on a
            fixed interval. Each scrape captures a snapshot of every hub's live chargepoint
            statuses (available, charging, inoperative, out-of-order). Hub metadata — name,
            address, connector types, operator, ratings, and pricing — is stored from the
            first time a hub is seen and updated on each scrape.
          </ROW>

          <ROW label="Scrape frequency">
            The scraper runs every 5 minutes. All utilisation, load, and status figures
            reflect the most recent completed scrape. The <b>Last Updated</b> timestamp on
            each hub shows exactly when that hub's data was last captured.
          </ROW>

          <ROW label="Coverage">
            Only hubs in England (lat 49.9–55.8, lng −5.7–1.8) with a rated capacity
            of 100 kW or above are collected.
          </ROW>

          <Divider title="Live Status Figures" />

          <ROW label="Utilisation %">
            <Code>charging ÷ (available + charging + inoperative + out-of-order + unknown) × 100</Code>
            {' '}Inoperative and unknown chargepoints are included in the denominator —
            a hub where half the ports are broken will never show 100% utilisation.
          </ROW>

          <ROW label="Est. Load (kW)">
            <Code>charging_count × hub_max_kW × 0.7</Code>
            {' '}The 0.7 factor accounts for the fact that real-world charging sessions
            rarely draw the full rated capacity (batteries taper as they fill, and
            vehicles have different acceptance rates). This is an estimate only.
          </ROW>

          <ROW label="Charging duration (⏱)">
            Shown on individual chargepoint cards in the hub detail view. Uses our
            scraper history to find when <Code>charging_count</Code> last transitioned
            from 0 → 1 for this hub — accurate to approximately the scrape interval
            (~5 minutes). If no scraper history is available for a hub, falls back
            to the operator's own <Code>network_updated_at</Code> timestamp, which can
            be hours or days stale depending on the operator.
          </ROW>

          <Divider title="Visits & Dwell Time" />

          <ROW label="How visits are detected">
            A visit is inferred when <Code>charging_count</Code> increases between two
            consecutive snapshots — each unit of increase counts as one visit start.
            When charging_count decreases, the oldest open visits are closed first (FIFO)
            and dwell time is recorded. Only <b>completed</b> visits are included in
            averages; active sessions are excluded.
          </ROW>

          <ROW label="Dwell time accuracy">
            Dwell times are the interval between when a session start and end were
            detected by the scraper, not the exact start and end of the charge.
            They are accurate to within one scrape interval (~5 minutes) on each end.
            Any session that both starts and ends within a single polling window is
            invisible to this method.
          </ROW>

          <Divider title="Known Limitations" />

          <ROW label="Max power is site-level">
            <Code>hub_max_kW</Code> is the site's total rated capacity, not per-EVSE
            power. A 350 kW hub with 4 ports shares that capacity — individual sessions
            may draw far less. Actual load is typically <b>30–50% lower</b> than the
            estimated figure.
          </ROW>

          <ROW label="Operator data quality">
            Per-chargepoint statuses (available, inoperative, charging) depend on
            operators publishing accurate real-time data to the Zapmap network. Some
            operators report infrequently or mark stuck chargepoints as available.
            Utilisation figures for those operators may be understated.
          </ROW>

          <ROW label="Multi-EVSE session attribution">
            Charging duration uses hub-level <Code>charging_count</Code> transitions.
            For hubs with multiple EVSEs, all currently-charging ports show the same
            "session started" time — the earliest charging session we detected, not
            each individual EVSE's start time.
          </ROW>
        </div>
      )}
    </div>
  )
}
