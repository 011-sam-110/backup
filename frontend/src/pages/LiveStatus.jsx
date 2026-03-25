import { useState, useEffect, useCallback } from 'react'
import StatCard from '../components/StatCard'
import HubTable from '../components/HubTable'
import PageLoader from '../components/PageLoader'
import HubDetailModal from '../components/HubDetailModal'
import { useFilters, applyFilters } from '../context/FilterContext'

const REFRESH_MS = 60_000

function Stat({ label, value, valueClass = '' }) {
  return (
    <span style={{ display: 'flex', alignItems: 'baseline', gap: 5 }}>
      <span className={valueClass} style={{ fontWeight: 700, fontSize: 15 }}>{value}</span>
      <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{label}</span>
    </span>
  )
}

function FilteredStatsStrip({ hubs }) {
  const totalEvses    = hubs.reduce((s, h) => s + (h.total_evses ?? 0), 0)
  const totalCharging = hubs.reduce((s, h) => s + (h.charging_count ?? 0), 0)
  const utilPct       = totalEvses > 0 ? totalCharging / totalEvses * 100 : 0
  const tier          = utilPct >= 50 ? 'red' : utilPct >= 30 ? 'amber' : 'green'

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 20,
      background: 'var(--accent-dim)', border: '1px solid rgba(34,211,238,0.2)',
      borderRadius: 9, padding: '9px 16px', marginBottom: 16,
      fontSize: 13, flexWrap: 'wrap',
    }}>
      <span style={{
        color: 'var(--accent)', fontWeight: 700, fontSize: 10,
        textTransform: 'uppercase', letterSpacing: '0.09em',
      }}>
        Selection
      </span>
      <Stat label="hubs"     value={hubs.length} />
      <Stat label="EVSEs"    value={totalEvses} />
      <Stat label="charging" value={totalCharging} valueClass="green" />
      <Stat label="avg util" value={`${utilPct.toFixed(2)}%`} valueClass={tier} />
    </div>
  )
}

export default function LiveStatus() {
  const [stats, setStats] = useState(null)
  const [deltas, setDeltas] = useState(null)
  const [sparkline, setSparkline] = useState([])
  const [hubs, setHubs] = useState([])
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [selectedHub, setSelectedHub] = useState(null)

  const filters = useFilters()
  const { hubsUrl, setAvailableOperators, dateRange } = filters

  const load = useCallback(async () => {
    try {
      const [statsRes, hubsRes, deltasRes, sparkRes] = await Promise.all([
        fetch('/api/stats'),
        fetch(hubsUrl()),
        fetch('/api/stats/deltas'),
        fetch('/api/sparkline?days=7'),
      ])
      const hubData = await hubsRes.json()
      setStats(await statsRes.json())
      setHubs(hubData)
      setDeltas(await deltasRes.json())
      setSparkline(await sparkRes.json())
      setLastUpdated(new Date())

      // Populate operator list in FilterContext
      const ops = [...new Set(hubData.map(h => h.operator).filter(Boolean))].sort()
      setAvailableOperators(ops)
    } catch {
      // silently retry on next interval
    } finally {
      setLoading(false)
    }
  }, [hubsUrl, setAvailableOperators]) // eslint-disable-line

  useEffect(() => {
    load()
    const id = setInterval(load, REFRESH_MS)
    return () => clearInterval(id)
  }, [load])

  // Re-fetch when date range changes
  useEffect(() => {
    setLoading(true)
    load()
  }, [dateRange]) // eslint-disable-line

  const fmtTime = (d) => d ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '—'

  const isFiltered = filters.search || filters.minEvses || filters.maxEvses ||
    filters.minUtil || filters.minKw || filters.maxKw ||
    filters.connectorFilter !== 'all' || filters.operatorFilter !== 'all'
  const filtered = applyFilters(hubs, filters)

  const utilDelta = deltas?.has_prior_data ? {
    text: `${Math.abs(deltas.util_delta_pp).toFixed(1)}pp`,
    positive: deltas.util_delta_pp >= 0,
  } : null

  const sparkValues = sparkline.map(d => d.avg_utilisation_pct)

  function fmtKwh(v) {
    if (v == null) return '—'
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)} GWh`
    if (v >= 1_000) return `${(v / 1_000).toFixed(1)} MWh`
    return `${Math.round(v)} kWh`
  }

  if (loading) return <PageLoader text="Loading live data…" />

  return (
    <>
      <div className="page-meta">
        <h1 className="page-meta-title">Live Status</h1>
        <span className="last-updated">
          {dateRange.start ? 'Averaged over selected period · ' : ''}
          Updated {fmtTime(lastUpdated)}
        </span>
      </div>

      <div className="stat-grid">
        <StatCard icon="🏢" label="Hubs Tracked" value={stats?.total_hubs ?? '—'} sub="" />
        <StatCard icon="🔌" label="Total EVSEs" value={stats?.total_evses ?? '—'} sub="across all hubs" />
        <StatCard
          icon="⚡"
          label="Charging Now"
          value={stats?.total_charging_evses ?? '—'}
          valueClass="green"
          sub="active sessions"
        />
        <StatCard
          icon="📊"
          label="Avg Utilisation"
          value={stats ? `${stats.avg_utilisation_pct}%` : '—'}
          valueClass={
            stats?.avg_utilisation_pct >= 50 ? 'red'
            : stats?.avg_utilisation_pct >= 30 ? 'amber'
            : 'green'
          }
          sub="charging ÷ total EVSEs"
          delta={utilDelta}
          sparkData={sparkValues.length > 0 ? sparkValues : null}
        />
        <StatCard
          icon="⚡"
          label="Est. Energy (7d)"
          value={fmtKwh(stats?.estimated_kwh_7d)}
          sub="±30–50% estimate"
        />
      </div>

      {isFiltered && filtered.length > 0 && <FilteredStatsStrip hubs={filtered} />}

      <div className="section-header" style={{ marginBottom: 14 }}>
        <span className="section-title">
          All Hubs
          <span style={{ color: 'var(--text-dim)', fontWeight: 600, fontSize: 10, marginLeft: 8 }}>
            {filtered.length !== hubs.length ? `${filtered.length} / ${hubs.length}` : hubs.length}
          </span>
        </span>
      </div>

      {hubs.length === 0
        ? <div className="empty">No data yet — run the scraper first.</div>
        : filtered.length === 0
          ? <div className="empty">No hubs match the current filters.</div>
          : <HubTable hubs={filtered} onHubClick={setSelectedHub} />
      }

      {selectedHub && <HubDetailModal hub={selectedHub} onClose={() => setSelectedHub(null)} />}
    </>
  )
}
