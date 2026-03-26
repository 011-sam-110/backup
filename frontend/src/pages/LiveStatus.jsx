import { useState, useEffect, useCallback } from 'react'
import StatCard from '../components/StatCard'

const IconBuilding = () => (
  <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <rect x="2" y="2" width="14" height="14" rx="1"/>
    <path d="M6 18V10h6v8"/>
    <path d="M6 6h1M11 6h1M6 9h1M11 9h1"/>
  </svg>
)
const IconPlug = () => (
  <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M9 14v3M6 3v4M12 3v4"/>
    <rect x="4" y="7" width="10" height="5" rx="2"/>
    <path d="M9 12v2"/>
  </svg>
)
const IconZap = () => (
  <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10.5 2L4 10h6l-2.5 6L15 8H9l1.5-6z"/>
  </svg>
)
const IconBarChart = () => (
  <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 14V8M7 14V5M11 14V9M15 14V3"/>
  </svg>
)
import HubTable from '../components/HubTable'
import PageLoader from '../components/PageLoader'
import HubDetailModal from '../components/HubDetailModal'
import MethodologyPanel from '../components/MethodologyPanel'
import { useFilters, applyFilters } from '../context/FilterContext'
import { authFetch } from '../context/AuthContext'
import { fmtKw, hubEstKw } from '../utils/status'

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
  const estKw         = hubs.reduce((s, h) => s + hubEstKw(h), 0)

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 20,
      background: 'var(--accent-dim)', border: '1px solid rgba(0,86,179,0.2)',
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
      <Stat label="est. load" value={fmtKw(estKw)} valueClass="amber" />
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
  const { hubsUrl, visitsUrl, setAvailableOperators, dateRange } = filters

  const load = useCallback(async () => {
    try {
      const [statsRes, hubsRes, deltasRes, sparkRes, visitsRes] = await Promise.all([
        authFetch('/api/stats'),
        authFetch(hubsUrl()),
        authFetch('/api/stats/deltas'),
        authFetch('/api/sparkline?days=7'),
        authFetch(visitsUrl()),
      ])
      const hubData = await hubsRes.json()
      const visitsData = await visitsRes.json()
      const visitMap = new Map(visitsData.map(v => [v.hub_uuid, v]))

      setStats(await statsRes.json())
      setHubs(hubData.map(h => ({
        ...h,
        visit_count: visitMap.get(h.uuid)?.visit_count ?? 0,
        avg_dwell_min: visitMap.get(h.uuid)?.avg_dwell_min ?? null,
        active_visits: visitMap.get(h.uuid)?.active_visits ?? 0,
      })))
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
  }, [hubsUrl, visitsUrl, setAvailableOperators]) // eslint-disable-line

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
    filters.connectorFilter !== 'all' || filters.operatorFilter.size > 0
  const filtered = applyFilters(hubs, filters)

  const utilDelta = deltas?.has_prior_data ? {
    text: `${Math.abs(deltas.util_delta_pp).toFixed(1)}pp`,
    positive: deltas.util_delta_pp >= 0,
  } : null

  const sparkValues = sparkline.map(d => d.avg_utilisation_pct)
  const totalEstKw   = hubs.reduce((s, h) => s + hubEstKw(h), 0)

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
        <StatCard icon={<IconBuilding />} label="Hubs Tracked" value={stats?.total_hubs ?? '—'} sub="" />
        <StatCard icon={<IconPlug />} label="Total EVSEs" value={stats?.total_evses ?? '—'} sub="across all hubs" />
        <StatCard
          icon={<IconZap />}
          label="Charging Now"
          value={stats?.total_charging_evses ?? '—'}
          valueClass="green"
          sub="active sessions"
        />
        <StatCard
          icon={<IconBarChart />}
          label="Avg Utilisation"
          value={stats ? `${stats.avg_utilisation_pct}%` : '—'}
          valueClass={
            stats?.avg_utilisation_pct >= 50 ? 'red'
            : stats?.avg_utilisation_pct >= 30 ? 'amber'
            : 'green'
          }
          sub="charging ÷ total EVSEs"
          delta={utilDelta}
        />
        <StatCard
          icon={<IconZap />}
          label="Est. Load"
          value={fmtKw(totalEstKw)}
          sub="active draw estimate"
        />
      </div>

      <MethodologyPanel />

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
