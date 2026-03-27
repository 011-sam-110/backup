import { useState, useEffect, useRef, useCallback } from 'react'
import * as XLSX from 'xlsx'
import UtilisationLine from '../components/charts/UtilisationLine'
import HubBarChart from '../components/charts/HubBarChart'
import HourlyPattern from '../components/charts/HourlyPattern'
import ReliabilityChart from '../components/charts/ReliabilityChart'
import HubDetailModal from '../components/HubDetailModal'
import PageLoader from '../components/PageLoader'
import CustomRangePanel from '../components/charts/CustomRangePanel'
import { useFilters, applyFilters } from '../context/FilterContext'
import { authFetch } from '../context/AuthContext'
import { hubEstKw, fmtKw, fmtKwh, fmtHour } from '../utils/status'

const REFRESH_MS = 60_000

const CHARTS = [
  { key: 'hubs',        label: 'Hub Utilisation',    icon: '▦' },
  { key: 'trend',       label: 'Trend',               icon: '↗' },
  { key: 'reliability', label: 'Network Composition', icon: '◑' },
  { key: 'hourly',      label: 'Day Pattern',         icon: '◷' },
]

function exportTrendData(data) {
  const rows = data.map(d => ({
    'Time': d.scraped_at,
    'Avg Utilisation %': d.avg_utilisation_pct,
    'Total Charging': d.total_charging,
    'Hub Count': d.hub_count,
  }))
  const ws = XLSX.utils.json_to_sheet(rows)
  const wb = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(wb, ws, 'Trend')
  XLSX.writeFile(wb, `utilisation_trend_${new Date().toISOString().slice(0, 10)}.xlsx`)
}

export default function Graphs() {
  const [history, setHistory] = useState([])
  const [hubs, setHubs] = useState([])
  const [hourly, setHourly] = useState([])
  const [reliabilityData, setReliabilityData] = useState([])
  const [stats, setStats] = useState(null)
  const [deltas, setDeltas] = useState(null)
  const [loading, setLoading] = useState(true)
  const [selectedHub, setSelectedHub] = useState(null)
  const [activeChart, setActiveChart] = useState('hubs')

  const filters = useFilters()
  const { analyticsParams, hubsUrl, setAvailableOperators,
          dateRange, operatorFilter, connectorFilter, minKw, maxKw,
          minEvses, maxEvses, activeGroupIds } = filters

  const enc = d => encodeURIComponent(d.toISOString())

  const abortRef = useRef(null)

  const load = useCallback(async (showSpinner = false) => {
    if (abortRef.current) abortRef.current.abort()
    const controller = new AbortController()
    abortRef.current = controller
    if (showSpinner) setLoading(true)
    try {
      const ap = analyticsParams()
      const [histRes, hubsRes, hourlyRes, relRes, statsRes, deltasRes] = await Promise.all([
        authFetch(`/api/history?hours=168${ap}`,        { signal: controller.signal }),
        authFetch(hubsUrl(),                             { signal: controller.signal }),
        authFetch(`/api/hourly-pattern?hours=168${ap}`, { signal: controller.signal }),
        authFetch(`/api/reliability?hours=168${ap}`,    { signal: controller.signal }),
        authFetch('/api/stats',                          { signal: controller.signal }),
        authFetch('/api/stats/deltas',                   { signal: controller.signal }),
      ])
      const hubData = await hubsRes.json()
      setHistory(await histRes.json())
      setHubs(hubData)
      setHourly(await hourlyRes.json())
      setReliabilityData(await relRes.json())
      setStats(await statsRes.json())
      setDeltas(await deltasRes.json())
      const ops = [...new Set(hubData.map(h => h.operator).filter(Boolean))].sort()
      setAvailableOperators(ops)
    } catch (err) {
      if (err.name !== 'AbortError') { /* ignore */ }
    } finally {
      if (!controller.signal.aborted) setLoading(false)
    }
  }, [hubsUrl, analyticsParams, setAvailableOperators]) // eslint-disable-line

  useEffect(() => {
    load(true)
    const id = setInterval(() => load(false), REFRESH_MS)
    return () => clearInterval(id)
  }, [load])

  // Re-fetch when filters change
  useEffect(() => {
    load(false)
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds]) // eslint-disable-line

  if (loading) return <PageLoader text="Loading charts…" />

  const filteredHubs = applyFilters(hubs, filters)

  const noData = history.length === 0 && hubs.length === 0

  // Primary chart stat lines
  const hubStat = filteredHubs.length > 0 ? (() => {
    const avg = filteredHubs.reduce((s, h) => s + (h.utilisation_pct ?? 0), 0) / filteredHubs.length
    const kw = filteredHubs.reduce((s, h) => s + hubEstKw(h), 0)
    return <>Avg <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · Est. <strong style={{ color: '#f59e0b' }}>{fmtKw(kw)}</strong> across {filteredHubs.length} hubs</>
  })() : null

  const trendStat = history.length >= 2 ? (() => {
    const avg = history.reduce((s, d) => s + (d.avg_utilisation_pct ?? 0), 0) / history.length
    const totalKwh = history.reduce((s, d) => s + (d.total_estimated_kwh ?? 0), 0)
    return <>Avg <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong>{fmtKwh(totalKwh) ? <> · Est. <strong style={{ color: '#f59e0b' }}>{fmtKwh(totalKwh)}</strong> over period</> : null}</>
  })() : null

  const reliabilityStat = reliabilityData.length >= 2 ? (() => {
    const avgC = reliabilityData.reduce((s, d) => s + (d.charging_pct ?? 0), 0) / reliabilityData.length
    const avgI = reliabilityData.reduce((s, d) => s + (d.inoperative_pct ?? 0) + (d.oos_pct ?? 0), 0) / reliabilityData.length
    return <>Avg charging <strong style={{ color: 'var(--accent)' }}>{avgC.toFixed(1)}%</strong> · Avg inoperative <strong style={{ color: '#f59e0b' }}>{avgI.toFixed(1)}%</strong></>
  })() : null

  const hourlyStat = hourly.length > 0 ? (() => {
    const avg = hourly.reduce((s, d) => s + (d.avg_utilisation_pct ?? 0), 0) / hourly.length
    const peak = hourly.reduce((b, d) => (d.avg_utilisation_pct ?? 0) > (b.avg_utilisation_pct ?? 0) ? d : b, hourly[0])
    return <>Avg <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · Peak at <strong style={{ color: 'var(--accent)' }}>{fmtHour(peak.hour)}</strong> ({peak.avg_utilisation_pct?.toFixed(1)}%)</>
  })() : null

  const statStyle = { fontSize: 12, color: 'var(--text-muted)' }

  return (
    <>
      <h1 className="page-meta-title" style={{ marginBottom: 28 }}>Analytics</h1>

      {noData ? (
        <div className="empty">No data yet. Run the scraper at least once, then come back here.</div>
      ) : (
        <>
          <div style={{ display: 'flex', gap: 6, marginBottom: 24, flexWrap: 'wrap' }}>
            {CHARTS.map(c => (
              <button
                key={c.key}
                className={`trend-tab${activeChart === c.key ? ' active' : ''}`}
                onClick={() => setActiveChart(c.key)}
              >
                <span style={{ marginRight: 5, opacity: 0.7, fontSize: 11 }}>{c.icon}</span>
                {c.label}
              </button>
            ))}
          </div>

          <div className="chart-section">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16, gap: 12 }}>
              <div className="chart-title" style={{ marginBottom: 0 }}>
                {CHARTS.find(c => c.key === activeChart)?.label}
              </div>
              {activeChart === 'hubs'        && hubStat        && <div style={statStyle}>{hubStat}</div>}
              {activeChart === 'trend'       && trendStat      && <div style={statStyle}>{trendStat}</div>}
              {activeChart === 'reliability' && reliabilityStat && <div style={statStyle}>{reliabilityStat}</div>}
              {activeChart === 'hourly'      && hourlyStat     && <div style={statStyle}>{hourlyStat}</div>}
              {activeChart === 'trend' && history.length >= 2 && (
                <button className="btn btn-outline" style={{ fontSize: 12, padding: '4px 12px', flexShrink: 0 }} onClick={() => exportTrendData(history)}>
                  ↓ Export
                </button>
              )}
            </div>

            {activeChart === 'hubs' && (
              <>
                {filteredHubs.length === 0
                  ? <div className="empty">No hub data.</div>
                  : <HubBarChart hubs={filteredHubs} onHubClick={setSelectedHub} label="Snapshot utilisation figure" />}
                <CustomRangePanel
                  title="Secondary Chart"
                  buildUrl={(range, sh, eh) =>
                    range.start && range.end
                      ? `/api/hubs?start_dt=${enc(range.start)}&end_dt=${enc(range.end)}&start_hour=${sh}&end_hour=${eh}`
                      : null
                  }
                  renderStat={(data) => {
                    const d = applyFilters(data || [], filters)
                    if (!d.length) return null
                    const avg = d.reduce((s, h) => s + (h.utilisation_pct ?? 0), 0) / d.length
                    return <>Avg utilisation <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> across {d.length} hubs</>
                  }}
                  renderChart={(data) => {
                    const filtered = applyFilters(data || [], filters)
                    return filtered.length === 0
                      ? <div className="empty">No hubs match in this range.</div>
                      : <HubBarChart hubs={filtered} onHubClick={setSelectedHub} label="Utilisation over a certain period" />
                  }}
                />
              </>
            )}

            {activeChart === 'trend' && (
              <>
                {history.length < 2
                  ? <div className="empty">Need 2+ scrape runs to show a trend.</div>
                  : <UtilisationLine data={history} />}
                <CustomRangePanel
                  title="SECONDARY CUSTOM CHART"
                  buildUrl={(range, sh, eh, fp) => {
                    const dt = range.start && range.end
                      ? `&start_dt=${enc(range.start)}&end_dt=${enc(range.end)}`
                      : '&hours=720'
                    return `/api/history?hours=8760${dt}&start_hour=${sh}&end_hour=${eh}${fp}`
                  }}
                  renderStat={(data) => {
                    if (!data || !data.length) return null
                    const avg = data.reduce((s, d) => s + (d.avg_utilisation_pct ?? 0), 0) / data.length
                    const totalCharging = data.reduce((s, d) => s + (d.total_charging ?? 0), 0)
                    const totalKwh = data.reduce((s, d) => s + (d.total_estimated_kwh ?? 0), 0)
                    return <>Avg <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · {totalCharging.toLocaleString()} sessions{fmtKwh(totalKwh) ? <> · Est. <strong style={{ color: '#f59e0b' }}>{fmtKwh(totalKwh)}</strong></> : null}</>
                  }}
                  renderChart={(data) =>
                    !data || data.length < 2
                      ? <div className="empty">Not enough data in this range.</div>
                      : <UtilisationLine data={data} />
                  }
                />
              </>
            )}

            {activeChart === 'reliability' && (
              <>
                <ReliabilityChart data={reliabilityData} />
                <CustomRangePanel
                  title="SECONDARY CUSTOM CHART"
                  buildUrl={(range, sh, eh, fp) => {
                    const dt = range.start && range.end
                      ? `&start_dt=${enc(range.start)}&end_dt=${enc(range.end)}`
                      : '&hours=720'
                    return `/api/reliability?hours=8760${dt}&start_hour=${sh}&end_hour=${eh}${fp}`
                  }}
                  renderStat={(data) => {
                    if (!data || !data.length) return null
                    const avgCharging = data.reduce((s, d) => s + (d.charging_pct ?? 0), 0) / data.length
                    const avgInop = data.reduce((s, d) => s + (d.inoperative_pct ?? 0) + (d.oos_pct ?? 0), 0) / data.length
                    return <>Avg charging <strong style={{ color: 'var(--accent)' }}>{avgCharging.toFixed(1)}%</strong> · Avg inoperative <strong style={{ color: 'var(--amber)' }}>{avgInop.toFixed(1)}%</strong></>
                  }}
                  renderChart={(data) => <ReliabilityChart data={data || []} />}
                />
              </>
            )}

            {activeChart === 'hourly' && (
              <>
                <HourlyPattern data={hourly} />
                <CustomRangePanel
                  title="SECONDARY CUSTOM CHART"
                  buildUrl={(range, sh, eh, fp) => {
                    const dt = range.start && range.end
                      ? `&start_dt=${enc(range.start)}&end_dt=${enc(range.end)}`
                      : '&hours=720'
                    return `/api/hourly-pattern?hours=8760${dt}&start_hour=${sh}&end_hour=${eh}${fp}`
                  }}
                  renderStat={(data) => {
                    if (!data || !data.length) return null
                    const avg = data.reduce((s, d) => s + (d.avg_utilisation_pct ?? 0), 0) / data.length
                    const peak = data.reduce((b, d) => (d.avg_utilisation_pct ?? 0) > (b.avg_utilisation_pct ?? 0) ? d : b, data[0])
                    const peakKw = data.reduce((b, d) => (d.avg_est_kw ?? 0) > (b.avg_est_kw ?? 0) ? d : b, data[0])
                    return <>Avg <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · Peak at <strong style={{ color: 'var(--accent)' }}>{fmtHour(peak.hour)}</strong> ({peak.avg_utilisation_pct?.toFixed(1)}%){peakKw.avg_est_kw > 0 ? <> · Peak kW at <strong style={{ color: '#f59e0b' }}>{fmtHour(peakKw.hour)}</strong></> : null}</>
                  }}
                  renderChart={(data) => <HourlyPattern data={data || []} />}
                />
              </>
            )}
          </div>

          {selectedHub && (
            <HubDetailModal hub={selectedHub} onClose={() => setSelectedHub(null)} />
          )}
        </>
      )}
    </>
  )
}
