import { useState, useEffect, useRef } from 'react'
import * as XLSX from 'xlsx'
import UtilisationLine from '../components/charts/UtilisationLine'
import HubBarChart from '../components/charts/HubBarChart'
import HourlyPattern from '../components/charts/HourlyPattern'
import ReliabilityChart from '../components/charts/ReliabilityChart'
import HubDetailModal from '../components/HubDetailModal'
import PageLoader from '../components/PageLoader'
import CustomRangePanel from '../components/charts/CustomRangePanel'
import { useFilters, applyFilters } from '../context/FilterContext'

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
          minEvses, maxEvses } = filters

  const enc = d => encodeURIComponent(d.toISOString())

  const abortRef = useRef(null)

  async function load() {
    if (abortRef.current) abortRef.current.abort()
    const controller = new AbortController()
    abortRef.current = controller
    setLoading(true)
    try {
      const ap = analyticsParams()
      const [histRes, hubsRes, hourlyRes, relRes, statsRes, deltasRes] = await Promise.all([
        fetch(`/api/history?hours=168${ap}`,        { signal: controller.signal }),
        fetch(hubsUrl(),                             { signal: controller.signal }),
        fetch(`/api/hourly-pattern?hours=168${ap}`, { signal: controller.signal }),
        fetch(`/api/reliability?hours=168${ap}`,    { signal: controller.signal }),
        fetch('/api/stats',                          { signal: controller.signal }),
        fetch('/api/stats/deltas',                   { signal: controller.signal }),
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
  }

  useEffect(() => {
    load()
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses]) // eslint-disable-line

  if (loading) return <PageLoader text="Loading charts…" />

  const filteredHubs = applyFilters(hubs, filters)

  const noData = history.length === 0 && hubs.length === 0

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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <div className="chart-title" style={{ marginBottom: 0 }}>
                {CHARTS.find(c => c.key === activeChart)?.label}
              </div>
              {activeChart === 'trend' && history.length >= 2 && (
                <button className="btn btn-outline" style={{ fontSize: 12, padding: '4px 12px' }} onClick={() => exportTrendData(history)}>
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
                  title="Custom Range"
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
                  title="Custom Range"
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
                    return <>Avg utilisation <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · {totalCharging.toLocaleString()} charging sessions</>
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
                  title="Custom Range"
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
                  title="Custom Range"
                  buildUrl={(range, sh, eh, fp) => {
                    const dt = range.start && range.end
                      ? `&start_dt=${enc(range.start)}&end_dt=${enc(range.end)}`
                      : '&hours=720'
                    return `/api/hourly-pattern?hours=8760${dt}&start_hour=${sh}&end_hour=${eh}${fp}`
                  }}
                  renderStat={(data) => {
                    if (!data || !data.length) return null
                    const peak = data.reduce((best, d) => (d.avg_utilisation_pct ?? 0) > (best.avg_utilisation_pct ?? 0) ? d : best, data[0])
                    const avg = data.reduce((s, d) => s + (d.avg_utilisation_pct ?? 0), 0) / data.length
                    const peakLabel = peak.hour === 0 ? '12am' : peak.hour === 12 ? '12pm' : peak.hour < 12 ? `${peak.hour}am` : `${peak.hour - 12}pm`
                    return <>Avg utilisation <strong style={{ color: 'var(--accent)' }}>{avg.toFixed(1)}%</strong> · Peak at <strong style={{ color: 'var(--accent)' }}>{peakLabel}</strong> ({peak.avg_utilisation_pct?.toFixed(1)}%)</>
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
