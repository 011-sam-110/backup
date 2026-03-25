const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

function fmtHour(h) {
  if (h === 0) return '12am'
  if (h === 12) return '12pm'
  return h < 12 ? `${h}am` : `${h - 12}pm`
}

export default function InsightCallout({ stats, deltas, heatmapData = [], hubs = [] }) {
  if (!stats) return null

  const sentences = []

  // Peak utilisation from heatmap
  if (heatmapData.length > 0) {
    const peak = heatmapData.reduce((max, d) =>
      d.avg_utilisation_pct > max.avg_utilisation_pct ? d : max,
      { avg_utilisation_pct: 0 }
    )
    if (peak.avg_utilisation_pct > 0) {
      sentences.push(
        <>Peak utilisation reached <strong>{peak.avg_utilisation_pct}%</strong> on <strong>{DAYS[peak.day_of_week]}s at {fmtHour(peak.hour)}</strong>.</>
      )
    }
  }

  // Hubs above threshold
  const hubsAbove70 = hubs.filter(h => (h.utilisation_pct ?? 0) >= 70).length
  if (hubsAbove70 > 0) {
    sentences.push(
      <><strong>{hubsAbove70} hub{hubsAbove70 !== 1 ? 's' : ''}</strong> {hubsAbove70 !== 1 ? 'are' : 'is'} currently above 70% — the threshold associated with queue formation.</>
    )
  }

  // Week-on-week delta
  if (deltas?.has_prior_data) {
    const delta = deltas.util_delta_pp
    const dir = delta >= 0 ? 'up' : 'down'
    const abs = Math.abs(delta).toFixed(1)
    sentences.push(
      <>Average utilisation is <strong style={{ color: delta >= 0 ? 'var(--green)' : 'var(--red)' }}>{dir} {abs}pp</strong> vs last week.</>
    )
  } else if (stats.avg_utilisation_pct > 0) {
    sentences.push(
      <>Current network-wide average: <strong>{stats.avg_utilisation_pct}%</strong> utilisation.</>
    )
  }

  if (sentences.length === 0) return null

  return (
    <div className="insight-callout">
      <span className="insight-label">📈 Insight</span>
      {sentences.map((s, i) => (
        <span key={i}>{s}{i < sentences.length - 1 ? ' ' : ''}</span>
      ))}
    </div>
  )
}
