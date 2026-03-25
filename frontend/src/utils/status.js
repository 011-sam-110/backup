export function utilTier(pct) {
  if (pct >= 50) return 'critical'
  if (pct >= 30) return 'busy'
  if (pct >= 25) return 'moderate'
  if (pct >= 15) return 'low'
  return 'normal'
}

export function utilColor(pct) {
  if (pct >= 50) return '#1D4ED8'  // dark blue  — excellent
  if (pct >= 30) return '#2563EB'  // blue        — busy
  if (pct >= 25) return '#60A5FA'  // light blue  — moderate
  if (pct >= 15) return '#93C5FD'  // pale blue   — low
  return '#CBD5E1'                  // slate-300   — idle
}

export function utilClass(pct) {
  if (pct >= 50) return 'red'
  if (pct >= 30) return 'orange'
  if (pct >= 25) return 'amber'
  if (pct >= 15) return 'lime'
  return 'green'
}

export function utilIcon(pct) {
  if (pct >= 50) return { icon: '⚠', label: 'Critical', ariaLabel: 'Critical utilisation' }
  if (pct >= 30) return { icon: '▲', label: 'High',     ariaLabel: 'High utilisation' }
  if (pct >= 25) return { icon: '▲', label: 'Moderate', ariaLabel: 'Moderate utilisation' }
  if (pct >= 15) return { icon: '●', label: 'Low',      ariaLabel: 'Low utilisation' }
  return                 { icon: '✓', label: 'Normal',   ariaLabel: 'Normal utilisation' }
}

/** Format a kW value as kW or MW */
export function fmtKw(kw) {
  if (kw == null || isNaN(kw)) return '—'
  if (kw >= 1_000_000) return `${(kw / 1_000_000).toFixed(1)} GW`
  if (kw >= 1_000) return `${(kw / 1_000).toFixed(1)} MW`
  return `${Math.round(kw)} kW`
}

/** Estimated load for a single hub: charging_count × max_power_kw × 0.7 */
export function hubEstKw(hub) {
  return (hub.charging_count ?? 0) * (hub.max_power_kw ?? 0) * 0.7
}

/** Bands used in the map legend */
export const MAP_BANDS = [
  { color: '#22c55e', label: '0–14%',  min: 0,  max: 15 },
  { color: '#84cc16', label: '15–24%', min: 15, max: 25 },
  { color: '#f59e0b', label: '25–29%', min: 25, max: 30 },
  { color: '#f97316', label: '30–49%', min: 30, max: 50 },
  { color: '#ef4444', label: '50%+',   min: 50, max: Infinity },
]
