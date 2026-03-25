export function utilTier(pct) {
  if (pct >= 50) return 'critical'
  if (pct >= 30) return 'busy'
  if (pct >= 25) return 'moderate'
  if (pct >= 15) return 'low'
  return 'normal'
}

export function utilColor(pct) {
  if (pct >= 50) return '#ef4444'  // red
  if (pct >= 30) return '#f97316'  // orange
  if (pct >= 25) return '#f59e0b'  // amber
  if (pct >= 15) return '#84cc16'  // lime
  return '#22c55e'                  // green
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

/** Bands used in the map legend */
export const MAP_BANDS = [
  { color: '#22c55e', label: '0–14%',  min: 0,  max: 15 },
  { color: '#84cc16', label: '15–24%', min: 15, max: 25 },
  { color: '#f59e0b', label: '25–29%', min: 25, max: 30 },
  { color: '#f97316', label: '30–49%', min: 30, max: 50 },
  { color: '#ef4444', label: '50%+',   min: 50, max: Infinity },
]
