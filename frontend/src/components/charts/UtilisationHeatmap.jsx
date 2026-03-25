import { useState } from 'react'

// SQLite strftime('%w'): 0=Sun … 6=Sat
// We display Mon–Sun left to right → remap: Mon=1 → col 0, …, Sun=0 → col 6
const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
const DOW_TO_COL = { 1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 0: 6 }

function fmtHour(h) {
  if (h === 0) return '12am'
  if (h === 12) return '12pm'
  return h < 12 ? `${h}am` : `${h - 12}pm`
}

function cellOpacity(points) {
  if (!points || points === 0) return 1   // no-data empty cells stay solid grey
  if (points < 3) return 0.35
  if (points < 6) return 0.65
  return 1
}

function cellColor(pct) {
  if (!pct || pct === 0) return '#131d2e'
  if (pct >= 80) return '#ef4444'
  if (pct >= 50) return '#f59e0b'
  if (pct >= 20) return '#10b981'
  return '#065f46'
}

const CELL_W = 36
const CELL_H = 18
const GAP = 2
const LABEL_W = 36
const LABEL_H = 20

export default function UtilisationHeatmap({ data = [] }) {
  const [tooltip, setTooltip] = useState(null)

  if (data.length === 0) {
    return (
      <div className="empty" style={{ padding: '20px 0' }}>
        Heatmap builds over 2+ weeks of polling. Check back soon.
      </div>
    )
  }

  // Build lookup: grid[col][hour] = avg_utilisation_pct
  const grid = Array.from({ length: 7 }, () => Array(24).fill(null))
  const points = {}
  for (const d of data) {
    const col = DOW_TO_COL[d.day_of_week]
    if (col !== undefined && d.hour >= 0 && d.hour < 24) {
      grid[col][d.hour] = d.avg_utilisation_pct
      points[`${col}-${d.hour}`] = d
    }
  }

  const totalW = LABEL_W + 7 * (CELL_W + GAP) - GAP
  const totalH = LABEL_H + 24 * (CELL_H + GAP) - GAP

  return (
    <div className="heatmap-wrap" style={{ position: 'relative' }}>
      <svg width={totalW} height={totalH} style={{ display: 'block', overflow: 'visible' }}>
        {/* Column headers */}
        {DAY_LABELS.map((day, col) => (
          <text
            key={day}
            x={LABEL_W + col * (CELL_W + GAP) + CELL_W / 2}
            y={LABEL_H - 4}
            textAnchor="middle"
            fill="#5c7a99"
            fontSize={10}
            fontWeight={700}
            fontFamily="Outfit, sans-serif"
          >
            {day}
          </text>
        ))}

        {/* Row labels + cells */}
        {Array.from({ length: 24 }, (_, hour) => (
          <g key={hour}>
            <text
              x={LABEL_W - 4}
              y={LABEL_H + hour * (CELL_H + GAP) + CELL_H / 2 + 1}
              textAnchor="end"
              dominantBaseline="middle"
              fill="#5c7a99"
              fontSize={9}
              fontFamily="Outfit, sans-serif"
            >
              {fmtHour(hour)}
            </text>
            {Array.from({ length: 7 }, (_, col) => {
              const pct = grid[col][hour]
              const key = `${col}-${hour}`
              const dp = points[key]
              return (
                <rect
                  key={key}
                  className="heatmap-cell"
                  x={LABEL_W + col * (CELL_W + GAP)}
                  y={LABEL_H + hour * (CELL_H + GAP)}
                  width={CELL_W}
                  height={CELL_H}
                  rx={2}
                  fill={cellColor(pct)}
                  opacity={cellOpacity(dp?.data_points)}
                  onMouseEnter={e => {
                    setTooltip({
                      x: e.clientX,
                      y: e.clientY,
                      day: DAY_LABELS[col],
                      hour,
                      pct: pct ?? 0,
                      points: dp?.data_points ?? 0,
                    })
                  }}
                  onMouseLeave={() => setTooltip(null)}
                />
              )
            })}
          </g>
        ))}
      </svg>

      {/* Hover tooltip */}
      {tooltip && (
        <div style={{
          position: 'fixed',
          left: tooltip.x + 12,
          top: tooltip.y - 10,
          background: '#0d1220',
          border: '1px solid #1c2840',
          borderRadius: 7,
          padding: '8px 12px',
          fontSize: 12,
          pointerEvents: 'none',
          zIndex: 999,
          fontFamily: 'Outfit, sans-serif',
        }}>
          <div style={{ color: '#5c7a99', marginBottom: 2 }}>{tooltip.day} {fmtHour(tooltip.hour)}</div>
          <div style={{ color: cellColor(tooltip.pct), fontWeight: 700 }}>{tooltip.pct}% avg utilisation</div>
          <div style={{ color: '#5c7a99', fontSize: 11 }}>{tooltip.points} data points</div>
        </div>
      )}

      {/* Colour legend */}
      <div style={{ display: 'flex', gap: 12, marginTop: 12, fontSize: 11, color: '#5c7a99', alignItems: 'center', fontFamily: 'Outfit, sans-serif' }}>
        {[['#131d2e', 'No data'], ['#065f46', '< 20%'], ['#10b981', '20–49%'], ['#f59e0b', '50–79%'], ['#ef4444', '≥ 80%']].map(([color, label]) => (
          <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 10, height: 10, background: color, borderRadius: 2, display: 'inline-block' }} />
            {label}
          </span>
        ))}
        <span style={{ marginLeft: 8, fontStyle: 'italic' }}>Faded = fewer than 6 readings</span>
      </div>
    </div>
  )
}
