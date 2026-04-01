import { useState } from 'react'

const COLS = [
  { key: 'hub_name',            label: 'Hub',                  fmt: v => v || '—',           align: 'left'  },
  { key: 'operator',            label: 'Operator',             fmt: v => v || '—',           align: 'left'  },
  { key: 'active_pct',          label: 'Active %',             fmt: v => v != null ? `${v}%` : '—',  align: 'right' },
  { key: 'full_capacity_pct',   label: 'Full cap. %',          fmt: v => v != null ? `${v}%` : '—',  align: 'right' },
  { key: 'full_capacity_hours', label: 'Full cap. hrs',        fmt: v => v != null ? `${v}h` : '—',  align: 'right' },
  { key: 'visits_per_day',      label: 'Visits/day',           fmt: v => v != null ? v.toFixed(1) : '—', align: 'right' },
  { key: 'avg_dwell_min',       label: 'Avg dwell',            fmt: v => v != null ? `${v}m` : '—',  align: 'right' },
  { key: 'total_snapshots',     label: 'Snapshots',            fmt: v => v ?? '—',            align: 'right' },
]

function activePctColor(pct) {
  if (pct == null) return 'var(--text-muted)'
  if (pct >= 50)   return '#22c55e'
  if (pct >= 25)   return '#f59e0b'
  return '#ef4444'
}

export default function HubPerformanceTable({ data, onHubClick }) {
  const [sortKey, setSortKey] = useState('active_pct')
  const [sortDir, setSortDir] = useState(-1) // -1 = desc, 1 = asc

  function handleSort(key) {
    if (key === sortKey) {
      setSortDir(d => -d)
    } else {
      setSortKey(key)
      setSortDir(-1)
    }
  }

  const sorted = [...data].sort((a, b) => {
    const av = a[sortKey] ?? -Infinity
    const bv = b[sortKey] ?? -Infinity
    if (typeof av === 'string') return sortDir * av.localeCompare(bv)
    return sortDir * (bv - av)
  })

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr>
            {COLS.map(col => (
              <th
                key={col.key}
                onClick={() => handleSort(col.key)}
                style={{
                  textAlign: col.align,
                  padding: '6px 10px',
                  borderBottom: '1px solid var(--border)',
                  color: sortKey === col.key ? 'var(--accent)' : 'var(--text-muted)',
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  userSelect: 'none',
                  fontWeight: 500,
                }}
              >
                {col.label}
                {sortKey === col.key && (
                  <span style={{ marginLeft: 4, opacity: 0.7 }}>{sortDir === -1 ? '↓' : '↑'}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map(row => (
            <tr
              key={row.uuid}
              onClick={() => onHubClick?.(row)}
              style={{ cursor: onHubClick ? 'pointer' : 'default' }}
              onMouseEnter={e => e.currentTarget.style.background = 'var(--surface-hover, rgba(255,255,255,0.04))'}
              onMouseLeave={e => e.currentTarget.style.background = ''}
            >
              {COLS.map(col => (
                <td
                  key={col.key}
                  style={{
                    textAlign: col.align,
                    padding: '6px 10px',
                    borderBottom: '1px solid var(--border)',
                    color: col.key === 'active_pct'
                      ? activePctColor(row[col.key])
                      : col.key === 'hub_name'
                        ? 'var(--text)'
                        : 'var(--text-secondary, var(--text-muted))',
                    whiteSpace: col.key === 'hub_name' ? 'normal' : 'nowrap',
                  }}
                >
                  {col.fmt(row[col.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
