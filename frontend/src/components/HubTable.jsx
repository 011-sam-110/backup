import { useState, useEffect } from 'react'
import { utilColor, utilTier, utilIcon, fmtKw, hubEstKw } from '../utils/status'

const PAGE_SIZE = 25

function fmtAgo(iso, now) {
  if (!iso) return '—'
  const secs = Math.floor((now - new Date(iso)) / 1000)
  if (secs < 10) return 'just now'
  if (secs < 60) return `${secs}s ago`
  const mins = Math.floor(secs / 60)
  if (mins < 60) return `${mins}m ago`
  return `${Math.floor(mins / 60)}h ago`
}

function freshnessColor(iso, now) {
  if (!iso) return 'var(--text-muted)'
  const secs = Math.floor((now - new Date(iso)) / 1000)
  if (secs < 120) return 'var(--green)'
  if (secs < 600) return 'var(--amber, #f59e0b)'
  return 'var(--text-muted)'
}

export default function HubTable({ hubs, onHubClick }) {
  const [sortKey, setSortKey] = useState('utilisation_pct')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)
  const [now, setNow] = useState(Date.now())

  useEffect(() => { setPage(1) }, [hubs])
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 10_000)
    return () => clearInterval(id)
  }, [])

  function toggleSort(key) {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
      setPage(1)
    }
  }

  function getVal(hub, key) {
    if (key === 'est_kw') return hubEstKw(hub)
    return hub[key] ?? -1
  }

  const sorted = [...hubs].sort((a, b) => {
    const av = getVal(a, sortKey)
    const bv = getVal(b, sortKey)
    return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1)
  })

  const paginated = sorted.slice(0, page * PAGE_SIZE)
  const hasMore = paginated.length < sorted.length

  const NUMERIC_KEYS = new Set(['max_power_kw','total_evses','charging_count','est_kw','available_count','inoperative_count','latitude','longitude','user_rating','utilisation_pct','visit_count','avg_dwell_min','util_24h'])

  function th(label, key, extraStyle = {}) {
    const active = sortKey === key
    const arrow = active ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''
    const ariaSort = active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
    const align = NUMERIC_KEYS.has(key) ? 'right' : 'left'
    return (
      <th
        onClick={() => toggleSort(key)}
        style={{ textAlign: align, ...(active ? { color: 'var(--text)' } : {}), ...extraStyle }}
        aria-sort={ariaSort}
      >
        {label}{arrow}
      </th>
    )
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {th('Hub UUID', 'uuid')}
            {th('Operator', 'operator')}
            {th('Rating', 'user_rating')}
            {th('Lat', 'latitude')}
            {th('Lng', 'longitude')}
            {th('Max kW', 'max_power_kw')}
            {th('EVSEs', 'total_evses')}
            {th('Charging', 'charging_count')}
            {th('Est. Load', 'est_kw')}
            {th('Available', 'available_count')}
            {th('Inop', 'inoperative_count')}
            {th('Utilisation', 'utilisation_pct')}
            {th('Visits', 'visit_count')}
            {th('Avg Dwell', 'avg_dwell_min')}
            {th('24h Avg Util', 'util_24h')}
            {th('Last Seen', 'scraped_at')}
          </tr>
        </thead>
        <tbody>
          {paginated.map(hub => {
            const pct = hub.utilisation_pct ?? 0
            const tier = utilTier(pct)
            const { icon, label } = utilIcon(pct)
            return (
              <tr
                key={hub.uuid}
                onClick={() => onHubClick?.(hub)}
                style={{ cursor: onHubClick ? 'pointer' : undefined }}
              >
                <td>
                  <div style={{ fontWeight: 600, fontSize: 13 }}>{hub.hub_name || hub.uuid}</div>
                  {hub.hub_name && (
                    <div className="mono" style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 1 }}>{hub.uuid}</div>
                  )}
                </td>
                <td style={{ fontSize: 12 }}>{hub.operator || <span style={{ color: 'var(--text-muted)' }}>—</span>}</td>
                <td style={{ fontSize: 12 }}>
                  {hub.user_rating != null
                    ? <>{`★ ${hub.user_rating.toFixed(1)}`}<span style={{ color: 'var(--text-muted)', fontSize: 10 }}> ({hub.user_rating_count})</span></>
                    : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                </td>
                <td>
                  <a
                    href={`https://www.google.com/maps?q=${hub.latitude},${hub.longitude}`}
                    target="_blank" rel="noopener noreferrer"
                    className="maps-link"
                    onClick={e => e.stopPropagation()}
                  >
                    {hub.latitude?.toFixed(4)}
                  </a>
                </td>
                <td>
                  <a
                    href={`https://www.google.com/maps?q=${hub.latitude},${hub.longitude}`}
                    target="_blank" rel="noopener noreferrer"
                    className="maps-link"
                    onClick={e => e.stopPropagation()}
                  >
                    {hub.longitude?.toFixed(4)}
                  </a>
                </td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{hub.max_power_kw}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{hub.total_evses}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: 'var(--green)' }}>{hub.charging_count ?? '—'}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: 'var(--amber, #f59e0b)', fontSize: 12 }}>
                  {hub.charging_count > 0 ? fmtKw(hubEstKw(hub)) : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                </td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{hub.available_count ?? '—'}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: hub.inoperative_count > 0 ? 'var(--amber)' : undefined }}>{hub.inoperative_count ?? '—'}</td>
                <td style={{ textAlign: 'right' }}>
                  <div className="util-cell" style={{ justifyContent: 'flex-end' }}>
                    <div className="util-bar-bg">
                      <div
                        className="util-bar"
                        style={{ width: `${Math.min(pct, 100)}%`, background: utilColor(pct) }}
                      />
                    </div>
                    <span
                      className={`util-badge badge-${tier}`}
                      aria-label={`${pct}% - ${label}`}
                    >
                      <span aria-hidden="true">{icon}</span> {pct}%
                    </span>
                  </div>
                </td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                  {hub.visit_count > 0 ? hub.visit_count : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                </td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: 'var(--text-muted)' }}>
                  {hub.avg_dwell_min != null ? `${hub.avg_dwell_min}m` : <span>—</span>}
                </td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: utilColor(hub.util_24h ?? 0) }}>
                  {hub.util_24h != null ? `${hub.util_24h}%` : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                </td>
                <td style={{ color: freshnessColor(hub.scraped_at, now), fontVariantNumeric: 'tabular-nums', fontSize: 12 }}>{fmtAgo(hub.scraped_at, now)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
      {(hasMore || sorted.length > PAGE_SIZE) && (
        <div className="pagination">
          <span className="pagination-info">Showing {paginated.length} of {sorted.length}</span>
          {hasMore && (
            <button className="btn btn-outline" onClick={() => setPage(p => p + 1)}>
              Load more
            </button>
          )}
        </div>
      )}
    </div>
  )
}
