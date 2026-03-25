import { useState, useEffect } from 'react'
import { utilColor, utilTier, utilIcon } from '../utils/status'

const PAGE_SIZE = 25

function fmtTime(iso) {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

export default function HubTable({ hubs, onHubClick }) {
  const [sortKey, setSortKey] = useState('utilisation_pct')
  const [sortDir, setSortDir] = useState('desc')
  const [page, setPage] = useState(1)

  useEffect(() => { setPage(1) }, [hubs])

  function toggleSort(key) {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
      setPage(1)
    }
  }

  const sorted = [...hubs].sort((a, b) => {
    const av = a[sortKey] ?? -1
    const bv = b[sortKey] ?? -1
    return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1)
  })

  const paginated = sorted.slice(0, page * PAGE_SIZE)
  const hasMore = paginated.length < sorted.length

  function th(label, key) {
    const active = sortKey === key
    const arrow = active ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''
    const ariaSort = active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
    return (
      <th
        onClick={() => toggleSort(key)}
        style={active ? { color: 'var(--text)' } : {}}
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
            {th('Available', 'available_count')}
            {th('Inop', 'inoperative_count')}
            {th('Utilisation', 'utilisation_pct')}
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
                <td>{hub.max_power_kw}</td>
                <td>{hub.total_evses}</td>
                <td className="green">{hub.charging_count ?? '—'}</td>
                <td>{hub.available_count ?? '—'}</td>
                <td className={hub.inoperative_count > 0 ? 'amber' : ''}>{hub.inoperative_count ?? '—'}</td>
                <td>
                  <div className="util-cell">
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
                <td style={{ color: 'var(--text-muted)' }}>{fmtTime(hub.scraped_at)}</td>
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
