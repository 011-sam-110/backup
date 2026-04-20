import { useState } from 'react'
import * as XLSX from 'xlsx'
import { useFilters, applyFilters } from '../context/FilterContext'
import { authFetch } from '../context/AuthContext'

const ALL_COLS = [
  { key: 'uuid',               label: 'Hub UUID' },
  { key: 'hub_name',           label: 'Site Name' },
  { key: 'operator',           label: 'Operator' },
  { key: 'latitude',           label: 'Latitude' },
  { key: 'longitude',          label: 'Longitude' },
  { key: 'max_power_kw',       label: 'Max kW' },
  { key: 'total_evses',        label: 'Total EVSEs' },
  { key: 'connector_types',    label: 'Connector Types' },
  { key: 'charging_count',     label: 'Charging' },
  { key: 'available_count',    label: 'Available' },
  { key: 'inoperative_count',  label: 'Inoperative' },
  { key: 'out_of_order_count', label: 'Out of Order' },
  { key: 'unknown_count',      label: 'Unknown' },
  { key: 'utilisation_pct',    label: 'Utilisation %' },
  {
    key: 'estimated_kw',
    label: 'Est. Draw (kW)',
    compute: row => {
      const kw = (row.charging_count ?? 0) * (row.max_power_kw ?? 0)
      return kw > 0 ? Math.round(kw) : ''
    },
  },
  { key: 'scraped_at',         label: 'Timestamp' },
]

const initCols = Object.fromEntries(ALL_COLS.map(c => [c.key, true]))

function toRows(data, activeCols) {
  return data.map(item => {
    const row = {}
    for (const col of activeCols) {
      let val = col.compute ? col.compute(item) : item[col.key]
      if (Array.isArray(val)) val = val.join(', ')
      if (val === null || val === undefined) val = ''
      row[col.label] = val
    }
    return row
  })
}

export default function ExportModal({ onClose }) {
  const filters = useFilters()
  const { hubsUrl } = filters

  const hasFilters = filters.search || filters.minKw || filters.maxKw ||
    filters.minUtil || filters.maxUtil || filters.connectorFilter !== 'all' ||
    filters.operatorFilter.size > 0 || filters.dateRange?.start

  const [scope, setScope] = useState('current')
  const [hubScope, setHubScope] = useState('all')
  const [hours, setHours] = useState(168)
  const [cols, setCols] = useState(initCols)
  const [downloading, setDownloading] = useState(false)

  const toggleCol = key => setCols(prev => ({ ...prev, [key]: !prev[key] }))
  const allSelected = ALL_COLS.every(c => cols[c.key])
  const toggleAll = () => {
    const next = !allSelected
    setCols(Object.fromEntries(ALL_COLS.map(c => [c.key, next])))
  }

  async function handleDownload() {
    const anySelected = ALL_COLS.some(c => cols[c.key])
    if (!anySelected) { alert('Select at least one column.'); return }

    setDownloading(true)
    try {
      const activeCols = ALL_COLS.filter(c => cols[c.key])
      const isFiltered = hubScope === 'filtered' && hasFilters
      let data

      if (scope === 'current') {
        const res = await authFetch(isFiltered ? hubsUrl() : '/api/hubs')
        const raw = await res.json()
        data = isFiltered ? applyFilters(raw, filters) : raw
      } else {
        const snapshotUrl = `/api/export/snapshots?hours=${hours}`
        if (isFiltered) {
          const [hubRes, snapRes] = await Promise.all([
            authFetch(hubsUrl()),
            authFetch(snapshotUrl),
          ])
          const [hubData, snapData] = await Promise.all([hubRes.json(), snapRes.json()])
          const filteredUUIDs = new Set(applyFilters(hubData, filters).map(h => h.uuid))
          data = snapData.filter(row => filteredUUIDs.has(row.uuid))
        } else {
          const res = await authFetch(snapshotUrl)
          data = await res.json()
        }
      }

      const ws = XLSX.utils.json_to_sheet(toRows(data, activeCols))
      const wb = XLSX.utils.book_new()
      const sheetName = scope === 'current' ? 'Current Snapshot' : `History ${hours}h`
      XLSX.utils.book_append_sheet(wb, ws, sheetName)

      const date = new Date().toISOString().slice(0, 10)
      const suffix = isFiltered ? '_filtered' : ''
      XLSX.writeFile(wb, `ev_hubs_${date}${suffix}.xlsx`)
      onClose()
    } catch (e) {
      alert('Export failed: ' + e.message)
    } finally {
      setDownloading(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" style={{ maxWidth: 500 }} onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">Export to Excel</span>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        {/* Scope */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', marginBottom: 10 }}>
            Scope
          </div>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, cursor: 'pointer', fontSize: 13 }}>
            <input type="radio" name="scope" value="current" checked={scope === 'current'} onChange={() => setScope('current')} />
            Current snapshot — one row per hub, latest data only
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', fontSize: 13 }}>
            <input type="radio" name="scope" value="history" checked={scope === 'history'} onChange={() => setScope('history')} />
            Full history — all scrape runs, last&nbsp;
            <input
              className="filter-input"
              type="number"
              min="1"
              max="8760"
              value={hours}
              onChange={e => setHours(Number(e.target.value))}
              style={{ width: 70, padding: '3px 8px' }}
              disabled={scope !== 'history'}
            />
            &nbsp;hours
          </label>
        </div>

        {/* Hubs */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', marginBottom: 10 }}>
            Hubs
          </div>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, cursor: 'pointer', fontSize: 13 }}>
            <input type="radio" name="hubScope" value="all" checked={hubScope === 'all'} onChange={() => setHubScope('all')} />
            All hubs
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: hasFilters ? 'pointer' : 'not-allowed', fontSize: 13, opacity: hasFilters ? 1 : 0.4 }}>
            <input
              type="radio"
              name="hubScope"
              value="filtered"
              checked={hubScope === 'filtered'}
              onChange={() => setHubScope('filtered')}
              disabled={!hasFilters}
            />
            Current filter
            {!hasFilters && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>(no filters active)</span>}
          </label>
        </div>

        {/* Columns */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <div style={{ fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)' }}>
              Columns
            </div>
            <button
              style={{ background: 'none', border: 'none', color: 'var(--accent)', fontSize: 12, cursor: 'pointer', padding: 0 }}
              onClick={toggleAll}
            >
              {allSelected ? 'Deselect all' : 'Select all'}
            </button>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px' }}>
            {ALL_COLS.map(col => (
              <label key={col.key} style={{ display: 'flex', alignItems: 'center', gap: 7, cursor: 'pointer', fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={cols[col.key]}
                  onChange={() => toggleCol(col.key)}
                />
                {col.label}
              </label>
            ))}
          </div>
        </div>

        {/* Actions */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10 }}>
          <button className="btn btn-outline" onClick={onClose}>Cancel</button>
          <button className="btn btn-primary" onClick={handleDownload} disabled={downloading}>
            {downloading ? 'Downloading...' : '↓ Download Excel'}
          </button>
        </div>
      </div>
    </div>
  )
}
