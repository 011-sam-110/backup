import { useState, useEffect } from 'react'
import { useFilters, applyFilters } from '../context/FilterContext'
import DateRangePicker from './DateRangePicker'
import ExportModal from './ExportModal'
import OperatorDropdown from './OperatorDropdown'
import { authFetch } from '../context/AuthContext'
import { groupColor } from '../utils/status'
import * as XLSX from 'xlsx'

const CONNECTOR_OPTIONS = [
  { value: 'all',              label: 'All connectors' },
  { value: 'IEC_62196_T2_COMBO', label: 'CCS2' },
  { value: 'CHADEMO',          label: 'CHAdeMO' },
  { value: 'IEC_62196_T2',     label: 'Type 2' },
]

function Section({ title, icon, defaultOpen = true, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={{ borderBottom: '1px solid var(--border)' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', display: 'flex', alignItems: 'center', gap: 7,
          padding: '9px 12px', background: 'none', border: 'none',
          color: '#495057', cursor: 'pointer',
          fontSize: 12, fontWeight: 700, textTransform: 'uppercase',
          letterSpacing: '0.09em', userSelect: 'none',
          fontFamily: 'Inter, inherit',
          transition: 'color 0.15s',
        }}
        onMouseOver={e => e.currentTarget.style.color = '#495057'}
        onMouseOut={e => e.currentTarget.style.color = '#495057'}
      >
        <span style={{ fontSize: 12, opacity: 0.65 }}>{icon}</span>
        <span style={{ flex: 1, textAlign: 'left' }}>{title}</span>
        <span style={{ fontSize: 9, transition: 'transform 0.2s', transform: open ? 'rotate(0deg)' : 'rotate(-90deg)', opacity: 0.5 }}>▾</span>
      </button>
      {open && (
        <div style={{ padding: '2px 12px 12px' }}>
          {children}
        </div>
      )}
    </div>
  )
}

function Label({ children }) {
  return (
    <div style={{ fontSize: 12, color: '#495057', marginBottom: 4, marginTop: 8, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
      {children}
    </div>
  )
}

export default function Toolbar() {
  const filters = useFilters()
  const {
    search, setSearch,
    minKw, setMinKw,
    maxKw, setMaxKw,
    minEvses, setMinEvses,
    maxEvses, setMaxEvses,
    minUtil, setMinUtil,
    maxUtil, setMaxUtil,
    connectorFilter, setConnectorFilter,
    operatorFilter, toggleOperator, clearOperators,
    dateRange, setDateRange,
    availableOperators,
    clearFilters,
    groups, loadGroups,
    activeGroupIds, toggleGroup, clearGroups,
    assigningGroupId, toggleAssigningGroup,
    groupFiltersActive,
  } = filters

  const [showExport, setShowExport] = useState(false)
  const [newGroupName, setNewGroupName] = useState('')
  const [creatingGroup, setCreatingGroup] = useState(false)
  const [renamingId, setRenamingId] = useState(null)
  const [renameVal, setRenameVal] = useState('')
  const [exportingGroups, setExportingGroups] = useState(false)

  const handleExportGroups = async () => {
    if (exportingGroups || groups.length === 0) return
    setExportingGroups(true)
    try {
      const allHubs = await authFetch('/api/hubs').then(r => r.json())
      const hubMap = new Map(allHubs.map(h => [h.uuid, h]))
      const wb = XLSX.utils.book_new()
      for (const g of groups) {
        const uuids = await authFetch(`/api/groups/${g.id}/hubs`).then(r => r.json())
        const rows = uuids.map(uuid => {
          const h = hubMap.get(uuid) || {}
          return {
            'Hub Name':        h.hub_name || uuid,
            'Operator':        h.operator || '—',
            'UUID':            uuid,
            'Max Power (kW)':  h.max_power_kw ?? '',
            'Total EVSEs':     h.total_evses ?? '',
            'Connector Types': (h.connector_types || []).join(', '),
            'Utilisation %':   h.utilisation_pct ?? '',
            'Charging':        h.charging_count ?? '',
            'Available':       h.available_count ?? '',
            'Inoperative':     h.inoperative_count ?? '',
            'Last Updated':    h.scraped_at || '',
          }
        })
        const ws = XLSX.utils.json_to_sheet(rows.length ? rows : [{ 'Hub Name': '(empty group)' }])
        XLSX.utils.book_append_sheet(wb, ws, g.name.slice(0, 31))
      }
      XLSX.writeFile(wb, `groups_export_${new Date().toISOString().slice(0, 10)}.xlsx`)
    } catch { /* ignore */ }
    setExportingGroups(false)
  }

  useEffect(() => { loadGroups() }, [loadGroups])

  const buildFilterPayload = () => {
    const payload = {
      connector_filter: connectorFilter !== 'all' ? connectorFilter : null,
      operator_filter:  operatorFilter.size > 0 ? JSON.stringify([...operatorFilter]) : null,
      min_kw:    minKw    !== '' ? parseFloat(minKw)    : null,
      max_kw:    maxKw    !== '' ? parseFloat(maxKw)    : null,
      min_evses: minEvses !== '' ? parseInt(minEvses)   : null,
      max_evses: maxEvses !== '' ? parseInt(maxEvses)   : null,
      min_util:  minUtil  !== '' ? parseFloat(minUtil)  : null,
      max_util:  maxUtil  !== '' ? parseFloat(maxUtil)  : null,
    }
    return Object.values(payload).some(v => v !== null) ? payload : null
  }

  const createGroup = async () => {
    const name = newGroupName.trim()
    if (!name || creatingGroup) return
    setCreatingGroup(true)
    try {
      const filterPayload = buildFilterPayload()
      await authFetch('/api/groups', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, ...(filterPayload || {}) }),
      })
      setNewGroupName('')
      await loadGroups()
    } catch { /* ignore */ }
    setCreatingGroup(false)
  }

  const saveFiltersToGroup = async (id) => {
    try {
      const payload = buildFilterPayload() || {
        connector_filter: null, operator_filter: null,
        min_kw: null, max_kw: null, min_evses: null,
        max_evses: null, min_util: null, max_util: null,
      }
      await authFetch(`/api/groups/${id}/filters`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      await loadGroups()
    } catch { /* ignore */ }
  }

  const deleteGroup = async (id) => {
    try {
      await authFetch(`/api/groups/${id}`, { method: 'DELETE' })
      clearGroups()
      await loadGroups()
    } catch { /* ignore */ }
  }

  const renameGroup = async (id) => {
    const name = renameVal.trim()
    if (!name) return
    try {
      await authFetch(`/api/groups/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      setRenamingId(null)
      await loadGroups()
    } catch { /* ignore */ }
  }

  const hasFilters = search || minKw || maxKw || minEvses || maxEvses || minUtil || maxUtil ||
    connectorFilter !== 'all' || operatorFilter.size > 0 ||
    dateRange.start


  return (
    <>
      <aside style={{
        width: 280,
        flexShrink: 0,
        background: '#F1F3F5',
        borderRight: '1px solid var(--border)',
        overflowY: 'auto',
        display: 'flex',
        flexDirection: 'column',
        fontSize: 13,
        color: 'var(--text)',
      }}>
        {/* Header */}
        <div style={{
          padding: '12px 12px 10px',
          borderBottom: '1px solid var(--border)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        }}>
          <span style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.09em', color: '#495057' }}>
            Filters
          </span>
          {hasFilters && (
            <button
              onClick={clearFilters}
              style={{
                background: 'none', border: 'none',
                color: 'var(--accent)', fontSize: 10,
                cursor: 'pointer', padding: '2px 6px',
                borderRadius: 4, fontFamily: 'Inter, inherit',
                fontWeight: 600, letterSpacing: '0.02em',
                transition: 'opacity 0.15s',
              }}
              title="Clear all filters"
            >
              Reset
            </button>
          )}
        </div>

        <Section title="Groups" icon="◈" defaultOpen={true}>
          {groups.length === 0 && (
            <div style={{ fontSize: 11, color: 'var(--text-dim)', marginBottom: 8 }}>
              No groups yet. Create one below.
            </div>
          )}
          {groups.map(g => (
            <div key={g.id} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
              {renamingId === g.id ? (
                <>
                  <input
                    value={renameVal}
                    onChange={e => setRenameVal(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') renameGroup(g.id); if (e.key === 'Escape') setRenamingId(null) }}
                    autoFocus
                    style={{
                      flex: 1, background: 'var(--bg)', border: '1px solid var(--border)',
                      borderRadius: 5, padding: '3px 6px', fontSize: 12,
                      color: 'var(--text)', outline: 'none',
                    }}
                  />
                  <button onClick={() => renameGroup(g.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--accent)', fontSize: 12 }}>✓</button>
                  <button onClick={() => setRenamingId(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 12 }}>✕</button>
                </>
              ) : (
                <>
                  <span
                    onClick={() => toggleGroup(g.id)}
                    style={{
                      width: 14, height: 14, borderRadius: 3, flexShrink: 0,
                      border: `1.5px solid ${activeGroupIds.has(g.id) ? 'var(--accent)' : 'var(--border)'}`,
                      background: activeGroupIds.has(g.id) ? 'var(--accent)' : 'transparent',
                      cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}
                  >
                    {activeGroupIds.has(g.id) && (
                      <svg width="9" height="9" viewBox="0 0 9 9" fill="none">
                        <path d="M1.5 4.5L3.5 6.5L7.5 2.5" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                      </svg>
                    )}
                  </span>
                  <span
                    style={{ width: 8, height: 8, borderRadius: '50%', flexShrink: 0, background: groupColor(g.id) }}
                  />
                  {(g.connector_filter || g.operator_filter || g.min_kw != null || g.max_kw != null ||
                    g.min_evses != null || g.max_evses != null || g.min_util != null || g.max_util != null) && (
                    <span title="Has saved filters" style={{ fontSize: 9, color: 'var(--accent)', opacity: 0.8, lineHeight: 1, flexShrink: 0 }}>▼</span>
                  )}
                  <span
                    onClick={() => toggleGroup(g.id)}
                    style={{ flex: 1, fontSize: 12, cursor: 'pointer', userSelect: 'none',
                      color: activeGroupIds.has(g.id) ? 'var(--accent)' : 'var(--text)',
                      fontWeight: activeGroupIds.has(g.id) ? 600 : 400,
                    }}
                  >
                    {g.name}
                  </span>
                  <span style={{ fontSize: 10, color: 'var(--text-dim)', marginRight: 2 }}>{g.hub_count}</span>
                  <button
                    onClick={() => saveFiltersToGroup(g.id)}
                    title="Save current filters to group"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 11, padding: '0 1px', lineHeight: 1 }}
                  >⊞</button>
                  <button
                    onClick={() => toggleAssigningGroup(g.id)}
                    title="Select hubs on map"
                    style={{
                      background: assigningGroupId === g.id ? groupColor(g.id) : 'none',
                      color: assigningGroupId === g.id ? '#fff' : 'var(--text-dim)',
                      border: 'none', cursor: 'pointer', fontSize: 11,
                      borderRadius: 4, padding: '0 2px', lineHeight: 1,
                    }}
                  >◎</button>
                  <button
                    onClick={() => { setRenamingId(g.id); setRenameVal(g.name) }}
                    title="Rename"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 11, padding: '0 1px', lineHeight: 1 }}
                  >✎</button>
                  <button
                    onClick={() => deleteGroup(g.id)}
                    title="Delete group"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 11, padding: '0 1px', lineHeight: 1 }}
                  >✕</button>
                </>
              )}
            </div>
          ))}
          {activeGroupIds.size > 0 && (
            <button
              onClick={clearGroups}
              style={{ fontSize: 10, background: 'none', border: 'none', cursor: 'pointer', color: 'var(--accent)', padding: '4px 0', fontFamily: 'Inter, inherit', fontWeight: 600 }}
            >
              Clear groups
            </button>
          )}
          <div style={{ display: 'flex', gap: 4, marginTop: 8 }}>
            <input
              value={newGroupName}
              onChange={e => setNewGroupName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && createGroup()}
              placeholder="New group…"
              className="filter-input"
              style={{ flex: 1, boxSizing: 'border-box' }}
            />
            <button
              onClick={createGroup}
              disabled={!newGroupName.trim() || creatingGroup}
              style={{
                background: 'var(--accent)', color: '#fff', border: 'none',
                borderRadius: 6, padding: '4px 8px', fontSize: 12,
                cursor: newGroupName.trim() && !creatingGroup ? 'pointer' : 'not-allowed',
                opacity: newGroupName.trim() && !creatingGroup ? 1 : 0.5,
              }}
            >+</button>
          </div>
        </Section>

        <Section title="Search" icon="⌕" defaultOpen={true}>
          <input
            className="filter-input"
            style={{ width: '100%', boxSizing: 'border-box' }}
            placeholder="Name, UUID, operator…"
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </Section>

        <Section title="Date Range" icon="📅" defaultOpen={true}>
          <DateRangePicker
            value={dateRange}
            onChange={range => setDateRange(range)}
          />
          {dateRange.start && (
            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 6 }}>
              Showing averaged data for selected period
            </div>
          )}
        </Section>

        <Section title="Filters" icon="⊟" defaultOpen={true}>
          {groupFiltersActive && (
            <div style={{ fontSize: 10, color: 'var(--accent)', marginBottom: 6, fontStyle: 'italic', opacity: 0.8 }}>
              Using group filters
            </div>
          )}
          <Label>Operator</Label>
          <OperatorDropdown
            operators={availableOperators}
            value={operatorFilter}
            onToggle={toggleOperator}
            onClearAll={clearOperators}
            disabled={groupFiltersActive}
          />

          <Label>Connector</Label>
          <select
            className="filter-input"
            style={{ width: '100%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
            value={groupFiltersActive ? 'all' : connectorFilter}
            onChange={e => !groupFiltersActive && setConnectorFilter(e.target.value)}
            disabled={groupFiltersActive}
          >
            {CONNECTOR_OPTIONS.map(c => (
              <option key={c.value} value={c.value}>{c.label}</option>
            ))}
          </select>

          <Label>Power (kW)</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Min'}
              min="0"
              value={groupFiltersActive ? '' : minKw}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMinKw(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Max'}
              min="0"
              value={groupFiltersActive ? '' : maxKw}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMaxKw(e.target.value)}
            />
          </div>

          <Label>EVSEs</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Min'}
              min="0"
              value={groupFiltersActive ? '' : minEvses}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMinEvses(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Max'}
              min="0"
              value={groupFiltersActive ? '' : maxEvses}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMaxEvses(e.target.value)}
            />
          </div>

          <Label>Utilisation %</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Min'}
              min="0"
              max="100"
              value={groupFiltersActive ? '' : minUtil}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMinUtil(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box', opacity: groupFiltersActive ? 0.45 : 1, cursor: groupFiltersActive ? 'not-allowed' : 'auto' }}
              type="number"
              placeholder={groupFiltersActive ? 'Group filter' : 'Max'}
              min="0"
              max="100"
              value={groupFiltersActive ? '' : maxUtil}
              disabled={groupFiltersActive}
              onChange={e => !groupFiltersActive && setMaxUtil(e.target.value)}
            />
          </div>
        </Section>

        <Section title="Export" icon="↓" defaultOpen={false}>
          <button
            className="btn btn-outline"
            style={{ width: '100%', marginTop: 4 }}
            onClick={() => setShowExport(true)}
          >
            ↓ Export to Excel
          </button>
          {groups.length > 0 && (
            <button
              className="btn btn-outline"
              style={{ width: '100%', marginTop: 6 }}
              onClick={handleExportGroups}
              disabled={exportingGroups}
            >
              {exportingGroups ? 'Exporting…' : '↓ Export Groups'}
            </button>
          )}
        </Section>

        {/* Spacer */}
        <div style={{ flex: 1 }} />
      </aside>

      {showExport && <ExportModal onClose={() => setShowExport(false)} />}
    </>
  )
}
