import { useState, useEffect } from 'react'
import { useFilters, applyFilters } from '../context/FilterContext'
import DateRangePicker from './DateRangePicker'
import ExportModal from './ExportModal'
import ArchiveModal from './ArchiveModal'
import OperatorDropdown from './OperatorDropdown'
import { authFetch } from '../context/AuthContext'
import { groupColor } from '../utils/status'
import * as XLSX from 'xlsx'

// Human-readable labels for known Zapmap connector standard codes.
// Unknown codes fall back to the raw value.
const CONNECTOR_LABELS = {
  IEC_62196_T2_COMBO: 'CCS2',
  CHADEMO:            'CHAdeMO',
  IEC_62196_T2:       'Type 2',
  IEC_62196_T1_COMBO: 'CCS1',
  IEC_62196_T1:       'Type 1',
  TESLA_R:            'Tesla (Roadster)',
  TESLA_S:            'Tesla (S)',
  GBT_AC:             'GB/T AC',
  GBT_DC:             'GB/T DC',
  DOMESTIC_A:         'Domestic A',
  DOMESTIC_B:         'Domestic B',
  DOMESTIC_C:         'Domestic C',
  DOMESTIC_D:         'Domestic D',
  DOMESTIC_E:         'Domestic E',
  DOMESTIC_F:         'Domestic F',
  DOMESTIC_G:         'Domestic G',
  DOMESTIC_H:         'Domestic H',
  DOMESTIC_I:         'Domestic I',
  DOMESTIC_J:         'Domestic J',
  DOMESTIC_K:         'Domestic K',
  DOMESTIC_L:         'Domestic L',
}

function Section({ title, icon, defaultOpen = true, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div style={{ borderBottom: '1px solid #000' }}>
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

export default function Toolbar({ isOpen, onClose, onToggle }) {
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
    startHour, setStartHour,
    endHour, setEndHour,
    availableOperators,
    clearFilters,
    groups, loadGroups,
    activeGroupIds, toggleGroup, clearGroups,
    assigningGroupId, toggleAssigningGroup,
  } = filters

  const [connectorOptions, setConnectorOptions] = useState([])

  useEffect(() => {
    authFetch('/api/connector-types')
      .then(r => r.json())
      .then(data => setConnectorOptions(data))
      .catch(() => {}) // non-fatal; falls back to empty list
  }, [])

  const [showExport, setShowExport] = useState(false)
  const [showArchive, setShowArchive] = useState(false)
  const [newGroupName, setNewGroupName] = useState('')
  const [creatingGroup, setCreatingGroup] = useState(false)
  const [renamingId, setRenamingId] = useState(null)
  const [renameVal, setRenameVal] = useState('')
  const [exportingGroups, setExportingGroups] = useState(false)
  const [addingToGroup, setAddingToGroup] = useState(null)

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

  const createGroup = async () => {
    const name = newGroupName.trim()
    if (!name || creatingGroup) return
    setCreatingGroup(true)
    try {
      await authFetch('/api/groups', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      setNewGroupName('')
      await loadGroups()
    } catch { /* ignore */ }
    setCreatingGroup(false)
  }

  const addFilteredToGroup = async (groupId) => {
    if (addingToGroup) return
    setAddingToGroup(groupId)
    try {
      const hubs = await authFetch(filters.hubsUrl()).then(r => r.json())
      const filtered = applyFilters(hubs, filters)
      const uuids = filtered.map(h => h.uuid)
      if (uuids.length > 0) {
        await authFetch(`/api/groups/${groupId}/hubs`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ hub_uuids: uuids }),
        })
        await loadGroups()
      }
    } catch { /* ignore */ }
    setAddingToGroup(null)
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

  const setGroupInterval = async (groupId, minutes) => {
    try {
      await authFetch(`/api/groups/${groupId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scrape_interval: minutes }),
      })
      await loadGroups()
    } catch { /* ignore */ }
  }

  const [addingIntervalGroup, setAddingIntervalGroup] = useState(null)
  const [targetedEnabled, setTargetedEnabled] = useState(true)
  const [targetedToggling, setTargetedToggling] = useState(false)

  useEffect(() => {
    authFetch('/api/settings').then(r => r.json()).then(d => setTargetedEnabled(d.targeted_scraping_enabled)).catch(() => {})
  }, [])

  const toggleTargetedScraping = async () => {
    if (targetedToggling) return
    setTargetedToggling(true)
    try {
      const res = await authFetch('/api/settings', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targeted_scraping_enabled: !targetedEnabled }),
      })
      const d = await res.json()
      setTargetedEnabled(d.targeted_scraping_enabled)
    } catch { /* ignore */ }
    setTargetedToggling(false)
  }

  const INTERVAL_COLOURS = {
    1: '#ef4444',
    2: '#f97316',
    3: '#eab308',
    4: '#22c55e',
    5: '#3b82f6',
  }

  const hasFilters = search || minKw || maxKw || minEvses || maxEvses || minUtil || maxUtil ||
    connectorFilter !== 'all' || operatorFilter.size > 0 ||
    dateRange.start || startHour !== null || endHour !== null


  return (
    <>
      <aside className={`sidebar${isOpen ? ' sidebar--open' : ' sidebar--closed'}`}>
        {/* Desktop collapse toggle */}
        <button className="sidebar-toggle-btn" onClick={onToggle} aria-label="Toggle sidebar">
          {isOpen ? '‹' : '›'}
        </button>
        {/* Header */}
        <div style={{
          padding: '12px 12px 10px',
          borderBottom: '1px solid var(--border)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          position: 'relative',
        }}>
          <span style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.09em', color: '#495057' }}>
            Filters
          </span>
          <button className="sidebar-close-btn" onClick={onClose} aria-label="Close filters">✕</button>
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

        <Section title="Search" icon="⌕" defaultOpen={true}>
          <input
            className="filter-input"
            style={{ width: '100%', boxSizing: 'border-box' }}
            placeholder="Name, UUID, operator…"
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </Section>

        <Section title="Targeted Scraping" icon="⏱" defaultOpen={false}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <span style={{ fontSize: 11, color: 'var(--text-dim)' }}>
              Assign groups to a polling interval (1–5 min).
            </span>
            <button
              onClick={toggleTargetedScraping}
              disabled={targetedToggling}
              title={targetedEnabled ? 'Targeted scraping ON — click to disable' : 'Targeted scraping OFF — click to enable'}
              style={{
                flexShrink: 0,
                background: targetedEnabled ? 'var(--accent)' : '#adb5bd',
                color: '#fff',
                border: 'none',
                borderRadius: 10,
                padding: '2px 10px',
                fontSize: 11,
                fontWeight: 700,
                cursor: targetedToggling ? 'default' : 'pointer',
                opacity: targetedToggling ? 0.6 : 1,
                transition: 'background 0.2s',
                letterSpacing: '0.03em',
              }}
            >
              {targetedEnabled ? 'ON' : 'OFF'}
            </button>
          </div>
          {[1, 2, 3, 4, 5].map(m => {
            const assignedGroups = groups.filter(g => g.scrape_interval === m)
            const unassignedGroups = groups.filter(g => !g.scrape_interval)
            const isAdding = addingIntervalGroup === m
            return (
              <div key={m} style={{ marginBottom: 8 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                  <span style={{
                    fontSize: 10, fontWeight: 700, color: INTERVAL_COLOURS[m],
                    background: INTERVAL_COLOURS[m] + '18',
                    border: `1px solid ${INTERVAL_COLOURS[m]}40`,
                    borderRadius: 4, padding: '1px 6px', minWidth: 36, textAlign: 'center',
                  }}>
                    {m} min
                  </span>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, flex: 1 }}>
                    {assignedGroups.map(g => (
                      <span key={g.id} style={{
                        display: 'inline-flex', alignItems: 'center', gap: 3,
                        fontSize: 10, background: 'var(--bg)',
                        border: '1px solid var(--border)', borderRadius: 4,
                        padding: '1px 4px', color: 'var(--text)',
                      }}>
                        {g.name}
                        <button
                          onClick={() => setGroupInterval(g.id, null)}
                          title="Remove from targeted scraping"
                          style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 9, padding: 0, lineHeight: 1 }}
                        >✕</button>
                      </span>
                    ))}
                  </div>
                  {unassignedGroups.length > 0 && !isAdding && (
                    <button
                      onClick={() => setAddingIntervalGroup(m)}
                      title={`Add group to ${m}-min scrape`}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-dim)', fontSize: 14, padding: '0 2px', lineHeight: 1 }}
                    >+</button>
                  )}
                </div>
                {isAdding && (
                  <div style={{ display: 'flex', gap: 4, marginLeft: 42 }}>
                    <select
                      autoFocus
                      className="filter-input"
                      style={{ flex: 1, boxSizing: 'border-box', fontSize: 11 }}
                      defaultValue=""
                      onChange={async e => {
                        const id = Number(e.target.value)
                        if (id) { await setGroupInterval(id, m) }
                        setAddingIntervalGroup(null)
                      }}
                      onBlur={() => setAddingIntervalGroup(null)}
                    >
                      <option value="" disabled>Pick group…</option>
                      {unassignedGroups.map(g => (
                        <option key={g.id} value={g.id}>{g.name}</option>
                      ))}
                    </select>
                  </div>
                )}
              </div>
            )
          })}
          {groups.length === 0 && (
            <div style={{ fontSize: 11, color: 'var(--text-dim)' }}>Create groups first to assign them here.</div>
          )}
        </Section>

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
                    onClick={() => addFilteredToGroup(g.id)}
                    title="Add all visible hubs to this group"
                    disabled={!!addingToGroup}
                    style={{
                      background: 'none', border: 'none', cursor: addingToGroup ? 'default' : 'pointer',
                      color: addingToGroup === g.id ? 'var(--accent)' : 'var(--text-dim)',
                      fontSize: 12, padding: '0 2px', lineHeight: 1, opacity: addingToGroup && addingToGroup !== g.id ? 0.4 : 1,
                    }}
                  >{addingToGroup === g.id ? '…' : '⊕'}</button>
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
                  {g.scrape_interval && (
                    <span
                      title={`Targeted scrape every ${g.scrape_interval} min`}
                      style={{
                        fontSize: 9, fontWeight: 700, color: INTERVAL_COLOURS[g.scrape_interval],
                        background: INTERVAL_COLOURS[g.scrape_interval] + '20',
                        borderRadius: 3, padding: '1px 3px', lineHeight: 1,
                      }}
                    >{g.scrape_interval}m</span>
                  )}
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
          <Label>Time of Day</Label>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <select
              className="filter-input"
              style={{ flex: 1, boxSizing: 'border-box' }}
              value={startHour ?? ''}
              onChange={e => setStartHour(e.target.value === '' ? null : Number(e.target.value))}
            >
              <option value="">Any</option>
              {Array.from({ length: 24 }, (_, h) => (
                <option key={h} value={h}>{String(h).padStart(2, '0')}:00</option>
              ))}
            </select>
            <span style={{ fontSize: 11, color: 'var(--text-muted)', flexShrink: 0 }}>to</span>
            <select
              className="filter-input"
              style={{ flex: 1, boxSizing: 'border-box' }}
              value={endHour ?? ''}
              onChange={e => setEndHour(e.target.value === '' ? null : Number(e.target.value))}
            >
              <option value="">Any</option>
              {Array.from({ length: 24 }, (_, h) => (
                <option key={h} value={h}>{String(h).padStart(2, '0')}:00</option>
              ))}
            </select>
          </div>
          {(startHour !== null || endHour !== null) && (
            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>
              Filters visits and snapshots to this hour range
            </div>
          )}
        </Section>

        <Section title="Filters" icon="⊟" defaultOpen={true}>
          <Label>Operator</Label>
          <OperatorDropdown
            operators={availableOperators}
            value={operatorFilter}
            onToggle={toggleOperator}
            onClearAll={clearOperators}
          />

          <Label>Connector</Label>
          <select
            className="filter-input"
            style={{ width: '100%', boxSizing: 'border-box' }}
            value={connectorFilter}
            onChange={e => setConnectorFilter(e.target.value)}
          >
            <option value="all">All connectors</option>
            {connectorOptions.map(c => (
              <option key={c.connector_type} value={c.connector_type}>
                {CONNECTOR_LABELS[c.connector_type] || c.connector_type} ({c.hub_count})
              </option>
            ))}
          </select>

          <Label>Power (kW)</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Min"
              min="0"
              value={minKw}
              onChange={e => setMinKw(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Max"
              min="0"
              value={maxKw}
              onChange={e => setMaxKw(e.target.value)}
            />
          </div>

          <Label>EVSEs</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Min"
              min="0"
              value={minEvses}
              onChange={e => setMinEvses(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Max"
              min="0"
              value={maxEvses}
              onChange={e => setMaxEvses(e.target.value)}
            />
          </div>

          <Label>Utilisation %</Label>
          <div style={{ display: 'flex', gap: 4 }}>
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Min"
              min="0"
              max="100"
              value={minUtil}
              onChange={e => setMinUtil(e.target.value)}
            />
            <input
              className="filter-input"
              style={{ width: '50%', boxSizing: 'border-box' }}
              type="number"
              placeholder="Max"
              min="0"
              max="100"
              value={maxUtil}
              onChange={e => setMaxUtil(e.target.value)}
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
          <button
            className="btn btn-outline"
            style={{ width: '100%', marginTop: 6 }}
            onClick={() => setShowArchive(true)}
          >
            ☁ Archived Exports
          </button>
        </Section>

        {/* Spacer */}
        <div style={{ flex: 1 }} />
      </aside>

      {showExport && <ExportModal onClose={() => setShowExport(false)} />}
      {showArchive && <ArchiveModal onClose={() => setShowArchive(false)} />}
    </>
  )
}
