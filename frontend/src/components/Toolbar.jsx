import { useState } from 'react'
import { useFilters, applyFilters } from '../context/FilterContext'
import DateRangePicker from './DateRangePicker'
import ExportModal from './ExportModal'
import OperatorDropdown from './OperatorDropdown'

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
    operatorFilter, setOperatorFilter,
    dateRange, setDateRange,
    availableOperators,
    clearFilters,
  } = filters

  const [showExport, setShowExport] = useState(false)

  const hasFilters = search || minKw || maxKw || minEvses || maxEvses || minUtil || maxUtil ||
    connectorFilter !== 'all' || operatorFilter !== 'all' ||
    dateRange.start


  return (
    <>
      <aside style={{
        width: 220,
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
          <Label>Operator</Label>
          <OperatorDropdown
            operators={availableOperators}
            value={operatorFilter}
            onChange={setOperatorFilter}
          />

          <Label>Connector</Label>
          <select
            className="filter-input"
            style={{ width: '100%', boxSizing: 'border-box' }}
            value={connectorFilter}
            onChange={e => setConnectorFilter(e.target.value)}
          >
            {CONNECTOR_OPTIONS.map(c => (
              <option key={c.value} value={c.value}>{c.label}</option>
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
        </Section>

        {/* Spacer */}
        <div style={{ flex: 1 }} />
      </aside>

      {showExport && <ExportModal onClose={() => setShowExport(false)} />}
    </>
  )
}
