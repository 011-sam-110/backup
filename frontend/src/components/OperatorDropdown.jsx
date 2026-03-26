import { useState, useEffect, useRef } from 'react'

const STORAGE_KEY = 'ev_operator_favourites'

function loadFavourites() {
  try { return new Set(JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]')) }
  catch { return new Set() }
}

export default function OperatorDropdown({ operators, value, onToggle, onClearAll }) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [favourites, setFavourites] = useState(loadFavourites)
  const containerRef = useRef(null)

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify([...favourites]))
  }, [favourites])

  // Close on outside click
  useEffect(() => {
    if (!open) return
    function handleClick(e) {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setOpen(false)
        setSearch('')
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [open])

  function toggleFavourite(op, e) {
    e.stopPropagation()
    setFavourites(prev => {
      const next = new Set(prev)
      next.has(op) ? next.delete(op) : next.add(op)
      return next
    })
  }

  const q = search.toLowerCase()
  const filtered = operators.filter(op => op.toLowerCase().includes(q))
  const favOps   = filtered.filter(op => favourites.has(op))
  const otherOps = filtered.filter(op => !favourites.has(op))
  const showAll  = !search || 'all operators'.includes(q)

  // Trigger label
  const triggerLabel = value.size === 0
    ? 'All operators'
    : value.size === 1
      ? [...value][0]
      : `${[...value][0]} +${value.size - 1} more`

  return (
    <div style={{ width: '100%' }} ref={containerRef}>
      {/* Trigger */}
      <div className="filter-input op-trigger" onClick={() => setOpen(o => !o)}>
        <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {triggerLabel}
        </span>
        {value.size > 0 && (
          <span
            style={{ fontSize: 10, color: 'var(--accent)', fontWeight: 700, flexShrink: 0, marginRight: 4, cursor: 'pointer' }}
            onClick={e => { e.stopPropagation(); onClearAll() }}
            title="Clear operator filter"
          >
            ✕
          </span>
        )}
        <span style={{ opacity: 0.45, fontSize: 11, flexShrink: 0 }}>{open ? '▲' : '▼'}</span>
      </div>

      {/* Inline panel */}
      {open && (
        <div className="op-panel">
          <div style={{ padding: '6px 6px 4px' }}>
            <input
              className="filter-input"
              placeholder="Search operators…"
              value={search}
              autoFocus
              onChange={e => setSearch(e.target.value)}
              style={{ width: '100%', boxSizing: 'border-box', fontSize: 12, padding: '5px 8px' }}
            />
          </div>

          <div className="op-list">
            {/* "All operators" row */}
            {showAll && (
              <div className={`op-option${value.size === 0 ? ' active' : ''}`} onClick={onClearAll}>
                <span style={{ marginRight: 8, display: 'flex', alignItems: 'center', flexShrink: 0 }}>
                  <Checkbox checked={value.size === 0} />
                </span>
                <span>All operators</span>
              </div>
            )}

            {/* Favourites */}
            {favOps.length > 0 && (
              <>
                <div className="op-section-label">Favourites</div>
                {favOps.map(op => (
                  <OpRow key={op} op={op} checked={value.has(op)} fav
                    onToggle={() => onToggle(op)}
                    onToggleFav={e => toggleFavourite(op, e)}
                  />
                ))}
                {otherOps.length > 0 && <div className="op-divider" />}
              </>
            )}

            {/* All others */}
            {otherOps.map(op => (
              <OpRow key={op} op={op} checked={value.has(op)} fav={false}
                onToggle={() => onToggle(op)}
                onToggleFav={e => toggleFavourite(op, e)}
              />
            ))}

            {!showAll && filtered.length === 0 && (
              <div className="op-empty">No operators match</div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function Checkbox({ checked }) {
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      width: 14, height: 14, borderRadius: 3, flexShrink: 0,
      border: `1.5px solid ${checked ? 'var(--accent)' : '#adb5bd'}`,
      background: checked ? 'var(--accent)' : 'transparent',
      transition: 'background 0.12s, border-color 0.12s',
    }}>
      {checked && (
        <svg width="9" height="7" viewBox="0 0 9 7" fill="none">
          <path d="M1 3.5L3.5 6L8 1" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      )}
    </span>
  )
}

function OpRow({ op, checked, fav, onToggle, onToggleFav }) {
  return (
    <div className={`op-option${checked ? ' active' : ''}`} onClick={onToggle}>
      <span style={{ marginRight: 8, display: 'flex', alignItems: 'center', flexShrink: 0 }}>
        <Checkbox checked={checked} />
      </span>
      <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {op}
      </span>
      <button
        className={`op-star${fav ? ' op-star--on' : ''}`}
        onClick={onToggleFav}
        title={fav ? 'Remove favourite' : 'Add favourite'}
      >
        {fav ? '★' : '☆'}
      </button>
    </div>
  )
}
