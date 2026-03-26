import { useState, useEffect } from 'react'

const STORAGE_KEY = 'ev_operator_favourites'

function loadFavourites() {
  try { return new Set(JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]')) }
  catch { return new Set() }
}

export default function OperatorDropdown({ operators, value, onChange }) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [favourites, setFavourites] = useState(loadFavourites)

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify([...favourites]))
  }, [favourites])

  function toggleFavourite(op, e) {
    e.stopPropagation()
    setFavourites(prev => {
      const next = new Set(prev)
      next.has(op) ? next.delete(op) : next.add(op)
      return next
    })
  }

  function select(op) {
    onChange(op)
    setOpen(false)
    setSearch('')
  }

  const q = search.toLowerCase()
  const filtered = operators.filter(op => op.toLowerCase().includes(q))
  const favOps   = filtered.filter(op => favourites.has(op))
  const otherOps = filtered.filter(op => !favourites.has(op))
  const showAll  = !search || 'all operators'.includes(q)

  return (
    <div style={{ width: '100%' }}>
      {/* Trigger */}
      <div className="filter-input op-trigger" onClick={() => setOpen(o => !o)}>
        <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {value === 'all' ? 'All operators' : value}
        </span>
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
            {/* "All operators" — always first, no star */}
            {showAll && (
              <div
                className={`op-option${value === 'all' ? ' active' : ''}`}
                onClick={() => select('all')}
              >
                <span>All operators</span>
              </div>
            )}

            {/* Favourites */}
            {favOps.length > 0 && (
              <>
                <div className="op-section-label">Favourites</div>
                {favOps.map(op => (
                  <OpRow key={op} op={op} active={value === op} fav
                    onSelect={() => select(op)}
                    onToggleFav={e => toggleFavourite(op, e)}
                  />
                ))}
                {otherOps.length > 0 && <div className="op-divider" />}
              </>
            )}

            {/* All others */}
            {otherOps.map(op => (
              <OpRow key={op} op={op} active={value === op} fav={false}
                onSelect={() => select(op)}
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

function OpRow({ op, active, fav, onSelect, onToggleFav }) {
  return (
    <div className={`op-option${active ? ' active' : ''}`} onClick={onSelect}>
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
