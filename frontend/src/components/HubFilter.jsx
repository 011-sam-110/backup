import { useState, useRef, useEffect } from 'react'

export default function HubFilter({ hubs, value, onChange }) {
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  const selected = hubs.find(h => h.uuid === value) || null

  useEffect(() => {
    function handler(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const displayLabel = selected ? (selected.hub_name || selected.uuid) : ''

  const filtered = hubs
    .filter(h => {
      if (!query) return true
      const q = query.toLowerCase()
      return h.uuid.toLowerCase().includes(q) || (h.hub_name || '').toLowerCase().includes(q)
    })
    .sort((a, b) => {
      if (a.hub_name && !b.hub_name) return -1
      if (!a.hub_name && b.hub_name) return 1
      return (a.hub_name || a.uuid).localeCompare(b.hub_name || b.uuid)
    })

  function select(hub) {
    onChange(hub.uuid)
    setQuery('')
    setOpen(false)
  }

  function clear(e) {
    e.stopPropagation()
    onChange(null)
    setQuery('')
  }

  return (
    <div ref={ref} style={{ position: 'relative', display: 'inline-block', minWidth: 280 }}>
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 6,
          background: 'var(--bg-card)', border: '1px solid var(--border)',
          borderRadius: 6, padding: '6px 10px', cursor: 'text',
        }}
        onClick={() => setOpen(true)}
      >
        <span style={{ fontSize: 12, color: 'var(--text-muted)', flexShrink: 0 }}>Hub:</span>
        <input
          style={{
            background: 'transparent', border: 'none', outline: 'none',
            color: 'var(--text)', fontSize: 13, flex: 1, minWidth: 0,
          }}
          placeholder="All hubs"
          value={open ? query : displayLabel}
          onChange={e => { setQuery(e.target.value); setOpen(true) }}
          onFocus={() => { setQuery(''); setOpen(true) }}
        />
        {value && (
          <button onClick={clear} style={{
            background: 'none', border: 'none', color: 'var(--text-muted)',
            cursor: 'pointer', fontSize: 16, lineHeight: 1, padding: 0,
          }}>×</button>
        )}
      </div>

      {selected?.hub_name && (
        <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)', marginTop: 3, paddingLeft: 2 }}>
          {selected.uuid}
        </div>
      )}

      {open && (
        <ul style={{
          position: 'absolute', top: '100%', left: 0, right: 0,
          background: 'var(--bg-card)', border: '1px solid var(--border)',
          borderRadius: 6, marginTop: 4, maxHeight: 260, overflowY: 'auto',
          zIndex: 100, listStyle: 'none', padding: 0,
        }}>
          {filtered.length === 0 ? (
            <li style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: 12 }}>No matches</li>
          ) : filtered.map(h => (
            <li
              key={h.uuid}
              onClick={() => select(h)}
              style={{
                padding: '8px 12px', cursor: 'pointer', fontSize: 13,
                borderBottom: '1px solid var(--border)',
                background: h.uuid === value ? 'rgba(99,102,241,0.12)' : 'transparent',
              }}
              onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.04)'}
              onMouseLeave={e => e.currentTarget.style.background = h.uuid === value ? 'rgba(99,102,241,0.12)' : 'transparent'}
            >
              <div>{h.hub_name || <span style={{ fontFamily: 'monospace' }}>{h.uuid}</span>}</div>
              {h.hub_name && (
                <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)', marginTop: 1 }}>{h.uuid}</div>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
