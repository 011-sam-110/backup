import { useState, useRef, useEffect } from 'react'
import { authFetch } from '../context/AuthContext'
import { useFilters } from '../context/FilterContext'

/**
 * Popover for adding/removing a hub from existing groups.
 * Create groups via the sidebar toolbar.
 *
 * Props:
 *   hubUuid   – the hub being added
 *   onClose   – called when the picker should close
 */
export default function GroupPicker({ hubUuid, onClose }) {
  const { groups, loadGroups } = useFilters()
  const [saving, setSaving] = useState(null) // group id currently being saved
  const [hubGroups, setHubGroups] = useState(null) // Set of group IDs this hub is in
  const ref = useRef(null)

  // Load which groups this hub already belongs to
  useEffect(() => {
    authFetch(`/api/hubs/${hubUuid}/groups`)
      .then(r => r.ok ? r.json() : [])
      .then(ids => setHubGroups(new Set(ids)))
      .catch(() => setHubGroups(new Set()))
  }, [hubUuid])

  // Close on outside click
  useEffect(() => {
    const handler = e => { if (ref.current && !ref.current.contains(e.target)) onClose() }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [onClose])

  const toggle = async (group) => {
    if (saving) return
    setSaving(group.id)
    const inGroup = hubGroups?.has(group.id)
    try {
      if (inGroup) {
        await authFetch(`/api/groups/${group.id}/hubs/${hubUuid}`, { method: 'DELETE' })
        setHubGroups(prev => { const n = new Set(prev); n.delete(group.id); return n })
      } else {
        await authFetch(`/api/groups/${group.id}/hubs`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ hub_uuids: [hubUuid] }),
        })
        setHubGroups(prev => new Set([...prev, group.id]))
        await loadGroups()
      }
    } catch { /* ignore */ }
    setSaving(null)
  }

  const popoverStyle = {
    position: 'absolute', top: '100%', right: 0, zIndex: 200,
    background: 'var(--surface)', border: '1px solid var(--border)',
    borderRadius: 10, padding: '12px 14px', minWidth: 220,
    boxShadow: '0 8px 24px rgba(0,0,0,0.45)',
    marginTop: 6,
  }

  return (
    <div ref={ref} style={popoverStyle}>
      <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.09em', marginBottom: 10 }}>
        Add to Group
      </div>

      {groups.length === 0 ? (
        <div style={{ fontSize: 12, color: 'var(--text-dim)' }}>Create a group in the sidebar first.</div>
      ) : groups.map(g => {
        const inGroup = hubGroups?.has(g.id)
        const isSaving = saving === g.id
        return (
          <div
            key={g.id}
            onClick={() => toggle(g)}
            style={{
              display: 'flex', alignItems: 'center', gap: 9,
              padding: '7px 4px', cursor: isSaving ? 'wait' : 'pointer',
              borderRadius: 6, transition: 'background 0.12s',
            }}
            onMouseOver={e => e.currentTarget.style.background = 'var(--surface-hover, rgba(255,255,255,0.05))'}
            onMouseOut={e => e.currentTarget.style.background = 'transparent'}
          >
            <span style={{
              width: 14, height: 14, borderRadius: 3, flexShrink: 0,
              border: '1.5px solid var(--border)',
              background: inGroup ? 'var(--accent)' : 'transparent',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              {inGroup && (
                <svg width="9" height="9" viewBox="0 0 9 9" fill="none">
                  <path d="M1.5 4.5L3.5 6.5L7.5 2.5" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              )}
            </span>
            <span style={{ fontSize: 13, flex: 1 }}>{g.name}</span>
            <span style={{ fontSize: 10, color: 'var(--text-dim)' }}>{g.hub_count}</span>
          </div>
        )
      })}
    </div>
  )
}
