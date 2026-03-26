import { createContext, useContext, useState, useCallback, useMemo, useRef } from 'react'
import { authFetch } from './AuthContext'

const FilterContext = createContext(null)

export function FilterProvider({ children }) {
  const [search, setSearch] = useState('')
  const [minEvses, setMinEvses] = useState('')
  const [maxEvses, setMaxEvses] = useState('')
  const [minUtil, setMinUtil] = useState('')
  const [maxUtil, setMaxUtil] = useState('')
  const [minKw, setMinKw] = useState('')
  const [maxKw, setMaxKw] = useState('')
  const [connectorFilter, setConnectorFilter] = useState('all')
  const [operatorFilter, setOperatorFilter] = useState(new Set())
  const [dateRange, setDateRange] = useState({ start: null, end: null })
  const [availableOperators, setAvailableOperators] = useState([])

  // Groups
  const [groups, setGroups] = useState([])
  const [activeGroupIds, setActiveGroupIds] = useState(new Set())
  const [groupHubs, setGroupHubs] = useState(new Map()) // group_id → string[]
  const [activeGroupFilters, setActiveGroupFilters] = useState(new Map()) // group_id → filter object
  const groupsRef = useRef(groups)

  const activeGroupUuids = useMemo(() => {
    if (activeGroupIds.size === 0) return new Set()
    const uuids = new Set()
    activeGroupIds.forEach(id => {
      const list = groupHubs.get(id) || []
      list.forEach(u => uuids.add(u))
    })
    return uuids
  }, [activeGroupIds, groupHubs])

  const mergedGroupFilters = useMemo(() => {
    if (activeGroupFilters.size === 0) return null
    const merged = {}
    activeGroupFilters.forEach(f => {
      if (f.connector_filter) merged.connector_filter = f.connector_filter
      if (f.operator_filter)  merged.operator_filter  = f.operator_filter
      if (f.min_kw   != null) merged.min_kw   = f.min_kw
      if (f.max_kw   != null) merged.max_kw   = f.max_kw
      if (f.min_evses != null) merged.min_evses = f.min_evses
      if (f.max_evses != null) merged.max_evses = f.max_evses
      if (f.min_util  != null) merged.min_util  = f.min_util
      if (f.max_util  != null) merged.max_util  = f.max_util
    })
    return Object.keys(merged).length ? merged : null
  }, [activeGroupFilters])

  const groupFiltersActive = activeGroupIds.size > 0 && mergedGroupFilters !== null

  const loadGroups = useCallback(async () => {
    try {
      const data = await authFetch('/api/groups').then(r => r.json())
      setGroups(data)
      groupsRef.current = data
    } catch { /* ignore */ }
  }, [])

  const toggleGroup = useCallback(async (id) => {
    setActiveGroupIds(prev => {
      const isOn = prev.has(id)
      const next = new Set(prev)
      isOn ? next.delete(id) : next.add(id)

      // Update activeGroupFilters in sync with toggle
      setActiveGroupFilters(m => {
        const n = new Map(m)
        if (isOn) {
          n.delete(id)
        } else {
          const grp = groupsRef.current.find(g => g.id === id)
          if (grp) {
            const hasAny = grp.connector_filter || grp.operator_filter ||
              grp.min_kw != null || grp.max_kw != null ||
              grp.min_evses != null || grp.max_evses != null ||
              grp.min_util != null || grp.max_util != null
            if (hasAny) n.set(id, grp)
          }
        }
        return n
      })

      return next
    })
    // Always re-fetch so newly added hubs are picked up
    try {
      const uuids = await authFetch(`/api/groups/${id}/hubs`).then(r => r.json())
      setGroupHubs(m => { const n = new Map(m); n.set(id, uuids); return n })
    } catch { /* ignore */ }
  }, [])

  const clearGroups = useCallback(() => {
    setActiveGroupIds(new Set())
    setActiveGroupFilters(new Map())
  }, [])

  const [assigningGroupId, setAssigningGroupId] = useState(null)

  const toggleAssigningGroup = useCallback((id) => {
    setAssigningGroupId(prev => prev === id ? null : id)
    // Pre-load hub UUIDs if not yet cached
    setGroupHubs(prev => {
      if (prev.has(id)) return prev
      authFetch(`/api/groups/${id}/hubs`).then(r => r.json()).then(uuids => {
        setGroupHubs(m => { const n = new Map(m); n.set(id, uuids); return n })
      }).catch(() => {})
      return prev
    })
  }, [])

  const toggleHubInGroup = useCallback(async (groupId, hubUuid) => {
    const inGroup = (groupHubs.get(groupId) || []).includes(hubUuid)
    try {
      if (inGroup) {
        await authFetch(`/api/groups/${groupId}/hubs/${hubUuid}`, { method: 'DELETE' })
        setGroupHubs(m => {
          const n = new Map(m)
          n.set(groupId, (m.get(groupId) || []).filter(u => u !== hubUuid))
          return n
        })
      } else {
        await authFetch(`/api/groups/${groupId}/hubs`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ hub_uuids: [hubUuid] }),
        })
        setGroupHubs(m => {
          const n = new Map(m)
          n.set(groupId, [...(m.get(groupId) || []), hubUuid])
          return n
        })
      }
      loadGroups() // refresh hub_count in sidebar
    } catch { /* ignore */ }
  }, [groupHubs, loadGroups])

  const clearFilters = useCallback(() => {
    setSearch('')
    setMinEvses('')
    setMaxEvses('')
    setMinUtil('')
    setMaxUtil('')
    setMinKw('')
    setMaxKw('')
    setConnectorFilter('all')
    setOperatorFilter(new Set())
    setDateRange({ start: null, end: null })
  }, [])

  const toggleOperator = useCallback((op) => {
    setOperatorFilter(prev => {
      const next = new Set(prev)
      next.has(op) ? next.delete(op) : next.add(op)
      return next
    })
  }, [])

  const clearOperators = useCallback(() => {
    setOperatorFilter(new Set())
  }, [])

  /** Returns query-string suffix for API calls that support date range, e.g. "&start_dt=...&end_dt=..." */
  const apiDateParams = useCallback(() => {
    if (dateRange.start && dateRange.end) {
      return `&start_dt=${encodeURIComponent(dateRange.start.toISOString())}&end_dt=${encodeURIComponent(dateRange.end.toISOString())}`
    }
    return ''
  }, [dateRange])

  /** Returns query-string suffix for analytics endpoints (date range + operator/connector/kW/groups) */
  const analyticsParams = useCallback(() => {
    const parts = []
    if (dateRange.start && dateRange.end) {
      parts.push(`start_dt=${encodeURIComponent(dateRange.start.toISOString())}`)
      parts.push(`end_dt=${encodeURIComponent(dateRange.end.toISOString())}`)
    }
    if (activeGroupIds.size > 0) {
      activeGroupIds.forEach(id => parts.push(`group_id=${id}`))
    } else {
      operatorFilter.forEach(op => parts.push(`operator=${encodeURIComponent(op)}`))
      if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    }
    const rMinKw    = groupFiltersActive ? mergedGroupFilters?.min_kw    : minKw
    const rMaxKw    = groupFiltersActive ? mergedGroupFilters?.max_kw    : maxKw
    const rMinEvses = groupFiltersActive ? mergedGroupFilters?.min_evses : minEvses
    const rMaxEvses = groupFiltersActive ? mergedGroupFilters?.max_evses : maxEvses
    if (rMinKw    != null && rMinKw    !== '') parts.push(`min_kw=${encodeURIComponent(rMinKw)}`)
    if (rMaxKw    != null && rMaxKw    !== '') parts.push(`max_kw=${encodeURIComponent(rMaxKw)}`)
    if (rMinEvses != null && rMinEvses !== '') parts.push(`min_evses=${encodeURIComponent(rMinEvses)}`)
    if (rMaxEvses != null && rMaxEvses !== '') parts.push(`max_evses=${encodeURIComponent(rMaxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds, groupFiltersActive, mergedGroupFilters])

  /** Query-string suffix for operator/connector/kW/groups only (no date range). */
  const filterOnlyParams = useCallback(() => {
    const parts = []
    if (activeGroupIds.size > 0) {
      activeGroupIds.forEach(id => parts.push(`group_id=${id}`))
    } else {
      operatorFilter.forEach(op => parts.push(`operator=${encodeURIComponent(op)}`))
      if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    }
    const rMinKw    = groupFiltersActive ? mergedGroupFilters?.min_kw    : minKw
    const rMaxKw    = groupFiltersActive ? mergedGroupFilters?.max_kw    : maxKw
    const rMinEvses = groupFiltersActive ? mergedGroupFilters?.min_evses : minEvses
    const rMaxEvses = groupFiltersActive ? mergedGroupFilters?.max_evses : maxEvses
    if (rMinKw    != null && rMinKw    !== '') parts.push(`min_kw=${encodeURIComponent(rMinKw)}`)
    if (rMaxKw    != null && rMaxKw    !== '') parts.push(`max_kw=${encodeURIComponent(rMaxKw)}`)
    if (rMinEvses != null && rMinEvses !== '') parts.push(`min_evses=${encodeURIComponent(rMinEvses)}`)
    if (rMaxEvses != null && rMaxEvses !== '') parts.push(`max_evses=${encodeURIComponent(rMaxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds, groupFiltersActive, mergedGroupFilters])

  /** Returns URL query params object for /api/hubs date range filtering */
  const hubsUrl = useCallback(() => {
    if (dateRange.start && dateRange.end) {
      return `/api/hubs?start_dt=${encodeURIComponent(dateRange.start.toISOString())}&end_dt=${encodeURIComponent(dateRange.end.toISOString())}`
    }
    return '/api/hubs'
  }, [dateRange])

  /** Returns URL for /api/visits with date range + all toolbar filters */
  const visitsUrl = useCallback(() => {
    const parts = []
    if (dateRange.start && dateRange.end) {
      parts.push(`start_dt=${encodeURIComponent(dateRange.start.toISOString())}`)
      parts.push(`end_dt=${encodeURIComponent(dateRange.end.toISOString())}`)
    }
    if (activeGroupIds.size > 0) {
      activeGroupIds.forEach(id => parts.push(`group_id=${id}`))
    } else {
      operatorFilter.forEach(op => parts.push(`operator=${encodeURIComponent(op)}`))
      if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    }
    const rMinKw    = groupFiltersActive ? mergedGroupFilters?.min_kw    : minKw
    const rMaxKw    = groupFiltersActive ? mergedGroupFilters?.max_kw    : maxKw
    const rMinEvses = groupFiltersActive ? mergedGroupFilters?.min_evses : minEvses
    const rMaxEvses = groupFiltersActive ? mergedGroupFilters?.max_evses : maxEvses
    if (rMinKw    != null && rMinKw    !== '') parts.push(`min_kw=${encodeURIComponent(rMinKw)}`)
    if (rMaxKw    != null && rMaxKw    !== '') parts.push(`max_kw=${encodeURIComponent(rMaxKw)}`)
    if (rMinEvses != null && rMinEvses !== '') parts.push(`min_evses=${encodeURIComponent(rMinEvses)}`)
    if (rMaxEvses != null && rMaxEvses !== '') parts.push(`max_evses=${encodeURIComponent(rMaxEvses)}`)
    return `/api/visits${parts.length ? '?' + parts.join('&') : ''}`
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds, groupFiltersActive, mergedGroupFilters])

  return (
    <FilterContext.Provider value={{
      search, setSearch,
      minEvses, setMinEvses,
      maxEvses, setMaxEvses,
      minUtil, setMinUtil,
      maxUtil, setMaxUtil,
      minKw, setMinKw,
      maxKw, setMaxKw,
      connectorFilter, setConnectorFilter,
      operatorFilter,
      dateRange, setDateRange,
      availableOperators, setAvailableOperators,
      clearFilters,
      toggleOperator,
      clearOperators,
      apiDateParams,
      analyticsParams,
      filterOnlyParams,
      hubsUrl,
      visitsUrl,
      groups, loadGroups,
      activeGroupIds, toggleGroup, clearGroups,
      activeGroupUuids, groupHubs,
      activeGroupFilters, mergedGroupFilters, groupFiltersActive,
      assigningGroupId, toggleAssigningGroup, toggleHubInGroup,
    }}>
      {children}
    </FilterContext.Provider>
  )
}

export function useFilters() {
  return useContext(FilterContext)
}

export function applyFilters(hubs, { search, minEvses, maxEvses, minUtil, maxUtil, minKw, maxKw, connectorFilter, operatorFilter, activeGroupIds, activeGroupUuids, mergedGroupFilters, groupFiltersActive }) {
  return hubs.filter(h => {
    if (search) {
      const q = search.toLowerCase()
      if (!h.uuid.toLowerCase().includes(q) &&
          !(h.hub_name || '').toLowerCase().includes(q) &&
          !(h.operator || '').toLowerCase().includes(q) &&
          !(h.connector_types || []).join(' ').toLowerCase().includes(q))
        return false
    }
    const gf = groupFiltersActive && mergedGroupFilters ? mergedGroupFilters : null
    const rMinEvses = gf ? gf.min_evses : minEvses
    const rMaxEvses = gf ? gf.max_evses : maxEvses
    const rMinUtil  = gf ? gf.min_util  : minUtil
    const rMaxUtil  = gf ? gf.max_util  : maxUtil
    const rMinKw    = gf ? gf.min_kw    : minKw
    const rMaxKw    = gf ? gf.max_kw    : maxKw
    if (rMinEvses != null && rMinEvses !== '' && h.total_evses < parseInt(rMinEvses)) return false
    if (rMaxEvses != null && rMaxEvses !== '' && h.total_evses > parseInt(rMaxEvses)) return false
    if (rMinUtil  != null && rMinUtil  !== '' && (h.utilisation_pct ?? 0) < parseFloat(rMinUtil)) return false
    if (rMaxUtil  != null && rMaxUtil  !== '' && (h.utilisation_pct ?? 0) > parseFloat(rMaxUtil)) return false
    if (rMinKw    != null && rMinKw    !== '' && (h.max_power_kw ?? 0) < parseFloat(rMinKw)) return false
    if (rMaxKw    != null && rMaxKw    !== '' && (h.max_power_kw ?? 0) > parseFloat(rMaxKw)) return false
    if (activeGroupIds?.size > 0) {
      if (!activeGroupUuids?.has(h.uuid)) return false
      if (gf?.connector_filter && !(h.connector_types || []).includes(gf.connector_filter)) return false
      if (gf?.operator_filter) {
        try {
          const ops = JSON.parse(gf.operator_filter)
          if (ops.length > 0 && !ops.map(o => o.toLowerCase()).includes((h.operator || '').toLowerCase())) return false
        } catch { /* ignore malformed */ }
      }
    } else {
      if (connectorFilter !== 'all' && !(h.connector_types || []).includes(connectorFilter)) return false
      if (operatorFilter.size > 0 && !operatorFilter.has(h.operator || '')) return false
    }
    return true
  })
}
