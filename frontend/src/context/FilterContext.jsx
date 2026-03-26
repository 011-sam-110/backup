import { createContext, useContext, useState, useCallback, useMemo } from 'react'
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

  const activeGroupUuids = useMemo(() => {
    if (activeGroupIds.size === 0) return new Set()
    const uuids = new Set()
    activeGroupIds.forEach(id => {
      const list = groupHubs.get(id) || []
      list.forEach(u => uuids.add(u))
    })
    return uuids
  }, [activeGroupIds, groupHubs])

  const loadGroups = useCallback(async () => {
    try {
      const data = await authFetch('/api/groups').then(r => r.json())
      setGroups(data)
    } catch { /* ignore */ }
  }, [])

  const toggleGroup = useCallback(async (id) => {
    setActiveGroupIds(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
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
  }, [])

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
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds])

  /** Query-string suffix for operator/connector/kW/groups only (no date range). */
  const filterOnlyParams = useCallback(() => {
    const parts = []
    if (activeGroupIds.size > 0) {
      activeGroupIds.forEach(id => parts.push(`group_id=${id}`))
    } else {
      operatorFilter.forEach(op => parts.push(`operator=${encodeURIComponent(op)}`))
      if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    }
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds])

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
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return `/api/visits${parts.length ? '?' + parts.join('&') : ''}`
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses, activeGroupIds])

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
    }}>
      {children}
    </FilterContext.Provider>
  )
}

export function useFilters() {
  return useContext(FilterContext)
}

export function applyFilters(hubs, { search, minEvses, maxEvses, minUtil, maxUtil, minKw, maxKw, connectorFilter, operatorFilter, activeGroupIds, activeGroupUuids }) {
  return hubs.filter(h => {
    if (search) {
      const q = search.toLowerCase()
      if (!h.uuid.toLowerCase().includes(q) &&
          !(h.hub_name || '').toLowerCase().includes(q) &&
          !(h.operator || '').toLowerCase().includes(q) &&
          !(h.connector_types || []).join(' ').toLowerCase().includes(q))
        return false
    }
    if (minEvses && h.total_evses < parseInt(minEvses)) return false
    if (maxEvses && h.total_evses > parseInt(maxEvses)) return false
    if (minUtil  && (h.utilisation_pct ?? 0) < parseFloat(minUtil)) return false
    if (maxUtil  && (h.utilisation_pct ?? 0) > parseFloat(maxUtil)) return false
    if (minKw    && (h.max_power_kw ?? 0) < parseFloat(minKw)) return false
    if (maxKw    && (h.max_power_kw ?? 0) > parseFloat(maxKw)) return false
    if (activeGroupIds?.size > 0) {
      if (!activeGroupUuids?.has(h.uuid)) return false
    } else {
      if (connectorFilter !== 'all' && !(h.connector_types || []).includes(connectorFilter)) return false
      if (operatorFilter.size > 0 && !operatorFilter.has(h.operator || '')) return false
    }
    return true
  })
}
