import { createContext, useContext, useState, useCallback } from 'react'

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
  const [operatorFilter, setOperatorFilter] = useState('all')
  const [dateRange, setDateRange] = useState({ start: null, end: null })
  const [availableOperators, setAvailableOperators] = useState([])

  const clearFilters = useCallback(() => {
    setSearch('')
    setMinEvses('')
    setMaxEvses('')
    setMinUtil('')
    setMaxUtil('')
    setMinKw('')
    setMaxKw('')
    setConnectorFilter('all')
    setOperatorFilter('all')
    setDateRange({ start: null, end: null })
  }, [])

  /** Returns query-string suffix for API calls that support date range, e.g. "&start_dt=...&end_dt=..." */
  const apiDateParams = useCallback(() => {
    if (dateRange.start && dateRange.end) {
      return `&start_dt=${encodeURIComponent(dateRange.start.toISOString())}&end_dt=${encodeURIComponent(dateRange.end.toISOString())}`
    }
    return ''
  }, [dateRange])

  /** Returns query-string suffix for analytics endpoints (date range + operator/connector/kW) */
  const analyticsParams = useCallback(() => {
    const parts = []
    if (dateRange.start && dateRange.end) {
      parts.push(`start_dt=${encodeURIComponent(dateRange.start.toISOString())}`)
      parts.push(`end_dt=${encodeURIComponent(dateRange.end.toISOString())}`)
    }
    if (operatorFilter !== 'all') parts.push(`operator=${encodeURIComponent(operatorFilter)}`)
    if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses])

  /** Query-string suffix for operator/connector/kW only (no date range). */
  const filterOnlyParams = useCallback(() => {
    const parts = []
    if (operatorFilter !== 'all') parts.push(`operator=${encodeURIComponent(operatorFilter)}`)
    if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return parts.length ? '&' + parts.join('&') : ''
  }, [operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses])

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
    if (operatorFilter !== 'all') parts.push(`operator=${encodeURIComponent(operatorFilter)}`)
    if (connectorFilter !== 'all') parts.push(`connector=${encodeURIComponent(connectorFilter)}`)
    if (minKw) parts.push(`min_kw=${encodeURIComponent(minKw)}`)
    if (maxKw) parts.push(`max_kw=${encodeURIComponent(maxKw)}`)
    if (minEvses) parts.push(`min_evses=${encodeURIComponent(minEvses)}`)
    if (maxEvses) parts.push(`max_evses=${encodeURIComponent(maxEvses)}`)
    return `/api/visits${parts.length ? '?' + parts.join('&') : ''}`
  }, [dateRange, operatorFilter, connectorFilter, minKw, maxKw, minEvses, maxEvses])

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
      operatorFilter, setOperatorFilter,
      dateRange, setDateRange,
      availableOperators, setAvailableOperators,
      clearFilters,
      apiDateParams,
      analyticsParams,
      filterOnlyParams,
      hubsUrl,
      visitsUrl,
    }}>
      {children}
    </FilterContext.Provider>
  )
}

export function useFilters() {
  return useContext(FilterContext)
}

export function applyFilters(hubs, { search, minEvses, maxEvses, minUtil, maxUtil, minKw, maxKw, connectorFilter, operatorFilter }) {
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
    if (connectorFilter !== 'all' && !(h.connector_types || []).includes(connectorFilter)) return false
    if (operatorFilter !== 'all' && (h.operator || '').toLowerCase() !== operatorFilter.toLowerCase())
      return false
    return true
  })
}
