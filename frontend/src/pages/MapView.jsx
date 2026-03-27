import { useState, useEffect, useCallback, useMemo } from 'react'
import { MapContainer, TileLayer, Marker, Tooltip, useMap } from 'react-leaflet'
import L from 'leaflet'
import HubDetailModal from '../components/HubDetailModal'
import { MAP_BANDS, groupColor } from '../utils/status'
import PageLoader from '../components/PageLoader'
import { useFilters, applyFilters } from '../context/FilterContext'
import { authFetch } from '../context/AuthContext'

function bandColor(pct) {
  const band = MAP_BANDS.find(b => pct >= b.min && pct < b.max)
  return band ? band.color : MAP_BANDS[0].color
}

const _pinCache = new Map()
function makePin(color) {
  if (!_pinCache.has(color)) {
    _pinCache.set(color, L.divIcon({
      className: '',
      iconSize: [20, 28],
      iconAnchor: [10, 28],
      popupAnchor: [0, -28],
      html: `<svg xmlns="http://www.w3.org/2000/svg" width="20" height="28" viewBox="0 0 20 28">
        <path d="M10 0C4.477 0 0 4.477 0 10c0 7.5 10 18 10 18s10-10.5 10-18C20 4.477 15.523 0 10 0z"
          fill="${color}" stroke="rgba(0,0,0,0.35)" stroke-width="1.2"/>
        <circle cx="10" cy="10" r="3.5" fill="rgba(255,255,255,0.55)"/>
      </svg>`,
    }))
  }
  return _pinCache.get(color)
}

function FlyTo({ target }) {
  const map = useMap()
  useEffect(() => {
    if (target) map.flyTo(target, 13, { duration: 1.2 })
  }, [target, map])
  return null
}

const REFRESH_MS = 60_000

export default function MapView() {
  const [hubs, setHubs] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedHub, setSelectedHub] = useState(null)
  const [flyTarget, setFlyTarget] = useState(null)
  const [mapStyle, setMapStyle] = useState('satellite')

  const filters = useFilters()
  const { hubsUrl, setAvailableOperators, dateRange, activeGroupIds, groupHubs,
          assigningGroupId, toggleAssigningGroup, toggleHubInGroup, groups } = filters

  const load = useCallback((showSpinner = false) => {
    if (showSpinner) setLoading(true)
    authFetch(hubsUrl())
      .then(r => r.json())
      .then(d => {
        setHubs(d)
        const ops = [...new Set(d.map(h => h.operator).filter(Boolean))].sort()
        setAvailableOperators(ops)
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [hubsUrl, setAvailableOperators])

  useEffect(() => {
    load(true)
    const id = setInterval(() => load(false), REFRESH_MS)
    return () => clearInterval(id)
  }, [load])

  // Re-fetch when date range changes
  useEffect(() => {
    load(false)
  }, [dateRange]) // eslint-disable-line

  // Build uuid→color map for active groups + assigning group so pins show their group color
  const groupColorMap = useMemo(() => {
    const m = new Map()
    const ids = assigningGroupId ? new Set([...activeGroupIds, assigningGroupId]) : activeGroupIds
    ids.forEach(id => {
      const color = groupColor(id)
      ;(groupHubs.get(id) || []).forEach(uuid => m.set(uuid, color))
    })
    return m
  }, [activeGroupIds, groupHubs, assigningGroupId])

  if (loading) return <PageLoader text="Loading map data…" />

  const filtered = applyFilters(hubs, filters)
  const hubLabel = h => h.hub_name || h.uuid

  const assigningGroup = assigningGroupId ? groups.find(g => g.id === assigningGroupId) : null

  return (
    <>
      <div className="map-page" style={{ position: 'relative' }}>
        {assigningGroupId && (
          <div style={{
            position: 'absolute', top: 12, left: '50%', transform: 'translateX(-50%)',
            zIndex: 1000, background: 'var(--surface)', border: `1px solid ${groupColor(assigningGroupId)}`,
            borderRadius: 10, padding: '8px 16px', display: 'flex', alignItems: 'center',
            gap: 12, boxShadow: '0 4px 16px rgba(0,0,0,0.4)', fontSize: 13, whiteSpace: 'nowrap',
          }}>
            <span style={{ color: groupColor(assigningGroupId), fontWeight: 700 }}>◎</span>
            <span>Click pins to add/remove from <strong>{assigningGroup?.name}</strong></span>
            <span style={{ color: 'var(--text-dim)', fontSize: 11 }}>
              {(groupHubs.get(assigningGroupId) || []).length} hubs
            </span>
            <button
              onClick={() => toggleAssigningGroup(assigningGroupId)}
              style={{
                background: groupColor(assigningGroupId), color: '#fff', border: 'none',
                borderRadius: 6, padding: '3px 10px', fontSize: 12, cursor: 'pointer',
              }}
            >Done</button>
          </div>
        )}
        <div style={{ position: 'relative', flex: 1, minWidth: 0 }}>
          <div className="map-container" style={{ position: 'static', flex: 'unset', height: '100%' }}>
          <MapContainer
            center={[52.5, -1.5]}
            zoom={6}
            style={{ height: '100%', width: '100%', background: '#0f1117' }}
            zoomControl={true}
          >
            {mapStyle === 'satellite' ? (
              <TileLayer
                url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                attribution='Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics'
                maxZoom={19}
              />
            ) : (
              <TileLayer
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              />
            )}
            {flyTarget && <FlyTo target={flyTarget} />}
            {filtered.map(hub => (
              <Marker
                key={hub.uuid}
                position={[hub.latitude, hub.longitude]}
                icon={makePin(groupColorMap.get(hub.uuid) ?? bandColor(hub.utilisation_pct ?? 0))}
                eventHandlers={{
                  click: () => {
                    if (assigningGroupId) {
                      toggleHubInGroup(assigningGroupId, hub.uuid)
                    } else {
                      setSelectedHub(hub)
                    }
                  }
                }}
              >
                <Tooltip direction="top" offset={[0, -28]} opacity={1}>
                  <div style={{ fontFamily: 'sans-serif', fontSize: 12, lineHeight: 1.4 }}>
                    <div style={{ fontWeight: 600 }}>{hubLabel(hub)}</div>
                    <div style={{ color: bandColor(hub.utilisation_pct ?? 0) }}>
                      {hub.utilisation_pct ?? 0}% utilised
                    </div>
                  </div>
                </Tooltip>
              </Marker>
            ))}
          </MapContainer>
          </div>
          <button
            onClick={() => setMapStyle(s => s === 'satellite' ? 'street' : 'satellite')}
            style={{
              position: 'absolute', bottom: 24, right: 10, zIndex: 1000,
              background: 'var(--surface)', border: '1px solid var(--border)',
              borderRadius: 8, padding: '5px 10px', fontSize: 12,
              cursor: 'pointer', color: 'var(--text)',
              boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
              display: 'flex', alignItems: 'center', gap: 5,
            }}
          >
            {mapStyle === 'satellite' ? '🗺 Street map' : '🛰 Satellite'}
          </button>
        </div>

        {/* Legend sidebar */}
        <div className="map-sidebar">
          <div style={{ padding: '14px 0 10px' }}>
            <div style={{
              fontSize: 10, color: 'var(--text-muted)', marginBottom: 10,
              fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.09em',
              paddingBottom: 6, borderBottom: '1px solid var(--border)',
            }}>
              Utilisation
            </div>
            {MAP_BANDS.map(b => (
              <div key={b.label} style={{ display: 'flex', alignItems: 'center', gap: 9, fontSize: 12, color: 'var(--text)', marginBottom: 6 }}>
                <span style={{ width: 9, height: 9, borderRadius: '50%', background: b.color, display: 'inline-block', flexShrink: 0, boxShadow: `0 0 5px ${b.color}66` }} />
                {b.label}
              </div>
            ))}
          </div>

          <div style={{ borderTop: '1px solid var(--border)', paddingTop: 14, marginTop: 4 }}>
            <div style={{
              fontSize: 10, color: 'var(--text-muted)', marginBottom: 10,
              fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.09em',
            }}>
              Top Hubs · {filtered.length} shown
            </div>
            {filtered
              .filter(h => h.utilisation_pct != null)
              .sort((a, b) => b.utilisation_pct - a.utilisation_pct)
              .slice(0, 10)
              .map((hub, i) => (
                <div
                  key={hub.uuid}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '8px 0', borderBottom: '1px solid var(--border)',
                    cursor: 'pointer', transition: 'opacity 0.15s',
                  }}
                  onMouseOver={e => e.currentTarget.style.opacity = '0.75'}
                  onMouseOut={e => e.currentTarget.style.opacity = '1'}
                  onClick={() => {
                    setFlyTarget([hub.latitude, hub.longitude])
                    setSelectedHub(hub)
                  }}
                >
                  <span style={{ color: 'var(--text-dim)', fontSize: 10, width: 16, flexShrink: 0, fontWeight: 700 }}>{i + 1}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {hubLabel(hub)}
                    </div>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 1 }}>{hub.total_evses} EVSEs · {hub.max_power_kw}kW</div>
                  </div>
                  <span style={{
                    fontSize: 11, fontWeight: 600, color: 'var(--text-muted)',
                  }}>
                    {hub.utilisation_pct ?? 0}%
                  </span>
                </div>
              ))}
          </div>
        </div>
      </div>

      {selectedHub && (
        <HubDetailModal hub={selectedHub} onClose={() => setSelectedHub(null)} />
      )}
    </>
  )
}
