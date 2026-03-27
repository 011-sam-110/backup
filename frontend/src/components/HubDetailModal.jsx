import { useEffect, useState } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import { utilClass } from '../utils/status'
import { authFetch } from '../context/AuthContext'
import GroupPicker from './GroupPicker'

function fmtTime(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

const CONNECTOR_SHORT = {
  IEC_62196_T2_COMBO: 'CCS2',
  IEC_62196_T2: 'Type 2',
  CHADEMO: 'CHAdeMO',
}

const STATUS_CONFIG = {
  AVAILABLE:   { label: 'Available',    color: '#22c55e', bg: 'rgba(34,197,94,0.10)',    icon: '✓' },
  CHARGING:    { label: 'Charging',     color: '#ef4444', bg: 'rgba(239,68,68,0.12)',    icon: '⚡' },
  INOPERATIVE: { label: 'Inoperative',  color: '#f59e0b', bg: 'rgba(245,158,11,0.10)',  icon: '⚠' },
  OUTOFORDER:  { label: 'Out of Order', color: '#ef4444', bg: 'rgba(239,68,68,0.08)',   icon: '✕' },
  UNKNOWN:     { label: 'Unknown',      color: '#6b7280', bg: 'rgba(107,114,128,0.08)', icon: '?' },
}

function StatRow({ label, value, valueClass = '' }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '7px 0', borderBottom: '1px solid var(--border)' }}>
      <span style={{ color: 'var(--text-muted)', fontSize: 13 }}>{label}</span>
      <span className={valueClass} style={{ fontWeight: 600, fontSize: 13 }}>{value}</span>
    </div>
  )
}

export default function HubDetailModal({ hub, onClose }) {
  const [history, setHistory] = useState([])
  const [histLoading, setHistLoading] = useState(true)
  const [detail, setDetail] = useState(null)
  const [detailLoading, setDetailLoading] = useState(true)
  const [showGroupPicker, setShowGroupPicker] = useState(false)
  const [sessionStart, setSessionStart] = useState(null)

  useEffect(() => {
    authFetch(`/api/hubs/${hub.uuid}/session-start`)
      .then(r => r.json())
      .then(d => setSessionStart(d.session_start || null))
      .catch(() => {})
  }, [hub.uuid])

  useEffect(() => {
    authFetch(`/api/history?hub_uuid=${hub.uuid}&hours=24`)
      .then(r => r.json())
      .then(d => setHistory(d))
      .catch(() => {})
      .finally(() => setHistLoading(false))
  }, [hub.uuid])

  useEffect(() => {
    authFetch(`/api/hubs/${hub.uuid}`)
      .then(r => r.json())
      .then(d => setDetail(d))
      .catch(() => {})
      .finally(() => setDetailLoading(false))
  }, [hub.uuid])

  const pct = hub.utilisation_pct ?? 0
  const mapsUrl = `https://www.google.com/maps?q=${hub.latitude},${hub.longitude}`

  const chartData = history.map(d => ({
    ...d,
    time: fmtTime(d.scraped_at),
  }))

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <div>
            <span className="modal-title">{hub.hub_name || hub.uuid}</span>
            {hub.hub_name && (
              <div className="mono" style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 3 }}>{hub.uuid}</div>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, position: 'relative' }}>
            <button
              onClick={() => setShowGroupPicker(v => !v)}
              style={{
                background: 'var(--accent-dim)', border: '1px solid rgba(0,86,179,0.3)',
                color: 'var(--accent)', borderRadius: 7, padding: '4px 10px',
                fontSize: 12, fontWeight: 600, cursor: 'pointer', fontFamily: 'Inter, inherit',
              }}
            >
              + Group
            </button>
            {showGroupPicker && (
              <GroupPicker hubUuid={hub.uuid} onClose={() => setShowGroupPicker(false)} />
            )}
            <button className="modal-close" onClick={onClose}>×</button>
          </div>
        </div>

        {/* Stats */}
        <div style={{ marginBottom: 20 }}>
          <StatRow
            label="Location"
            value={
              <a href={mapsUrl} target="_blank" rel="noopener noreferrer" className="maps-link">
                {hub.latitude?.toFixed(4)}, {hub.longitude?.toFixed(4)}
              </a>
            }
          />
          <StatRow label="Max Power" value={`${hub.max_power_kw} kW`} />
          <StatRow label="Total EVSEs" value={hub.total_evses} />
          <StatRow label="Connector Types" value={(hub.connector_types || []).join(', ') || '—'} />
          <StatRow label="Charging" value={hub.charging_count ?? '—'} valueClass="green" />
          <StatRow label="Available" value={hub.available_count ?? '—'} />
          <StatRow label="Inoperative" value={hub.inoperative_count ?? '—'} valueClass={hub.inoperative_count > 0 ? 'amber' : ''} />
          <StatRow label="Out of Order" value={hub.out_of_order_count ?? '—'} valueClass={hub.out_of_order_count > 0 ? 'red' : ''} />
          <StatRow
            label="Utilisation"
            value={`${pct}%`}
            valueClass={utilClass(pct)}
          />
          <StatRow label="Last Updated" value={fmtTime(hub.scraped_at)} />
          {hub.operator && <StatRow label="Operator" value={hub.operator} />}
          {(hub.address || hub.city) && (
            <StatRow
              label="Address"
              value={[hub.address, hub.city, hub.postal_code].filter(Boolean).join(', ')}
            />
          )}
          {hub.user_rating != null && (
            <StatRow
              label="Rating"
              value={`★ ${hub.user_rating.toFixed(1)} (${hub.user_rating_count ?? 0} reviews)`}
            />
          )}
          {hub.is_24_7 === 1 && <StatRow label="Hours" value="24 / 7" />}
          {hub.pricing?.length > 0 && (
            <StatRow label="Pricing" value={hub.pricing.join(' · ')} />
          )}
          {hub.payment_methods?.length > 0 && (
            <StatRow
              label="Payments"
              value={hub.payment_methods.map(m => m.replace(/_/g, ' ')).join(', ')}
            />
          )}
        </div>

        {/* Mini history chart */}
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 10, color: 'var(--text-muted)' }}>
          24h Utilisation History
        </div>
        {histLoading ? (
          <div className="loading" style={{ padding: '20px 0' }}>Loading...</div>
        ) : chartData.length < 2 ? (
          <div className="empty" style={{ padding: '20px 0' }}>Not enough history yet.</div>
        ) : (
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={chartData} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                dataKey="time"
                tick={{ fill: '#6B7280', fontSize: 10, fontFamily: 'Inter, sans-serif' }}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                domain={[0, 100]}
                tick={{ fill: '#6B7280', fontSize: 10, fontFamily: 'Inter, sans-serif' }}
                axisLine={false}
                tickLine={false}
                tickFormatter={v => `${v}%`}
                width={36}
              />
              <Tooltip
                contentStyle={{ background: 'var(--surface)', border: '1px solid var(--border-light)', borderRadius: 8, fontSize: 12, fontFamily: 'Inter, sans-serif' }}
                formatter={v => [`${v}%`, 'Utilisation']}
                labelStyle={{ color: 'var(--text-muted)' }}
              />
              <Line
                type="monotone"
                dataKey="avg_utilisation_pct"
                stroke="#2563EB"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}

        {detailLoading ? (
          <div className="loading" style={{ padding: '20px 0' }}>Loading chargepoints…</div>
        ) : detail?.devices_raw_loc?.length > 0 && (() => {
          // Layer 1: device-uuid → evse-uuid map
          const liveMap = {}
          // Layer 2: flat evse-uuid map (handles device UUID mismatch between APIs)
          const liveByEvse = {}
          // Layer 3: positional map [devIdx][evseIdx] (handles all UUID mismatches)
          const liveByIndex = []
          // Layer 4: evse-uuid → network_updated_at timestamp
          const liveUpdatedAt = {}
          for (const d of detail.latest_devices_status || []) {
            liveMap[d.device_uuid] = liveMap[d.device_uuid] || {}
            const evseStatuses = []
            for (const e of d.evses || []) {
              const s = (e.network_status || 'UNKNOWN').toUpperCase()
              liveMap[d.device_uuid][e.evse_uuid] = s
              liveByEvse[e.evse_uuid] = s
              if (e.network_updated_at) liveUpdatedAt[e.evse_uuid] = e.network_updated_at
              evseStatuses.push(s)
            }
            liveByIndex.push(evseStatuses)
          }
          function fmtDuration(isoTs) {
            if (!isoTs) return null
            const mins = Math.floor((Date.now() - new Date(isoTs).getTime()) / 60000)
            if (mins < 1) return '<1m'
            if (mins < 60) return `${mins}m`
            const h = Math.floor(mins / 60), m = mins % 60
            return m > 0 ? `${h}h ${m}m` : `${h}h`
          }
          function getStatus(devIdx, evseIdx, devUuid, evseUuid) {
            return (liveMap[devUuid] && liveMap[devUuid][evseUuid])
              || liveByEvse[evseUuid]
              || (liveByIndex[devIdx] && liveByIndex[devIdx][evseIdx])
              || 'UNKNOWN'
          }

          const cards = detail.devices_raw_loc.flatMap((dev, devIdx) => {
            const pd = dev.payment_details || {}
            return (dev.evses || []).map((evse, evseIdx) => {
              const status = getStatus(devIdx, evseIdx, dev.uuid, evse.uuid).toUpperCase().replace(/ /g, '')
              const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.UNKNOWN
              const kw = Math.round((evse.connectors?.[0]?.max_electric_power || 0) / 1000)
              const connector = CONNECTOR_SHORT[evse.connectors?.[0]?.standard] || evse.connectors?.[0]?.standard || '—'
              const updatedAt = liveUpdatedAt[evse.uuid] || null
              return { devUuid: dev.uuid, evseUuid: evse.uuid, ref: dev.physical_reference, status, cfg, kw, connector, pd, updatedAt }
            })
          })

          const counts = {}
          for (const c of cards) counts[c.status] = (counts[c.status] || 0) + 1
          const allUnknown = cards.length > 0 && cards.every(c => c.status === 'UNKNOWN')

          return (
            <div style={{ marginTop: 20 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-muted)' }}>
                  Chargepoints ({cards.length})
                </div>
                {Object.entries(counts).map(([s, n]) => {
                  const cfg = STATUS_CONFIG[s] || STATUS_CONFIG.UNKNOWN
                  return (
                    <span key={s} style={{ fontSize: 11, color: cfg.color, fontWeight: 600 }}>
                      {cfg.icon} {n} {cfg.label}
                    </span>
                  )
                })}
              </div>
              {allUnknown && (
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 10, fontStyle: 'italic' }}>
                  Live per-chargepoint status not available from this operator.
                </div>
              )}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(110px, 1fr))', gap: 8 }}>
                {cards.map(c => (
                  <div key={c.evseUuid} style={{
                    background: c.cfg.bg,
                    border: `1px solid ${c.cfg.color}40`,
                    borderRadius: 8, padding: '10px 10px 8px',
                    display: 'flex', flexDirection: 'column', gap: 4,
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <span style={{ fontWeight: 700, fontSize: 13 }}>#{c.ref}</span>
                      {c.kw > 0 && <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{c.kw}kW</span>}
                    </div>
                    <div style={{ color: c.cfg.color, fontWeight: 600, fontSize: 11 }}>
                      {c.cfg.icon} {c.cfg.label}
                    </div>
                    {c.status === 'CHARGING' && fmtDuration(sessionStart || c.updatedAt) && (
                      <div style={{ color: c.cfg.color, fontSize: 10, opacity: 0.85 }}>
                        ⏱ {fmtDuration(sessionStart || c.updatedAt)}
                      </div>
                    )}
                    <div style={{ color: 'var(--text-muted)', fontSize: 10 }}>{c.connector}</div>
                    {c.pd.pricing && (
                      <div style={{ color: '#f59e0b', fontSize: 10, marginTop: 2 }}>{c.pd.pricing}</div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )
        })()}
      </div>
    </div>
  )
}
