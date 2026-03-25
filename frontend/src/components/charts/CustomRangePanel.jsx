import { useState, useEffect, useRef } from 'react'
import DateRangePicker from '../DateRangePicker'
import { useFilters } from '../../context/FilterContext'

const HOUR_OPTS = Array.from({ length: 24 }, (_, h) => ({
  value: h,
  label: h === 0 ? '12am' : h === 12 ? '12pm' : h < 12 ? `${h}am` : `${h - 12}pm`,
}))

function defaultRange() {
  const end = new Date()
  end.setHours(23, 59, 59, 999)
  const start = new Date()
  start.setDate(start.getDate() - 29)
  start.setHours(0, 0, 0, 0)
  return { start, end }
}

export default function CustomRangePanel({ title = 'Custom Range', buildUrl, renderChart, renderStat }) {
  const [localRange, setLocalRange] = useState(defaultRange)
  const [startHour, setStartHour] = useState(0)
  const [endHour, setEndHour] = useState(23)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const abortRef = useRef(null)

  const { operatorFilter, connectorFilter, minKw, maxKw, filterOnlyParams } = useFilters()

  useEffect(() => {
    if (abortRef.current) abortRef.current.abort()
    const controller = new AbortController()
    abortRef.current = controller

    const sh = Math.min(startHour, endHour)
    const eh = Math.max(startHour, endHour)
    const fp = filterOnlyParams()
    const url = buildUrl(localRange, sh, eh, fp)
    if (!url) {
      setData(null)
      setLoading(false)
      return
    }

    setLoading(true)
    fetch(url, { signal: controller.signal })
      .then(r => r.json())
      .then(d => { if (!controller.signal.aborted) setData(d) })
      .catch(() => {})
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
  }, [localRange, startHour, endHour, operatorFilter, connectorFilter, minKw, maxKw]) // eslint-disable-line

  const stat = !loading && data && renderStat ? renderStat(data) : null

  return (
    <div className="companion-panel">
      <div className="companion-title">{title}</div>

      <div className="companion-controls">
        <div style={{ flex: '1 1 180px', maxWidth: 260 }}>
          <DateRangePicker value={localRange} onChange={setLocalRange} />
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>From</span>
          <select
            className="filter-input"
            value={startHour}
            onChange={e => setStartHour(Number(e.target.value))}
          >
            {HOUR_OPTS.map(o => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>To</span>
          <select
            className="filter-input"
            value={endHour}
            onChange={e => setEndHour(Number(e.target.value))}
          >
            {HOUR_OPTS.map(o => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>

        {stat && (
          <div style={{ fontSize: 12, color: 'var(--text-muted)', marginLeft: 'auto' }}>
            {stat}
          </div>
        )}
      </div>

      {loading ? (
        <div className="loading" style={{ padding: '20px 0' }}>Loading…</div>
      ) : (
        renderChart(data)
      )}
    </div>
  )
}
