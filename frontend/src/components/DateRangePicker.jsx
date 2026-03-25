import { useState, useEffect, useRef } from 'react'

// ─── Helpers ───────────────────────────────────────────────────────────────

function sod(d) { return new Date(d.getFullYear(), d.getMonth(), d.getDate()) }
function eod(d) { return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59, 999) }

function isSameDay(a, b) {
  return a && b &&
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
}

function isInRange(d, start, end) {
  if (!start || !end) return false
  const t = d.getTime()
  return t >= sod(start).getTime() && t <= eod(end).getTime()
}

function formatRange(start, end) {
  if (!start) return 'All time'
  const fmt = d => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
  const fmtShort = d => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
  if (!end || isSameDay(start, end)) return fmt(start)
  if (start.getFullYear() === end.getFullYear()) {
    return `${fmtShort(start)} – ${fmt(end)}`
  }
  return `${fmt(start)} – ${fmt(end)}`
}

function getCalendarDays(year, month) {
  // Grid starting Monday, padded to full weeks
  const first = new Date(year, month, 1)
  const last = new Date(year, month + 1, 0)
  let dow = first.getDay() // 0=Sun
  if (dow === 0) dow = 7   // treat Sun as 7 for Mon-based grid
  const offset = dow - 1   // blanks before day 1

  const days = []
  for (let i = 1 - offset; i <= last.getDate(); i++) {
    days.push(i > 0 ? new Date(year, month, i) : null)
  }
  while (days.length % 7 !== 0) days.push(null)
  return days
}

function today() { return sod(new Date()) }

const PRESETS = [
  { label: 'Today',      fn: () => ({ start: today(), end: eod(new Date()) }) },
  { label: 'Yesterday',  fn: () => { const d = new Date(); d.setDate(d.getDate()-1); return { start: sod(d), end: eod(d) } } },
  { label: 'Last 7 days',fn: () => { const d = new Date(); d.setDate(d.getDate()-6); return { start: sod(d), end: eod(new Date()) } } },
  { label: 'Last 30 days',fn: () => { const d = new Date(); d.setDate(d.getDate()-29); return { start: sod(d), end: eod(new Date()) } } },
  { label: 'Last 90 days',fn: () => { const d = new Date(); d.setDate(d.getDate()-89); return { start: sod(d), end: eod(new Date()) } } },
  { label: 'This month', fn: () => { const n = new Date(); return { start: new Date(n.getFullYear(), n.getMonth(), 1), end: eod(new Date()) } } },
]

const WEEKDAYS = ['M', 'T', 'W', 'T', 'F', 'S', 'S']
const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December']

// ─── Component ─────────────────────────────────────────────────────────────

export default function DateRangePicker({ value, onChange }) {
  const [open, setOpen] = useState(false)
  const [selecting, setSelecting] = useState(false)  // true = picking end date
  const [hover, setHover] = useState(null)
  const [tempStart, setTempStart] = useState(null)
  const [viewYear, setViewYear] = useState(new Date().getFullYear())
  const [viewMonth, setViewMonth] = useState(new Date().getMonth())
  const ref = useRef(null)

  const start = value?.start ?? null
  const end = value?.end ?? null

  // Close on outside click
  useEffect(() => {
    function onDown(e) {
      if (ref.current && !ref.current.contains(e.target)) {
        setOpen(false)
        setSelecting(false)
        setTempStart(null)
      }
    }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])

  function applyPreset(preset) {
    const range = preset.fn()
    onChange(range)
    setSelecting(false)
    setTempStart(null)
    setOpen(false)
  }

  function handleDayClick(day) {
    if (!day) return
    if (!selecting) {
      // First click: set start
      setTempStart(sod(day))
      setSelecting(true)
    } else {
      // Second click: set end
      const s = tempStart
      const e = sod(day)
      if (e >= s) {
        onChange({ start: s, end: eod(day) })
      } else {
        onChange({ start: e, end: eod(new Date(s.getTime())) })
      }
      setSelecting(false)
      setTempStart(null)
      setOpen(false)
    }
  }

  function prevMonth() {
    if (viewMonth === 0) { setViewMonth(11); setViewYear(y => y - 1) }
    else setViewMonth(m => m - 1)
  }
  function nextMonth() {
    if (viewMonth === 11) { setViewMonth(0); setViewYear(y => y + 1) }
    else setViewMonth(m => m + 1)
  }

  function clearRange(e) {
    e.stopPropagation()
    onChange({ start: null, end: null })
    setSelecting(false)
    setTempStart(null)
  }

  const days = getCalendarDays(viewYear, viewMonth)
  const displayStart = selecting ? tempStart : start
  const displayEnd = selecting ? hover : end

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      {/* Trigger */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%',
          background: 'var(--surface)',
          border: '1px solid var(--border-light)',
          borderRadius: 6,
          color: 'var(--text)',
          padding: '6px 10px',
          fontSize: 12,
          cursor: 'pointer',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 6,
          textAlign: 'left',
        }}
      >
        <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          📅 {formatRange(start, end)}
        </span>
        {start && (
          <span
            onClick={clearRange}
            style={{ color: 'var(--text-muted)', fontSize: 14, lineHeight: 1, padding: '0 2px' }}
            title="Clear"
          >×</span>
        )}
      </button>

      {/* Popover */}
      {open && (
        <div style={{
          position: 'fixed',
          zIndex: 9999,
          ...popoverPosition(ref),
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: 10,
          boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
          display: 'flex',
          minWidth: 480,
          overflow: 'hidden',
        }}>
          {/* Presets */}
          <div style={{
            width: 130,
            borderRight: '1px solid var(--border)',
            padding: '8px 0',
            flexShrink: 0,
          }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', padding: '4px 12px 8px' }}>
              Quick select
            </div>
            {PRESETS.map(p => (
              <button
                key={p.label}
                onClick={() => applyPreset(p)}
                style={{
                  display: 'block', width: '100%', textAlign: 'left',
                  padding: '7px 12px', background: 'none', border: 'none',
                  color: 'var(--text)', fontSize: 12, cursor: 'pointer',
                  transition: 'background 0.1s',
                }}
                onMouseOver={e => e.currentTarget.style.background = 'rgba(0,0,0,0.04)'}
                onMouseOut={e => e.currentTarget.style.background = 'none'}
              >
                {p.label}
              </button>
            ))}
            {(start || selecting) && (
              <>
                <div style={{ height: 1, background: 'var(--border)', margin: '8px 0' }} />
                <button
                  onClick={clearRange}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left',
                    padding: '7px 12px', background: 'none', border: 'none',
                    color: 'var(--text-muted)', fontSize: 12, cursor: 'pointer',
                  }}
                  onMouseOver={e => e.currentTarget.style.color = 'var(--text)'}
                  onMouseOut={e => e.currentTarget.style.color = 'var(--text-muted)'}
                >
                  Clear
                </button>
              </>
            )}
          </div>

          {/* Calendar */}
          <div style={{ padding: 16, flex: 1 }}>
            {/* Month nav */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <button onClick={prevMonth} style={navBtnStyle}>‹</button>
              <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text)' }}>
                {MONTHS[viewMonth]} {viewYear}
              </span>
              <button onClick={nextMonth} style={navBtnStyle}>›</button>
            </div>

            {/* Weekday headers */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 2, marginBottom: 4 }}>
              {WEEKDAYS.map((d, i) => (
                <div key={i} style={{ textAlign: 'center', fontSize: 10, color: 'var(--text-muted)', fontWeight: 600, padding: '2px 0' }}>
                  {d}
                </div>
              ))}
            </div>

            {/* Days grid */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 2 }}>
              {days.map((day, i) => {
                if (!day) return <div key={i} />
                const isStart = isSameDay(day, displayStart)
                const isEnd = displayEnd && isSameDay(day, displayEnd)
                const inRange = displayStart && displayEnd && isInRange(day, displayStart, displayEnd)
                const isCurrentMonth = day.getMonth() === viewMonth
                const isToday = isSameDay(day, new Date())
                return (
                  <div
                    key={i}
                    onClick={() => handleDayClick(day)}
                    onMouseEnter={() => selecting && setHover(sod(day))}
                    style={{
                      textAlign: 'center',
                      padding: '5px 0',
                      borderRadius: (isStart || isEnd) ? 4 : inRange ? 0 : 4,
                      background: (isStart || isEnd)
                        ? 'var(--accent)'
                        : inRange
                          ? 'rgba(0,86,179,0.10)'
                          : 'transparent',
                      color: !isCurrentMonth
                        ? 'var(--text-muted)'
                        : (isStart || isEnd)
                          ? '#fff'
                          : 'var(--text)',
                      fontSize: 12,
                      cursor: 'pointer',
                      fontWeight: isToday ? 700 : 400,
                      outline: isToday && !isStart && !isEnd ? '1px solid var(--accent)' : 'none',
                      outlineOffset: -1,
                    }}
                  >
                    {day.getDate()}
                  </div>
                )
              })}
            </div>

            {selecting && (
              <div style={{ marginTop: 10, fontSize: 11, color: 'var(--text-muted)', textAlign: 'center' }}>
                Click to set end date
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const navBtnStyle = {
  background: 'none', border: '1px solid var(--border)',
  color: 'var(--text)', borderRadius: 4, width: 26, height: 26,
  cursor: 'pointer', fontSize: 16, display: 'flex', alignItems: 'center', justifyContent: 'center',
}

function popoverPosition(ref) {
  if (!ref.current) return { top: 40, left: 0 }
  const rect = ref.current.getBoundingClientRect()
  const spaceBelow = window.innerHeight - rect.bottom
  const top = spaceBelow > 320 ? rect.bottom + 4 : rect.top - 4 - 320
  let left = rect.left
  if (left + 480 > window.innerWidth) left = window.innerWidth - 488
  return { top: Math.max(8, top), left: Math.max(8, left) }
}
