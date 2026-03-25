import { useEffect, useRef, useState } from 'react'

const MESSAGES = [
  'Fetching hub data…',
  'Calculating utilisation…',
  'Almost ready…',
]

export default function PageLoader({ text, minMs = 2000, onReady }) {
  const canvasRef = useRef(null)
  const animRef = useRef(null)
  const startRef = useRef(Date.now())
  const [msgIdx, setMsgIdx] = useState(0)
  const [leaving, setLeaving] = useState(false)

  // Cycle loading messages
  useEffect(() => {
    const id = setInterval(() => setMsgIdx(i => (i + 1) % MESSAGES.length), 1500)
    return () => clearInterval(id)
  }, [])

  // Charging arc canvas animation
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    const SIZE = 200
    canvas.width = SIZE
    canvas.height = SIZE
    const cx = SIZE / 2
    const cy = SIZE / 2
    const R = 72
    const SWEEP_FRAMES = 108  // 1.8s at 60fps
    let frame = 0

    // Colour lerp helpers
    function lerp(a, b, t) { return a + (b - a) * t }
    function lerpColor(c1, c2, t) {
      return [lerp(c1[0], c2[0], t), lerp(c1[1], c2[1], t), lerp(c1[2], c2[2], t)]
    }
    function toRgb([r, g, b]) { return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})` }
    const GREEN = [16, 185, 129]
    const AMBER = [245, 158, 11]
    const CYAN  = [34, 211, 238]

    function arcColor(t) {
      if (t < 0.5) return toRgb(lerpColor(GREEN, AMBER, t * 2))
      return toRgb(lerpColor(AMBER, CYAN, (t - 0.5) * 2))
    }

    function draw() {
      ctx.clearRect(0, 0, SIZE, SIZE)
      const progress = (frame % SWEEP_FRAMES) / SWEEP_FRAMES  // 0→1
      const pct = Math.round(progress * 85)

      // Background ring
      ctx.beginPath()
      ctx.arc(cx, cy, R, 0, Math.PI * 2)
      ctx.strokeStyle = 'rgba(255,255,255,0.07)'
      ctx.lineWidth = 8
      ctx.stroke()

      // Sweeping arc — draw as many short segments for colour gradient
      const startAngle = -Math.PI / 2
      const endAngle = startAngle + progress * 2.7  // 0 → ~270°
      const SEGS = 60
      for (let i = 0; i < SEGS; i++) {
        const t0 = i / SEGS
        const t1 = (i + 1) / SEGS
        if (t1 * 2.7 > (progress * 2.7 + 0.01)) break
        const a0 = startAngle + t0 * 2.7
        const a1 = startAngle + t1 * 2.7
        ctx.beginPath()
        ctx.arc(cx, cy, R, a0, a1)
        ctx.strokeStyle = arcColor(t0)
        ctx.lineWidth = 8
        ctx.lineCap = 'round'
        ctx.stroke()
      }

      // Arc end cap glow dot
      const capAngle = startAngle + progress * 2.7
      const dotX = cx + Math.cos(capAngle) * R
      const dotY = cy + Math.sin(capAngle) * R
      ctx.beginPath()
      ctx.arc(dotX, dotY, 5, 0, Math.PI * 2)
      ctx.fillStyle = arcColor(progress)
      ctx.fill()

      // Percentage counter
      ctx.fillStyle = '#dae5f5'
      ctx.font = 'bold 32px Outfit, -apple-system, BlinkMacSystemFont, sans-serif'
      ctx.textAlign = 'center'
      ctx.textBaseline = 'middle'
      ctx.fillText(`${pct}%`, cx, cy - 8)

      // ⚡ label
      ctx.fillStyle = '#22d3ee'
      ctx.font = '16px Outfit, -apple-system, BlinkMacSystemFont, sans-serif'
      ctx.fillText('⚡', cx, cy + 22)

      frame++
      animRef.current = requestAnimationFrame(draw)
    }

    draw()
    return () => cancelAnimationFrame(animRef.current)
  }, [])

  // If parent signals ready, honour minimum display time then fade out
  useEffect(() => {
    if (!onReady) return
    // nothing — parent controls unmounting; we just ensure min display via startRef
  }, [onReady])

  return (
    <div
      className={leaving ? 'loader-leaving' : ''}
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '80px 20px',
        gap: 16,
      }}
    >
      <canvas ref={canvasRef} style={{ display: 'block' }} />
      <div style={{ color: 'var(--text-muted)', fontSize: 14, minHeight: 20 }}>
        {MESSAGES[msgIdx]}
      </div>
    </div>
  )
}
