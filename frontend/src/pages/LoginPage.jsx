import { useState } from 'react'
import { useAuth } from '../context/AuthContext'

export default function LoginPage() {
  const { login } = useAuth()
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const res = await fetch('/api/auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
      })
      if (res.ok) {
        const { token } = await res.json()
        login(token)
      } else {
        setError('Incorrect password')
      }
    } catch {
      setError('Could not connect to server')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      height: '100vh', display: 'flex', flexDirection: 'column',
      background: 'var(--bg)', fontFamily: 'Inter, sans-serif',
    }}>
      <nav className="nav">
        <div className="nav-brand">
          <span className="nav-wordmark">EV<span className="nav-wordmark-accent">Monitor</span></span>
        </div>
      </nav>

      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{
          background: 'var(--surface)', border: '1px solid #212529',
          borderRadius: 8, padding: '40px 40px 36px', width: '100%', maxWidth: 360,
          boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
        }}>
          <div style={{ marginBottom: 28 }}>
            <div style={{ fontSize: 18, fontWeight: 700, color: 'var(--text)', marginBottom: 6 }}>Sign in</div>
            <div style={{ fontSize: 13, color: 'var(--text-muted)' }}>Enter the dashboard password to continue</div>
          </div>

          <form onSubmit={handleSubmit}>
            <label style={{
              display: 'block', fontSize: 12, fontWeight: 600,
              textTransform: 'uppercase', letterSpacing: '0.05em',
              color: 'var(--text-muted)', marginBottom: 6,
            }}>
              Password
            </label>
            <input
              className="filter-input"
              type="password"
              value={password}
              onChange={e => { setPassword(e.target.value); setError('') }}
              placeholder="Enter password"
              autoFocus
              style={{ width: '100%', boxSizing: 'border-box', fontSize: 14, marginBottom: error ? 6 : 0 }}
            />
            {error && (
              <div style={{ fontSize: 12, color: 'var(--red)', marginBottom: 4 }}>{error}</div>
            )}
            <button
              className="btn btn-primary"
              type="submit"
              disabled={loading || !password}
              style={{ width: '100%', marginTop: 14, padding: '9px 16px', fontSize: 14 }}
            >
              {loading ? 'Signing in…' : 'Sign in'}
            </button>
          </form>
        </div>
      </div>
    </div>
  )
}
