import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import LiveStatus from './pages/LiveStatus'
import Graphs from './pages/Graphs'
import MapView from './pages/MapView'
import LoginPage from './pages/LoginPage'
import { FilterProvider } from './context/FilterContext'
import { AuthProvider, useAuth } from './context/AuthContext'
import Toolbar from './components/Toolbar'
import 'leaflet/dist/leaflet.css'
import './index.css'

function AppInner() {
  const { token, logout } = useAuth()
  const [sidebarOpen, setSidebarOpen] = useState(true)

  useEffect(() => {
    document.body.style.overflow = sidebarOpen && window.innerWidth <= 768 ? 'hidden' : ''
    return () => { document.body.style.overflow = '' }
  }, [sidebarOpen])

  if (!token) return <LoginPage />

  return (
    <BrowserRouter>
      <FilterProvider>
        <nav className="nav">
          <button
            className="hamburger"
            onClick={() => setSidebarOpen(o => !o)}
            aria-label="Toggle filters"
          >
            ☰
          </button>
          <div className="nav-brand">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <span className="nav-wordmark">
                EV<span className="nav-wordmark-accent">Monitor</span>
              </span>
              <span style={{ fontSize: 7.5, fontWeight: 700, letterSpacing: '0.12em', color: '#ffffff', lineHeight: 1 }}>

              </span>
            </div>
          </div>
          <div className="nav-links">
            <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}
              onClick={() => window.innerWidth <= 768 && setSidebarOpen(false)}>
              Live Status
            </NavLink>
            <NavLink to="/graphs" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}
              onClick={() => window.innerWidth <= 768 && setSidebarOpen(false)}>
              Analytics
            </NavLink>
            <NavLink to="/map" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}
              onClick={() => window.innerWidth <= 768 && setSidebarOpen(false)}>
              Map
            </NavLink>
          </div>
          <button
            onClick={logout}
            className="sign-out-btn"
            style={{
              position: 'absolute', right: 24,
              background: 'none', border: 'none',
              color: '#94A3B8', fontSize: 13, cursor: 'pointer',
              fontFamily: 'Inter, inherit', fontWeight: 500,
              transition: 'color 0.15s',
            }}
            onMouseOver={e => e.currentTarget.style.color = '#F1F5F9'}
            onMouseOut={e => e.currentTarget.style.color = '#94A3B8'}
          >
            Sign out
          </button>
        </nav>
        <div className="app-body" style={{ position: 'relative' }}>
          <div
            className={`sidebar-backdrop${sidebarOpen ? ' active' : ''}`}
            onClick={() => setSidebarOpen(false)}
          />
          <Toolbar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} onToggle={() => setSidebarOpen(o => !o)} />
          {!sidebarOpen && (
            <button
              className="sidebar-reopen-btn"
              onClick={() => setSidebarOpen(true)}
              aria-label="Open filters"
            >
              ›
            </button>
          )}
          <main className="main-content">
            <Routes>
              <Route path="/" element={<LiveStatus />} />
              <Route path="/graphs" element={<Graphs />} />
              <Route path="/map" element={<MapView />} />
            </Routes>
          </main>
        </div>
      </FilterProvider>
    </BrowserRouter>
  )
}

export default function App() {
  return (
    <AuthProvider>
      <AppInner />
    </AuthProvider>
  )
}
