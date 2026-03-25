import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import LiveStatus from './pages/LiveStatus'
import Graphs from './pages/Graphs'
import MapView from './pages/MapView'
import { FilterProvider } from './context/FilterContext'
import Toolbar from './components/Toolbar'
import 'leaflet/dist/leaflet.css'
import './index.css'

export default function App() {
  return (
    <BrowserRouter>
    <FilterProvider>
      <nav className="nav">
        <div className="nav-brand">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <span className="nav-wordmark">
              EV<span className="nav-wordmark-accent">Monitor</span>
            </span>
            <span style={{ fontSize: 7.5, fontWeight: 700, letterSpacing: '0.12em', color: '#ffffff', lineHeight: 1 }}>
              POWERED BY EVANTI
            </span>
          </div>
        </div>
        <div className="nav-links">
          <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
            Live Status
          </NavLink>
          <NavLink to="/graphs" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
            Analytics
          </NavLink>
          <NavLink to="/map" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
            Map
          </NavLink>
        </div>
      </nav>
      <div className="app-body">
        <Toolbar />
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
