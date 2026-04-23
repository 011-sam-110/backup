import { useState, useEffect } from 'react'
import { authFetch } from '../context/AuthContext'

function fmtSize(bytes) {
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function fmtDate(iso) {
  return new Date(iso).toLocaleDateString('en-GB', {
    day: '2-digit', month: 'short', year: 'numeric',
  })
}

export default function ArchiveModal({ onClose }) {
  const [exports, setExports] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    authFetch('/api/exports')
      .then(r => {
        if (r.status === 503) throw new Error('R2 storage not configured on the server')
        if (!r.ok) throw new Error('Failed to load export list')
        return r.json()
      })
      .then(setExports)
      .catch(e => setError(e.message))
  }, [])

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" style={{ maxWidth: 520 }} onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">Archived Exports</span>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>

        {error && (
          <div style={{ color: 'var(--red, #ef4444)', fontSize: 13, padding: '8px 0 16px' }}>{error}</div>
        )}
        {!exports && !error && (
          <div style={{ color: 'var(--text-muted)', fontSize: 13, padding: '8px 0 16px' }}>Loading…</div>
        )}
        {exports && exports.length === 0 && (
          <div style={{ color: 'var(--text-muted)', fontSize: 13, padding: '8px 0 16px' }}>No archived exports found in R2.</div>
        )}
        {exports && exports.length > 0 && (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr>
                {['File', 'Date', 'Size', ''].map(h => (
                  <th key={h} style={{
                    textAlign: h === 'File' ? 'left' : 'right',
                    paddingBottom: 10,
                    color: 'var(--text-muted)',
                    fontWeight: 600,
                    fontSize: 11,
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                  }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {exports.map(f => (
                <tr key={f.filename} style={{ borderTop: '1px solid var(--border)' }}>
                  <td style={{ padding: '9px 0', fontWeight: 500 }}>{f.filename}</td>
                  <td style={{ textAlign: 'right', color: 'var(--text-muted)', paddingLeft: 16, whiteSpace: 'nowrap' }}>{fmtDate(f.modified)}</td>
                  <td style={{ textAlign: 'right', color: 'var(--text-muted)', paddingLeft: 12, whiteSpace: 'nowrap' }}>{fmtSize(f.size_bytes)}</td>
                  <td style={{ paddingLeft: 12 }}>
                    <a
                      href={f.url}
                      download={f.filename}
                      className="btn btn-outline"
                      style={{ fontSize: 11, padding: '3px 10px', whiteSpace: 'nowrap', textDecoration: 'none' }}
                    >
                      ↓ Download
                    </a>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 20 }}>
          <button className="btn btn-outline" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  )
}
