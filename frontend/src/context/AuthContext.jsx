import { createContext, useContext, useState } from 'react'

const TOKEN_KEY = 'ev_token'
const AuthContext = createContext(null)

/** Drop-in replacement for fetch() that injects the auth token. */
export function authFetch(url, opts = {}) {
  const token = sessionStorage.getItem(TOKEN_KEY)
  const headers = { ...(opts.headers || {}) }
  if (token) headers['Authorization'] = `Bearer ${token}`
  return fetch(url, { ...opts, headers })
}

export function AuthProvider({ children }) {
  const [token, setToken] = useState(() => sessionStorage.getItem(TOKEN_KEY))

  function login(t) {
    sessionStorage.setItem(TOKEN_KEY, t)
    setToken(t)
  }

  function logout() {
    sessionStorage.removeItem(TOKEN_KEY)
    setToken(null)
  }

  return (
    <AuthContext.Provider value={{ token, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  return useContext(AuthContext)
}
