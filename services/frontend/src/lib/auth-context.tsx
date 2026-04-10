'use client'

import React, { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'
import { setAuthRef } from '@/lib/api'

interface AuthContextValue {
  accessToken: string | null
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [accessToken, setAccessToken] = useState<string | null>(null)
  // Starts true so the AuthGuard can wait for the cold-load refresh attempt
  // to settle before deciding whether to redirect to /login. Avoids a flash
  // of unauthenticated UI when the refresh cookie is still valid on a fresh
  // browser session.
  const [isLoading, setIsLoading] = useState(true)
  const tokenRef = useRef<string | null>(null)

  const updateToken = useCallback((token: string | null) => {
    tokenRef.current = token
    setAccessToken(token)
  }, [])

  useEffect(() => {
    setAuthRef({
      getToken: () => tokenRef.current,
      setToken: updateToken,
    })
  }, [updateToken])

  // Cold-load: try to restore the session from the httpOnly refresh cookie.
  // If it succeeds, the user is logged in without ever seeing /login.
  // If it fails (no cookie, expired cookie), we just stay logged out.
  useEffect(() => {
    let cancelled = false

    void (async () => {
      try {
        const response = await fetch('/api/auth/refresh', {
          method: 'POST',
          credentials: 'include',
        })
        if (cancelled) return
        if (response.ok) {
          const data = (await response.json()) as { access_token: string; token_type: string }
          updateToken(data.access_token)
        }
      } catch {
        // network error or refresh rejected — fall through to logged-out state
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [updateToken])

  const login = useCallback(
    async (email: string, password: string) => {
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ email, password }),
      })

      if (!response.ok) {
        throw response
      }

      const data = (await response.json()) as { access_token: string; token_type: string }
      updateToken(data.access_token)
    },
    [updateToken]
  )

  const logout = useCallback(async () => {
    await fetch('/api/auth/logout', {
      method: 'POST',
      credentials: 'include',
    })
    updateToken(null)
  }, [updateToken])

  return (
    <AuthContext.Provider value={{ accessToken, isLoading, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (ctx === null) {
    throw new Error('useAuth must be used inside AuthProvider')
  }
  return ctx
}
