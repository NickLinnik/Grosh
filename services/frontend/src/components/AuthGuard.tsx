'use client'

import { usePathname, useRouter } from 'next/navigation'
import React, { useEffect } from 'react'
import { useAuth } from '@/lib/auth-context'

export function AuthGuard({ children }: { children: React.ReactNode }) {
  const { accessToken, isLoading } = useAuth()
  const router = useRouter()
  const pathname = usePathname()

  useEffect(() => {
    // Wait for the cold-load refresh attempt to settle before deciding
    // whether to redirect — otherwise users with a valid refresh cookie
    // see a brief flash of /login on every cold reload.
    if (isLoading) return
    if (accessToken === null && pathname !== '/login') {
      router.replace('/login')
    }
  }, [accessToken, isLoading, pathname, router])

  if (isLoading) return null
  if (accessToken === null && pathname !== '/login') return null

  return <>{children}</>
}
