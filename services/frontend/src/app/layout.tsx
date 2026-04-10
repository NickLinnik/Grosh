import type { Metadata } from 'next'
import { AuthProvider } from '@/lib/auth-context'
import { AuthGuard } from '@/components/AuthGuard'
import React from 'react'

export const metadata: Metadata = {
  title: 'Grosh',
  description: 'Family finance platform',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AuthProvider>
          <AuthGuard>{children}</AuthGuard>
        </AuthProvider>
      </body>
    </html>
  )
}
