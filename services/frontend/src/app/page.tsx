'use client'

import { useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth-context'

export default function Home() {
  const { logout } = useAuth()
  const router = useRouter()

  async function handleLogout() {
    await logout()
    router.push('/login')
  }

  return (
    <main>
      <p>Grosh</p>
      <button onClick={handleLogout}>Log out</button>
    </main>
  )
}
