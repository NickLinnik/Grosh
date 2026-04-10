interface AuthRef {
  getToken: () => string | null
  setToken: (token: string | null) => void
}

let authRef: AuthRef | null = null

export function setAuthRef(ref: AuthRef): void {
  authRef = ref
}

async function refreshToken(): Promise<string | null> {
  const response = await fetch('/api/auth/refresh', {
    method: 'POST',
    credentials: 'include',
  })

  if (!response.ok) {
    return null
  }

  const data = (await response.json()) as { access_token: string; token_type: string }
  return data.access_token
}

function buildHeaders(options: RequestInit, token: string | null): Headers {
  const headers = new Headers(options.headers)
  if (token !== null) {
    headers.set('Authorization', `Bearer ${token}`)
  }
  return headers
}

export async function apiFetch(path: string, options: RequestInit = {}): Promise<Response> {
  const token = authRef?.getToken() ?? null
  const firstResponse = await fetch(path, {
    ...options,
    headers: buildHeaders(options, token),
    credentials: 'include',
  })

  if (firstResponse.status !== 401) {
    return firstResponse
  }

  const newToken = await refreshToken()

  if (newToken === null) {
    authRef?.setToken(null)
    if (typeof window !== 'undefined') {
      window.location.href = '/login?reason=expired'
    }
    return firstResponse
  }

  authRef?.setToken(newToken)

  return fetch(path, {
    ...options,
    headers: buildHeaders(options, newToken),
    credentials: 'include',
  })
}
