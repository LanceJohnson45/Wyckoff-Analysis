let renewal: Promise<boolean> | null = null

export async function renewResearchWebSession(): Promise<boolean> {
  if (renewal) return renewal
  renewal = readEntry()
  try { return await renewal }
  finally { renewal = null }
}

async function readEntry(): Promise<boolean> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 5000)
  try {
    const response = await fetch('/chat', { credentials: 'same-origin', cache: 'no-store', signal: controller.signal })
    return response.ok && response.headers.get('X-Research-Web-Session') === 'ready'
  } catch { return false }
  finally { clearTimeout(timer) }
}
