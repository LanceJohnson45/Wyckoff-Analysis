import { randomUUID } from 'node:crypto'
import type { IncomingHttpHeaders, IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'
import type { Plugin } from 'vite'

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
const READ_PATH = new RegExp(`^/v1/(?:reports/(?:latest|${UUID}(?:/(?:market|groups|compare|rules|universe|anchors/[A-Z][A-Z0-9.-]{0,31}))?)|research-runs(?:/${UUID}(?:/(?:reports|events|parameters|inputs|quant-attempts))?)?|watchlists(?:/events)?|stocks/[A-Z][A-Z0-9.-]{0,31}/observations|strategies|parameter-sets(?:/${UUID})?|preparations|chat-turns(?:/${UUID})?|theme-proposals(?:/${UUID})?|input-publications|universe-overrides(?:/[A-Z][A-Z0-9.-]{0,31})?)$`)
const WRITE_PATH = new RegExp(`^/v1/(?:research-runs(?:/${UUID}/(?:retry|cancel))?|parameter-sets|strategies/(?:martin|wyckoff)/activate-parameters|chat-turns(?:/${UUID}/cancel)?|theme-proposals(?:/${UUID}/confirm)?|universe-overrides)$`)
const MAX_BODY_BYTES = 8192
const CHAT_BODY_BYTES = 49152
const INPUT_BODY_BYTES = 65536

export function localResearchEnabled(environment: Record<string, string>, command: 'serve' | 'build'): boolean {
  if (command !== 'serve' || environment.STOCK_RESEARCH_WEB_SINGLE_USER !== 'true') return false
  if (environment.STOCK_RESEARCH_WEB_PROXY_ENABLED !== 'true') {
    throw new Error('本机单用户界面须先启用研究代理')
  }
  return true
}

function fail(res: ServerResponse, status: 403 | 404 | 413 | 503, message: string): void {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
  const code = { 403: 'research.forbidden', 404: 'research.not_found', 413: 'research.invalid_input', 503: 'research.unavailable' }[status]
  res.end(JSON.stringify({ code, message, data: null, correlation_id: randomUUID() }))
}

export function trustedResearchRequest(headers: IncomingHttpHeaders): boolean {
  try {
    if (!headers.host || !/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(headers.host)) return false
    const base = new URL(`http://${headers.host}`)
    return ['127.0.0.1', 'localhost'].includes(base.hostname)
      && (!headers.origin || headers.origin === base.origin)
      && (!headers['sec-fetch-site'] || ['same-origin', 'none'].includes(String(headers['sec-fetch-site'])))
  } catch { return false }
}

export function allowedResearchRoute(method: string | undefined, pathname: string): boolean {
  return method === 'GET' ? READ_PATH.test(pathname) : method === 'POST' && WRITE_PATH.test(pathname)
}

async function requestBody(req: IncomingMessage, maximum: number): Promise<ArrayBuffer | undefined> {
  if (req.method !== 'POST') return undefined
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of req) {
    const part = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    bytes += part.byteLength
    if (bytes > maximum) throw new RangeError('body limit')
    chunks.push(part)
  }
  return Uint8Array.from(Buffer.concat(chunks)).buffer
}

async function forward(req: IncomingMessage, res: ServerResponse, target: URL, token: string, maximum: number): Promise<void> {
  const headers: Record<string, string> = { Authorization: `Bearer ${token}` }
  for (const key of ['content-type', 'idempotency-key', 'last-event-id']) {
    const value = req.headers[key]
    if (typeof value === 'string') headers[key] = value
  }
  const body = await requestBody(req, maximum)
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 135_000)
  const cleanup = () => { clearTimeout(timer); controller.abort() }
  res.once('close', cleanup)
  try {
    const response = await fetch(target, { method: req.method, headers, body, signal: controller.signal, redirect: 'error' })
    res.statusCode = response.status
    for (const key of ['content-type', 'cache-control', 'x-correlation-id']) {
      const value = response.headers.get(key)
      if (value) res.setHeader(key, value)
    }
    if (!response.body) { cleanup(); res.end(); return }
    const stream = Readable.from(response.body)
    stream.once('end', cleanup)
    stream.once('error', () => { cleanup(); res.destroy() })
    stream.pipe(res)
  } catch {
    cleanup()
    if (!res.headersSent) fail(res, 503, '研究连接暂不可用')
    else res.destroy()
  }
}

export function stockResearchProxy(environment: Record<string, string>): Plugin {
  const enabled = environment.STOCK_RESEARCH_WEB_PROXY_ENABLED === 'true'
  const token = environment.STOCK_RESEARCH_API_TOKEN || ''
  const host = environment.STOCK_RESEARCH_API_HOST || '127.0.0.1'
  const port = environment.STOCK_RESEARCH_API_PORT || '8788'
  return {
    name: 'stock-research-loopback-proxy',
    configureServer(server) {
      if (enabled && (host !== '127.0.0.1' || !/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535 || token.length < 32)) {
        throw new Error('研究代理须配置独立身份及有效loopback端口')
      }
      const bind = server.config.server.host
      if (enabled && bind !== undefined && bind !== '127.0.0.1' && bind !== 'localhost') {
        throw new Error('研究开发代理只允许本机监听')
      }
      server.middlewares.use('/api/stock-research', async (req, res) => {
        if (!trustedResearchRequest(req.headers)) { fail(res, 403, '研究连接来源未授权'); return }
        if (!enabled) { fail(res, 503, '此设备尚未连接研究服务'); return }
        const route = new URL(req.url || '/', 'http://127.0.0.1')
        const allowed = allowedResearchRoute(req.method, route.pathname)
        if (!allowed) { fail(res, 404, '研究入口不存在'); return }
        const maximum = req.method !== 'POST' ? MAX_BODY_BYTES : route.pathname === '/v1/chat-turns' ? CHAT_BODY_BYTES : ['/v1/theme-proposals', '/v1/universe-overrides'].includes(route.pathname) ? INPUT_BODY_BYTES : MAX_BODY_BYTES
        if (Number(req.headers['content-length']) > maximum) { fail(res, 413, '研究请求过大'); req.resume(); return }
        try { await forward(req, res, new URL(route.pathname + route.search, `http://127.0.0.1:${port}`), token, maximum) }
        catch { if (!res.headersSent) fail(res, 413, '研究请求过大') }
      })
    },
  }
}
