import { afterEach, describe, expect, it, vi } from 'vitest'
import { createEvaluation, readEvaluations, ResearchApiError, submissionUnconfirmed } from './api'

const ID = '11111111-1111-4111-8111-111111111111'
const job = { id: ID, kind: 'historical_quant_pool', profile: 'session-fixture', cutoff: '2026-11-20', allow_current_universe: false, status: 'pending', completed_sections: 0, error_code: null, result_id: null, cancel_requested: false, attempt_count: 0, create_time: '2026-11-25T01:00:00+08:00', update_time: '2026-11-25T01:00:00+08:00' }
const envelope = (data: unknown) => ({ code: '0000', message: 'ok', data, correlation_id: ID })
const expired = () => Response.json({ ...envelope(null), code: 'research.unauthenticated', message: 'expired' }, { status: 401, headers: { 'X-Research-Web-Session': 'required' } })
const refreshed = () => new Response('<html>synthetic entry</html>', { headers: { 'X-Research-Web-Session': 'ready' } })
afterEach(() => vi.unstubAllGlobals())

describe('本机静态工作台续会话', () => {
  it('仅明确未认证后更新cookie，再用相同正文/键/超时信号重放一次', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(expired()).mockResolvedValueOnce(refreshed()).mockResolvedValueOnce(Response.json(envelope(job)))
    vi.stubGlobal('fetch', fetch)
    const body = { profile: job.profile, cutoff: job.cutoff, allow_current_universe: false }
    expect((await createEvaluation(body, ID)).id).toBe(ID)
    expect(fetch).toHaveBeenCalledTimes(3)
    expect(fetch.mock.calls[1]?.[0]).toBe('/chat')
    expect(fetch.mock.calls[1]?.[1].credentials).toBe('same-origin')
    expect(fetch.mock.calls[2]?.[1].body).toBe(fetch.mock.calls[0]?.[1].body)
    expect(fetch.mock.calls[2]?.[1].headers['Idempotency-Key']).toBe(ID)
    expect(fetch.mock.calls[2]?.[1].signal).toBe(fetch.mock.calls[0]?.[1].signal)
  })
  it.each([401, 503])('一般%s不触发本机会话续期或自动重发', async status => {
    const fetch = vi.fn().mockResolvedValue(Response.json({ ...envelope(null), code: 'research.unavailable', message: 'synthetic error' }, { status }))
    vi.stubGlobal('fetch', fetch)
    await expect(readEvaluations(1)).rejects.toThrow('synthetic error')
    expect(fetch).toHaveBeenCalledTimes(1)
  })
  it('同批过期查询合并一次续期，全部重读原页', async () => {
    let renew: (response: Response) => void = () => { throw new Error('renewal not requested') }
    const first = new Set<string>()
    const fetch = vi.fn((url: string) => {
      if (url === '/chat') return new Promise<Response>(resolve => { renew = resolve })
      if (!first.has(url)) { first.add(url); return Promise.resolve(expired()) }
      const page = new URL(url, 'http://127.0.0.1').searchParams.get('page')
      return Promise.resolve(Response.json(envelope({ items: [], total: 0, page: Number(page), page_size: 5 })))
    })
    vi.stubGlobal('fetch', fetch)
    const requests = [readEvaluations(1), readEvaluations(2)]
    await vi.waitFor(() => expect(fetch.mock.calls.filter(([url]) => url === '/chat')).toHaveLength(1))
    renew(refreshed())
    expect((await Promise.all(requests)).map(value => value.page)).toEqual([1, 2])
    expect(fetch.mock.calls.filter(([url]) => url === '/chat')).toHaveLength(1)
  })
  it('入口没有正确标记时不重放，第二次401也不循环续期', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(expired()).mockResolvedValueOnce(new Response('<html>public app</html>'))
    vi.stubGlobal('fetch', fetch)
    await expect(readEvaluations(1)).rejects.toThrow('expired')
    expect(fetch).toHaveBeenCalledTimes(2)
    fetch.mockReset().mockResolvedValueOnce(expired()).mockResolvedValueOnce(refreshed()).mockResolvedValueOnce(expired())
    await expect(readEvaluations(1)).rejects.toThrow('expired')
    expect(fetch).toHaveBeenCalledTimes(3)
  })
})

describe('结果未知的原提交核对', () => {
  it.each([401, 403, 409, 422])('初次%s可确定拒绝，后续核对不能丢弃旧意图', status => {
    const error = new ResearchApiError('synthetic rejection', status, 'research.rejected')
    expect(submissionUnconfirmed(error)).toBe(false)
    expect(submissionUnconfirmed(error, true)).toBe(true)
  })
  it.each([
    new ResearchApiError('connection lost', 0, 'connection_unconfirmed'),
    new ResearchApiError('backend unavailable', 503, 'research.unavailable'),
    new ResearchApiError('invalid receipt', 200, 'invalid_response'),
  ])('未知响应始终保留原意图：%s', error => {
    expect(submissionUnconfirmed(error)).toBe(true)
    expect(submissionUnconfirmed(error, true)).toBe(true)
  })
  it('核对中的非协议异常也不能把较早的受理变成已拒绝', () => {
    expect(submissionUnconfirmed(new Error('synthetic failure'), true)).toBe(true)
  })
})
