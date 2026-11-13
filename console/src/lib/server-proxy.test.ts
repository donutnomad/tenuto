import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, it } from 'vitest'
import { loadProxyConfig, proxyRuntimeRequest, upstreamURL, type ProxyConfig } from './server-proxy'

interface Captured {
  method?: string
  url?: string
  headers: http.IncomingHttpHeaders
  body: string
}

const servers: http.Server[] = []

afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => new Promise((r) => { s.closeAllConnections(); s.close(r) })))
})

// upstream 启动一个本机 HTTP Server 冒充 Tenuto Server，记录收到的请求。
async function upstream(handler: (req: http.IncomingMessage, res: http.ServerResponse, captured: Captured) => void) {
  const requests: Captured[] = []
  const server = http.createServer((req, res) => {
    const captured: Captured = { method: req.method, url: req.url, headers: req.headers, body: '' }
    requests.push(captured)
    req.setEncoding('utf8')
    req.on('data', (chunk) => { captured.body += chunk })
    req.on('end', () => handler(req, res, captured))
  })
  servers.push(server)
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  const { port } = server.address() as AddressInfo
  return { server, requests, url: `http://127.0.0.1:${port}` }
}

function browserRequest(path: string, init: RequestInit = {}) {
  return new Request(`http://console.example${path}`, {
    method: 'POST',
    body: '{"namespace":"default"}',
    ...init,
    headers: {
      'content-type': 'application/json',
      authorization: 'Bearer from-browser',
      cookie: 'sso_session=secret',
      'x-forwarded-host': 'attacker.example',
      traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
      ...(init.headers as Record<string, string> | undefined),
    },
  })
}

describe('proxyRuntimeRequest', () => {
  it('forwards method, path, query, body and safe headers, and injects the server token', async () => {
    const up = await upstream((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json', 'set-cookie': 'upstream=1', 'x-internal': 'yes' })
      res.end('{"runs":[]}')
    })
    const config = loadProxyConfig({ TENUTO_SERVER_URL: up.url + '/', TENUTO_SERVER_AUTH_TOKEN: 'server-token-0123456789' })
    const res = await proxyRuntimeRequest(browserRequest('/runtime/workflow/list?debug=1'), ['list'], config)

    expect(res.status).toBe(200)
    expect(await res.text()).toBe('{"runs":[]}')
    expect(res.headers.get('content-type')).toBe('application/json')
    expect(res.headers.get('set-cookie')).toBeNull()
    expect(res.headers.get('x-internal')).toBeNull()

    const [got] = up.requests
    expect(got.method).toBe('POST')
    expect(got.url).toBe('/runtime/workflow/list?debug=1')
    expect(got.body).toBe('{"namespace":"default"}')
    expect(got.headers.authorization).toBe('Bearer server-token-0123456789')
    expect(got.headers['content-type']).toBe('application/json')
    expect(got.headers.traceparent).toBe('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')
    expect(got.headers.cookie).toBeUndefined()
    expect(got.headers['x-forwarded-host']).toBeUndefined()
    expect(got.headers.host).toBe(new URL(up.url).host)
  })

  it('drops the browser Authorization header when no server token is configured', async () => {
    const up = await upstream((_req, res) => res.end('{}'))
    await proxyRuntimeRequest(browserRequest('/runtime/workflow/list'), ['list'], loadProxyConfig({ TENUTO_SERVER_URL: up.url }))
    expect(up.requests[0].headers.authorization).toBeUndefined()
  })

  it('passes through server status codes and error bodies', async () => {
    const up = await upstream((_req, res) => {
      res.writeHead(401, { 'content-type': 'text/plain; charset=utf-8', 'www-authenticate': 'Bearer realm="tenuto"' })
      res.end('unauthorized\n')
    })
    const res = await proxyRuntimeRequest(browserRequest('/runtime/workflow/get-run'), ['get-run'], loadProxyConfig({ TENUTO_SERVER_URL: up.url }))
    expect(res.status).toBe(401)
    expect(res.headers.get('content-type')).toBe('text/plain; charset=utf-8')
    expect(await res.text()).toBe('unauthorized\n')

    const empty = await upstream((_req, res) => { res.writeHead(204); res.end() })
    const noContent = await proxyRuntimeRequest(browserRequest('/runtime/workflow/wait-closed'), ['wait-closed'], loadProxyConfig({ TENUTO_SERVER_URL: empty.url }))
    expect(noContent.status).toBe(204)
  })

  it('streams the response body as the server writes it', async () => {
    let finish: () => void = () => {}
    const up = await upstream((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.write('{"part":')
      finish = () => res.end('1}')
    })
    const res = await proxyRuntimeRequest(browserRequest('/runtime/workflow/wait-closed'), ['wait-closed'], loadProxyConfig({ TENUTO_SERVER_URL: up.url }))
    const reader = res.body!.getReader()
    const first = await reader.read()
    expect(new TextDecoder().decode(first.value)).toBe('{"part":')
    finish()
    let rest = ''
    for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) rest += new TextDecoder().decode(chunk.value)
    expect(rest).toBe('1}')
  })

  it('aborts the upstream request when the browser disconnects', async () => {
    let closed: () => void = () => {}
    const upstreamClosed = new Promise<void>((r) => { closed = r })
    const up = await upstream((req) => { req.socket.on('close', () => closed()) }) // 长轮询：一直不应答
    const controller = new AbortController()
    const pending = proxyRuntimeRequest(browserRequest('/runtime/workflow/wait-closed', { signal: controller.signal }), ['wait-closed'], loadProxyConfig({ TENUTO_SERVER_URL: up.url }))
    await new Promise((r) => setTimeout(r, 50))
    controller.abort()
    await upstreamClosed
    expect((await pending).status).toBe(502)
  })

  it('returns 502 without a content type when the server is unreachable', async () => {
    const up = await upstream((_req, res) => res.end())
    const url = up.url
    await new Promise((r) => { up.server.close(r) })
    const res = await proxyRuntimeRequest(browserRequest('/runtime/workflow/list'), ['list'], loadProxyConfig({ TENUTO_SERVER_URL: url }))
    expect(res.status).toBe(502)
    expect(res.headers.get('content-type')).toBeNull()
  })

  it('rejects traversal segments without contacting the server', async () => {
    const up = await upstream((_req, res) => res.end())
    const config = loadProxyConfig({ TENUTO_SERVER_URL: up.url })
    for (const segments of [['..', 'activity', 'poll'], ['.', 'list'], ['list', ''], []]) {
      const res = await proxyRuntimeRequest(browserRequest('/runtime/workflow/x'), segments, config)
      expect(res.status).toBe(400)
    }
    expect(up.requests).toHaveLength(0)
  })
})

describe('upstreamURL', () => {
  const config: ProxyConfig = loadProxyConfig({ TENUTO_SERVER_URL: 'https://tenuto.internal:7400/gateway/' })

  it('keeps every target under <server>/runtime/workflow/', () => {
    expect(upstreamURL(config, ['list'], '?a=1')?.href).toBe('https://tenuto.internal:7400/gateway/runtime/workflow/list?a=1')
    // 解码后含 / 或 .. 的单段被重新编码，不能跳出前缀。
    expect(upstreamURL(config, ['../../admin/partitions'], '')?.pathname).toBe('/gateway/runtime/workflow/..%2F..%2Fadmin%2Fpartitions')
    expect(upstreamURL(config, ['@evil.example', 'x'], '')?.host).toBe('tenuto.internal:7400')
    expect(upstreamURL(config, ['..', 'admin'], '')).toBeNull()
  })
})

describe('loadProxyConfig', () => {
  it('defaults to the local server and reads the token only from TENUTO_SERVER_AUTH_TOKEN', () => {
    const config = loadProxyConfig({ NEXT_PUBLIC_API_PORT: '7500', NEXT_PUBLIC_TENUTO_SERVER_AUTH_TOKEN: 'leaked' })
    expect(config.server.href).toBe('http://localhost:7500/')
    expect(config.authToken).toBeUndefined()
  })

  it('rejects invalid server URLs and incomplete TLS settings', () => {
    expect(() => loadProxyConfig({ TENUTO_SERVER_URL: 'file:///etc/passwd' })).toThrow(/http or https/)
    expect(() => loadProxyConfig({ TENUTO_SERVER_URL: 'http://user:pw@tenuto:7400' })).toThrow(/credentials/)
    expect(() => loadProxyConfig({ TENUTO_SERVER_URL: 'https://tenuto', TENUTO_SERVER_TLS_CERT_FILE: 'c.pem' })).toThrow(/together/)
    expect(() => loadProxyConfig({ TENUTO_SERVER_URL: 'http://tenuto', TENUTO_SERVER_TLS_CA_FILE: 'ca.pem' })).toThrow(/https/)
  })
})
