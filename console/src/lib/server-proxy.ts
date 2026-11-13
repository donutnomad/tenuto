// Console 到 Tenuto Server 的服务端代理（只在 Node.js 运行时执行，由 app/runtime/workflow/[...path]/route.ts 调用）。
//
// 浏览器只访问 Console 自己的 /runtime/workflow/*；代理把请求转发到固定的 TENUTO_SERVER_URL，
// 并在服务端加上 `Authorization: Bearer <TENUTO_SERVER_AUTH_TOKEN>` 与可选的 mTLS 客户端证书。
// token 与证书只存在于 Console 的服务端进程，不进入浏览器。
import { readFileSync } from 'node:fs'
import http from 'node:http'
import https from 'node:https'
import { Readable } from 'node:stream'
import type { ReadableStream as NodeReadableStream } from 'node:stream/web'

// PROXIED_PATH 是代理转发的唯一路径前缀：Console 只调用 Workflow 的 Runtime API。
export const PROXIED_PATH = '/runtime/workflow/'

// 转发给 Server 的请求头。浏览器的 Cookie、Authorization、Host 与 X-Forwarded-* 不转发。
const REQUEST_HEADERS = ['accept', 'content-type', 'traceparent', 'tracestate']
// 返回给浏览器的应答头。
const RESPONSE_HEADERS = ['cache-control', 'content-type', 'retry-after']

export interface ProxyConfig {
  // server 是 Server 的地址（可以带路径前缀），不含末尾的 /。
  server: URL
  authToken?: string
  tls?: { ca?: Buffer; cert?: Buffer; key?: Buffer }
}

type Env = Record<string, string | undefined>

// loadProxyConfig 读取代理配置。TENUTO_SERVER_URL 缺省为 http://localhost:${NEXT_PUBLIC_API_PORT || 7400}。
export function loadProxyConfig(env: Env = process.env): ProxyConfig {
  const raw = (env.TENUTO_SERVER_URL || `http://localhost:${env.NEXT_PUBLIC_API_PORT || '7400'}`).trim()
  let server: URL
  try {
    server = new URL(raw.replace(/\/+$/, ''))
  } catch {
    throw new Error(`TENUTO_SERVER_URL ${JSON.stringify(raw)} is not a URL`)
  }
  if (server.protocol !== 'http:' && server.protocol !== 'https:') {
    throw new Error('TENUTO_SERVER_URL must be an http or https URL')
  }
  if (server.search || server.hash || server.username || server.password) {
    throw new Error('TENUTO_SERVER_URL must not contain credentials, a query or a fragment')
  }
  const config: ProxyConfig = { server }
  const token = env.TENUTO_SERVER_AUTH_TOKEN?.trim()
  if (token) config.authToken = token

  const caFile = env.TENUTO_SERVER_TLS_CA_FILE?.trim()
  const certFile = env.TENUTO_SERVER_TLS_CERT_FILE?.trim()
  const keyFile = env.TENUTO_SERVER_TLS_KEY_FILE?.trim()
  if (Boolean(certFile) !== Boolean(keyFile)) {
    throw new Error('TENUTO_SERVER_TLS_CERT_FILE and TENUTO_SERVER_TLS_KEY_FILE must be configured together')
  }
  if (caFile || certFile) {
    if (server.protocol !== 'https:') {
      throw new Error('TENUTO_SERVER_TLS_* requires an https TENUTO_SERVER_URL')
    }
    config.tls = {
      ca: caFile ? readFileSync(caFile) : undefined,
      cert: certFile ? readFileSync(certFile) : undefined,
      key: keyFile ? readFileSync(keyFile) : undefined,
    }
  }
  return config
}

let cachedConfig: ProxyConfig | undefined

// proxyConfig 在第一次请求时读取并缓存配置；改环境变量或证书文件后重启 Console 生效。
export function proxyConfig(): ProxyConfig {
  cachedConfig ??= loadProxyConfig()
  return cachedConfig
}

// upstreamURL 把路由参数拼成 Server 上的地址。每段单独编码，`.`、`..` 与空段被拒绝，结果必须仍在
// <server>/runtime/workflow/ 之下；请求的 Host 与路径都不能改变目标主机。不合法时返回 null。
export function upstreamURL(config: ProxyConfig, segments: string[], search: string): URL | null {
  if (segments.length === 0 || segments.some((s) => s === '' || s === '.' || s === '..')) {
    return null
  }
  const prefix = config.server.pathname.replace(/\/+$/, '') + PROXIED_PATH
  const target = new URL(config.server.origin + prefix + segments.map(encodeURIComponent).join('/') + search)
  if (target.origin !== config.server.origin || !target.pathname.startsWith(prefix)) {
    return null
  }
  return target
}

function requestHeaders(incoming: Headers, authToken?: string): Record<string, string> {
  const headers: Record<string, string> = {}
  for (const name of REQUEST_HEADERS) {
    const value = incoming.get(name)
    if (value !== null) headers[name] = value
  }
  if (authToken) headers.authorization = `Bearer ${authToken}`
  return headers
}

function responseHeaders(incoming: http.IncomingHttpHeaders): Headers {
  const headers = new Headers()
  for (const name of RESPONSE_HEADERS) {
    const value = incoming[name]
    if (typeof value === 'string') headers.set(name, value)
  }
  return headers
}

function plain(status: number, text: string): Response {
  return new Response(text, { status, headers: { 'content-type': 'text/plain; charset=utf-8' } })
}

// proxyRuntimeRequest 把 request 转发到 Server 并原样返回状态码、允许的应答头与应答体。应答体以流返回，
// 长轮询（wait-closed 等）在 Server 应答之前一直挂起；浏览器断开时 request.signal 中止上游请求。
// Server 连不上时返回没有 Content-Type 的 502，浏览器据此提示 Server 不可达。
export function proxyRuntimeRequest(request: Request, segments: string[], config: ProxyConfig): Promise<Response> {
  const target = upstreamURL(config, segments, new URL(request.url).search)
  if (!target) {
    return Promise.resolve(plain(400, 'invalid runtime path'))
  }
  const transport = target.protocol === 'https:' ? https : http
  return new Promise<Response>((resolve) => {
    let settled = false
    const upstream = transport.request(target, {
      method: request.method,
      headers: requestHeaders(request.headers, config.authToken),
      signal: request.signal,
      ...config.tls,
    }, (res) => {
      settled = true
      const status = res.statusCode ?? 502
      const nullBody = status === 204 || status === 205 || status === 304
      if (nullBody) res.resume()
      const body = nullBody ? null : (Readable.toWeb(res) as ReadableStream<Uint8Array>)
      resolve(new Response(body, { status, headers: responseHeaders(res.headers) }))
    })
    upstream.on('error', (error) => {
      if (settled) return
      settled = true
      if (!request.signal.aborted) {
        console.error(`tenuto console proxy: ${target.origin}${target.pathname}: ${error.message}`)
      }
      resolve(new Response(null, { status: 502 }))
    })
    if (request.body) {
      const body = Readable.fromWeb(request.body as NodeReadableStream<Uint8Array>)
      body.on('error', (error) => upstream.destroy(error))
      body.pipe(upstream)
    } else {
      upstream.end()
    }
  })
}
