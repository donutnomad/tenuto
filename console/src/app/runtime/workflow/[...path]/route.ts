import { proxyConfig, proxyRuntimeRequest } from '@/lib/server-proxy'

// 浏览器对 /runtime/workflow/* 的调用经这里转发到 Tenuto Server（见 src/lib/server-proxy.ts）。
// Runtime API 只有 POST 端点，其他方法由 Next.js 应答 405。
export async function POST(request: Request, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params
  return proxyRuntimeRequest(request, path, proxyConfig())
}
