import type { NextConfig } from "next";

// Tenuto Server 地址：优先 TENUTO_SERVER_URL，其次用 NEXT_PUBLIC_API_PORT 拼本机地址。浏览器对 /runtime/workflow/*
// 的请求由 src/app/runtime/workflow/[...path]/route.ts 在服务端转发（加上 TENUTO_SERVER_AUTH_TOKEN），这里只用它生成提示文本。
const apiPort = process.env.NEXT_PUBLIC_API_PORT || '7400';
const serverURL = (process.env.TENUTO_SERVER_URL || `http://localhost:${apiPort}`).replace(/\/+$/, '');

// 允许从其他主机访问 dev server 时，用逗号分隔的主机名或通配模式（如 `10.1.1.*`）。
const devOrigins = (process.env.TENUTO_CONSOLE_DEV_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const nextConfig: NextConfig = {
  allowedDevOrigins: devOrigins,
  // 代理目标地址写进前端包，Server 连不上时提示里给出这个地址（不含 token）。
  env: { TENUTO_CONSOLE_SERVER_URL: serverURL },
  async headers() {
    return [
      {
        source: '/:path*.wasm',
        headers: [
          { key: 'Cache-Control', value: 'no-store, must-revalidate' },
        ],
      },
      {
        source: '/wasm_exec.js',
        headers: [
          { key: 'Cache-Control', value: 'no-store, must-revalidate' },
        ],
      },
    ]
  },
};

export default nextConfig;
