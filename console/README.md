# Tenuto Console

Tenuto Console 是 Tenuto 的 Workflow 运维与调试界面，包含 Namespace、Run、Workflow 定义、Worker、Schedule、Queue、批量操作、Event History、Debugger 和 Trace 页面。Trace 重放由 `console/wasm` 构建的浏览器 WASM 模块提供。

## 环境要求

| 工具 | 版本 | 用途 |
| --- | --- | --- |
| Node.js | 20.9+ | Next.js 16 |
| pnpm | 10+ | 依赖安装（锁文件为 `pnpm-lock.yaml`） |
| Go | 仓库根目录 `go.mod` 声明的版本 | 构建 `public/replay.wasm` |

## 安装

在仓库根目录执行：

```bash
pnpm --dir console install
```

## 启动

完整开发环境（Tenuto Server、Demo Worker/Seeder 与 Console）：

```bash
make dev
```

只启动 Console，连接已经运行的 Tenuto Server：

```bash
make dev-console APP_PORT=7400 UI_PORT=7401
```

`make dev` 与 `make dev-console` 先执行 `make wasm`，生成 `public/replay.wasm` 并从 Go 安装目录复制 `public/wasm_exec.js`。两个文件都是构建产物，不进版本库；直接在 `console/` 下运行 `pnpm dev` 或 `pnpm build` 之前，先在仓库根目录执行一次 `make wasm`，否则 Trace 页面加载 WASM 会返回 404。

## 环境变量

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `TENUTO_SERVER_URL` | 空 | `/runtime/workflow/*` 请求转发到的 Tenuto Server 地址，例如 `http://tenuto.internal:7400`；为空时使用 `http://localhost:${NEXT_PUBLIC_API_PORT}` |
| `TENUTO_SERVER_AUTH_TOKEN` | 空 | Server 开启 `WORKFLOW_HTTP_AUTH_TOKEN` 时，转发请求带上的 Bearer token；只在服务端读取，不要用 `NEXT_PUBLIC_` 前缀 |
| `TENUTO_SERVER_TLS_CA_FILE` | 空 | 校验 Server 证书的 CA 文件（PEM） |
| `TENUTO_SERVER_TLS_CERT_FILE`、`TENUTO_SERVER_TLS_KEY_FILE` | 空 | Server 开启 mTLS 时的客户端证书与私钥 |
| `NEXT_PUBLIC_API_PORT` | `7400` | 本机 Tenuto Server 端口，`TENUTO_SERVER_URL` 为空时生效；`make` 目标按 `APP_PORT` 设置 |
| `NEXT_PUBLIC_API_URL` | 空 | 浏览器直接请求的 API 前缀，只用于没有开启认证的开发环境；为空时浏览器请求同源 `/runtime/workflow/*`，由 Console 服务端转发 |
| `TENUTO_CONSOLE_DEV_ORIGINS` | 空 | 允许从其他主机访问 dev server 的来源，逗号分隔的主机名或通配模式，例如 `10.1.1.*,my-laptop.local` |

转发由 `src/app/runtime/workflow/[...path]/route.ts`（逻辑在 `src/lib/server-proxy.ts`）完成：只转发 POST，路径固定在 `/runtime/workflow/` 下，不转发浏览器的 Cookie 与 Authorization 头。`TENUTO_SERVER_URL`、`TENUTO_SERVER_AUTH_TOKEN` 与 `TENUTO_SERVER_TLS_*` 在第一次请求时读取，`TENUTO_CONSOLE_DEV_ORIGINS` 在 Next.js 启动时读取；`NEXT_PUBLIC_*` 在构建时写入前端代码。Console 没有用户登录，配置了 token 后对外开放时放在你自己的 SSO 或鉴权代理之后。

## 验证

```bash
pnpm test                  # vitest 单元测试
pnpm exec tsc --noEmit     # 类型检查
pnpm build                 # 生产构建
```
