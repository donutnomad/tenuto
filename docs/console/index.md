---
title: Tenuto Console
description: Namespace、Run、Worker、Schedule 与调试入口
status: Preview
---

# Tenuto Console

<FeatureStatus status="Preview" />

Tenuto Console 是 Workflow 运维、可见性和源码调试界面。默认开发地址为 `http://localhost:7401`。

## 页面结构

| 页面 | 用途 |
| --- | --- |
| Namespaces | 查看租户与 Run、Workflow、Worker、Schedule 数量 |
| Runs | 查询运行实例、状态、等待点与 Search Attributes |
| Definitions | 查看已注册 Workflow Type 和版本 |
| Workers | 查看 Identity、Build ID、Task Queue 和 heartbeat |
| Schedules | 管理 Cron、Timezone、Overlap Policy 与暂停状态 |
| Queues | 查看逻辑 Task Queue 与路由信息 |
| Batch | 跟踪批量 Cancel、Terminate 和 Reset |

侧边栏在宽度 768px 及以上常驻；更窄的屏幕上收起，点页头左侧的菜单按钮打开，点链接、遮罩或按 Esc 关闭。390px 宽的手机屏幕上页面不横向滚动，宽表格与 Run Detail 的页签栏在自身区域内横向滚动。

## Run Detail

Run Detail 提供：

- Summary：Workflow ID、Type、Run ID、Namespace、版本和时间；失败的 Run 显示终态失败，Blocked 的 Run 显示 Workflow Task 失败原因。
- Pending：未结束 Activity 的当前尝试、Worker、心跳、最近失败与下一次重试时间。
- Relationships：Parent、Child、Continue-As-New 与 Reset Chain。
- Event History：分页事件、分类过滤和结构化详情。
- Timeline：按时间查看 Workflow Task、Activity、Timer 和 Child。
- Queries：调用已注册 Query Handler。
- 操作：Cancel、Terminate、Reset、Signal 与 Activity Reset。

## Trace 入口

Run Detail 的 Trace Tab 展示毛玻璃预览和独立调试台入口。完整 Trace 使用独立页面，占满可用空间，保留 URL 调试状态。

## API 连接

浏览器调用 Console 自己的 `/runtime/workflow/*`，Console 的服务端（`src/app/runtime/workflow/[...path]/route.ts`）把 POST 转发到固定的 `TENUTO_SERVER_URL`（缺省 `http://localhost:${NEXT_PUBLIC_API_PORT}`），应答以流返回，长轮询照常挂起。Server 开启认证时：

| 变量 | 作用 |
| --- | --- |
| `TENUTO_SERVER_AUTH_TOKEN` | 转发时加上 `Authorization: Bearer <token>`，取 Server `WORKFLOW_HTTP_AUTH_TOKEN` 中的一个；只在 Console 服务端读取，不进入浏览器 |
| `TENUTO_SERVER_TLS_CA_FILE` | 校验 Server 证书的 CA（PEM），`TENUTO_SERVER_URL` 需为 https |
| `TENUTO_SERVER_TLS_CERT_FILE`、`TENUTO_SERVER_TLS_KEY_FILE` | Server 开启 mTLS 时出示的客户端证书与私钥 |

代理不转发浏览器的 Cookie 与 Authorization 头，路径只能落在 `/runtime/workflow/` 之下。这些变量与证书在第一次请求时读取，修改后重启 Console。Console 没有用户登录，持有 token 后能打开它的人就拥有 Runtime API 的权限：对外开放时放在你自己的 SSO 或鉴权代理之后（见[安全部署](/deploy/security#console-权限)）。`NEXT_PUBLIC_API_URL` 让浏览器直连 Server，只用于没有开启认证的开发环境。

Server 连不上时（Console 代理连接失败、网关返回非 Server 写出的 5xx，或直连时网络失败），页面显示 `Cannot connect to Tenuto Server at <地址>.` 与 Retry 按钮，地址是代理目标或 `NEXT_PUBLIC_API_URL`。Server 应答 401（开启了 `WORKFLOW_HTTP_AUTH_TOKEN`，Console 没有配置 `TENUTO_SERVER_AUTH_TOKEN` 或 token 不对）时，页面显示 `Tenuto Server rejected the Console's credentials (HTTP 401).` 并提示设置 `TENUTO_SERVER_AUTH_TOKEN` 后重启 Console；`NEXT_PUBLIC_API_URL` 直连时提示去掉它、改用代理。Server 自己返回的其他错误（包括 500 `internal server error`）显示为 `Failed to load <资源>: <Server 返回的文本>`，同样带 Retry。

```bash
NEXT_PUBLIC_API_PORT=7400 pnpm --dir console dev -- -p 7401
```
