---
title: 安全部署
description: Tenuto 网络边界、鉴权、凭证、数据与高风险操作保护
---

# 安全部署

Tenuto Server 内置两种可选的身份认证，缺省都关闭：

- **Bearer Token**：配置 `WORKFLOW_HTTP_AUTH_TOKEN` 后，`/runtime/*` 与 `/admin/*` 要求 `Authorization: Bearer <token>`。
- **mTLS**：配置 `WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` 后，HTTP 监听在 TLS 握手阶段要求并校验客户端证书。

两者可以同时开启。它们只做认证，不做授权：通过认证的调用方拥有全部 Namespace 的全部权限（见下一节）。按 Namespace 或按调用方分权需要在网关按路径分组授权。Task Token 只校验请求属于哪个任务租约，不校验调用方是谁。Server、TaskQ、数据库照常限制在受控网络内。

## 调用方的权限

两种认证都关闭时，能访问 Server HTTP 端口的调用方就是下面的「调用方」；开启后是持有 token 或客户端证书的调用方。它拥有全部 Namespace 的全部权限：

- 读：任何 Run 的输入、输出、History（含 Activity 输入输出、Signal、Update、心跳详情）、Search Attributes、Workflow Config（`list`、`get-run`、`history`、`activity-states`、`get-config`、`list-configs`、`query`）。Namespace 只是寻址范围，不是权限边界。
- 写：启动、Signal、Update、取消、终止、Reset 任何 Run，修改 Run 的版本策略与 Activity Worker target，创建、触发、删除 Schedule，启动覆盖整个 Namespace 的 Batch Operation。
- 登记并激活 Workflow 定义（`register`、`artifacts`）：之后启动的 Run 在每个副本的内嵌 Workflow Worker 上执行这份 WLang Program。Program 只能调用 Workflow API 与确定性的标准库函数，不能读写文件、网络或进程；它可以调度任意 Activity Type 到任意 Task Queue，由该队列上的 Activity Worker 用调用方给的输入执行。
- 冒充 Worker：长轮询领取 Workflow Task 与 Activity Task（拿到任务输入），并以任意结果完成它们。
- 修改 TaskQ 物理队列的分区数（`POST /admin/partitions`，只增不减，上限 1024）。
- `WORKFLOW_PPROF_ADDR` 开启时，从 pprof 监听读取内存、goroutine 与 CPU profile（含内存中的业务数据）。pprof 是独立监听，不经过下面的 token 与 mTLS 校验。

## Bearer Token

```bash
WORKFLOW_HTTP_AUTH_TOKEN=<32 字节以上的随机串>
# 或从 Secret Manager 挂载的文件读取（二选一）
WORKFLOW_HTTP_AUTH_TOKEN_FILE=/run/secrets/tenuto-token
```

- 受保护的路径：`/runtime/*` 与 `/admin/*`。`/health/live`、`/health/ready` 不校验（编排平台探针用）；Server 的 metrics 经 OTLP 推送，不在 HTTP 端口上；`WORKFLOW_PPROF_ADDR` 的 pprof 监听不校验，只绑定本机或运维网段。
- 缺少凭证应答 `401` 与 `WWW-Authenticate: Bearer realm="tenuto"`，凭证不对时再带 `error="invalid_token"`。未就绪的副本同样先校验凭证。
- token 至少 16 字节，只含可见 ASCII 字符（`openssl rand -hex 32` 生成）。Server 只保存 token 的 SHA-256，按定长摘要做常数时间比较，比较耗时与请求里的 token 无关。
- 轮换：变量里写多个 token，以逗号分隔（文件里也可以每行一个），任一个都通过。先给所有副本加上新 token（`old,new`），把 Client、Worker、Console 切到新 token，再去掉旧 token。文件只在启动时读取，改动后滚动重启副本生效。
- token 以明文随请求发送，生产环境同时开启 HTTPS（`WORKFLOW_HTTP_TLS_CERT_FILE`、`WORKFLOW_HTTP_TLS_KEY_FILE`）或只在受控网络里明文传输。

调用方的配置：

| 调用方 | 配置 |
| --- | --- |
| Go SDK（Client 与从它创建的 Worker） | `client.Options.AuthToken`，见 [Client](/sdk/client) |
| Console | `TENUTO_SERVER_AUTH_TOKEN`（只在 Console 服务端读取），见下文 [Console 权限](#console-权限) |
| `tenuto-workflow-gen -url` | 环境变量 `TENUTO_AUTH_TOKEN` |
| curl 与其他 HTTP 客户端 | `-H "Authorization: Bearer $TOKEN"` |

## mTLS

```bash
WORKFLOW_HTTP_TLS_CERT_FILE=/etc/tenuto/tls/server.pem
WORKFLOW_HTTP_TLS_KEY_FILE=/etc/tenuto/tls/server-key.pem
WORKFLOW_HTTP_TLS_CLIENT_CA_FILE=/etc/tenuto/tls/client-ca.pem
WORKFLOW_HEALTH_ADDR=:7401   # 明文健康检查端口，供探针使用
```

`WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` 需要同时配置服务端证书与私钥，否则 Server 启动失败。开启后 HTTP 监听用 `RequireAndVerifyClientCert`：没有出示证书或证书不是这份 CA 签发的连接在握手阶段被拒绝，请求到不了任何路径。Server 只校验证书链，不按证书的 Subject 区分调用方。

健康检查：mTLS 在 TLS 层生效，主端口上的 `/health/*` 同样要求客户端证书，Kubernetes 的 `httpGet` 探针出示不了证书。配置 `WORKFLOW_HEALTH_ADDR` 后 Server 另开一个明文 HTTP 监听，只提供 `/health/live` 与 `/health/ready`（其他路径 404），探针指向这个端口；它只暴露就绪状态，绑定 Pod 网络即可。不配置时探针改用能出示证书的 `exec` 探针。

调用方：Go SDK 用 `client.Options.TLS`（`Certificates` 放客户端证书，`RootCAs` 放签发 Server 证书的 CA）；Console 用 `TENUTO_SERVER_TLS_CA_FILE`、`TENUTO_SERVER_TLS_CERT_FILE`、`TENUTO_SERVER_TLS_KEY_FILE`。SDK 把 TLS 握手被拒（Server 不信任客户端证书、客户端不信任 Server 证书）视为配置错误，不重试。

## 网关授权

内置认证不区分调用方。需要按调用方或 Namespace 分权时，网关按下面的路径分组授权：

| 调用方 | 路径 |
| --- | --- |
| Workflow Worker（独立部署时） | `/runtime/workflow-task/*`、`artifacts`（Worker 启动时登记随二进制发布的 Program） |
| Activity Worker | `/runtime/activity/*`、`register-activity-worker-version`、`heartbeat-activity-worker-version`、`deregister-activity-worker-version`、`artifacts`（同上） |
| 业务 Client | `start`、`start-version`、`signal`、`signal-with-start`、`update`、`update-with-start`、`query`、`get-run`、`wait-closed`、`list`、`history`、`history-page`、`search-attributes`、`cancel` |
| 只读观测（Console） | `list*`、`get-*`、`history*`、`query`、`activity-states`、`run-stats`、`capabilities` |
| 运维写操作 | `terminate`、`reset`、`reset-activity`、`set-policy`、`set-worker-target`、`register`、Schedule 与 Batch 的写端点、`dispatch-pending` |
| 管理 | `/admin/partitions` |

没有写出前缀的路径都在 `/runtime/workflow/` 下。`artifacts` 登记新内容时把它设为 latest：持有 Worker 凭证的调用方因此也能发布 Program，Worker 凭证按发布凭证保管。网关要求其他凭证（按请求刷新的 token、签名头）时，Go SDK 的 `client.Options.HTTPClient` 接受自定义 `*http.Client`，在它的 Transport 上附加；Worker 沿用 Client 的连接。

所有 POST 必须带 `Content-Type: application/json`，其他取值应答 415。浏览器跨站提交表单或 `text/plain` 请求不经 CORS 预检，Server 拒绝这类请求，第三方网页因此不能借受信网络里运维人员的浏览器调用 Runtime API；Server 不应答 CORS 预检。

## 网络边界

- Runtime HTTP API 只暴露给业务服务、受控 Worker 网络和 Console 所在的网关。
- `/admin/*` 只对运维网段开放。
- 数据库只允许 Server 访问。
- TaskQ Frontend 只允许 Server 和 Worker 访问。
- 内嵌 matching 的 gRPC 端口（默认 7235）只需在 Server 副本之间可达，不对 Worker、业务服务或外部网络开放。
- 健康检查端点（`/health/live`、`/health/ready`，开启 mTLS 时是 `WORKFLOW_HEALTH_ADDR`）可由编排平台访问。
- `WORKFLOW_PPROF_ADDR` 只在排查时开启，绑定本机或运维网段。

## 资源上限

| 项目 | 上限 |
| --- | --- |
| 请求体 | 4 MiB，超出应答 413 |
| 单个 Run 的 History | 51,200 个事件或 50 MiB，超出时 Run 以 `HistoryLimitExceeded` 失败 |
| Run List 一页 / 一次 `run_ids` | 1000 |
| History 分页一页 | 1000 个事件 |
| 长轮询（`poll`、`wait-closed`） | 60s |
| WLang 执行 | 每个 Workflow Task 100 万条语句（每个 Workflow Task 重新计数）、1024 个协程；调用深度 128、数组 10 万元素、对象 10 万键、单个字符串 16 MiB；每个 Workflow Task 受 `WORKFLOW_WORKER_TASK_TIMEOUT` 限时 |

超出 WLang 预算的 Run 以 `E_BUDGET` 失败，副本照常运行。字符串拼接、`strings.Repeat`、`fmt.Sprintf` 等可以放大数据的操作在分配之前按参数算出结果大小，超出 16 MiB 时直接失败（见 [执行预算](/develop/go-subset#执行预算)）。这些上限针对单个值：Program 仍可以在步数预算内创建许多接近上限的值，合计内存由副本的内存限制兜底。只登记来自受信构建流水线的 Program，并把 `register`、`artifacts` 端点限定给发布流程与 Worker。

## 凭证

Runtime API token（`WORKFLOW_HTTP_AUTH_TOKEN_FILE`）、mTLS 私钥、Task Token Secret、History Page Token Key、数据库密码、TaskQ TLS 私钥和外部 Activity 凭证使用 Secret Manager 注入。Server 不在日志中输出 DSN、Secret 与用户载荷；500、502、503 应答只返回固定文本，原因写入 Server 日志。4xx 应答的错误文本可能引用请求里的字段值。Trace 与 metrics 导出 Namespace、Workflow ID、Run ID、Workflow Type、Activity Type、Update 名称等标识与错误文本，不导出载荷；业务 ID 本身敏感时不要把它写进 Workflow ID。

## 数据保护

Workflow 输入、Activity 结果、Signal、Update 和 Search Attributes 可能包含业务数据，Server 原样保存在数据库中。按 Namespace 划分访问权限需要网关按请求体的 `namespace` 字段授权；为 History、备份和导出设置保留期限（见 [配置项](/reference/configuration#retention-gc)）。

## Console 权限

Console 没有用户登录。浏览器调用 Console 自己的 `/runtime/workflow/*`，Console 的服务端把请求转发到固定的 `TENUTO_SERVER_URL`，并加上 `Authorization: Bearer $TENUTO_SERVER_AUTH_TOKEN` 与可选的 mTLS 客户端证书。token 与证书只在 Console 服务端进程里，不进入浏览器；代理不转发浏览器的 Cookie 与 Authorization 头，只转发 `/runtime/workflow/` 下的 POST，目标主机固定，请求路径不能改变它。

Console 因此持有 Server 的凭证：能打开 Console 的人就拥有 Runtime API 的全部 Workflow 权限（终止、Reset、登记 Program 等）。对外开放的 Console 必须放在你自己的 SSO 或鉴权代理之后。Trace 支持查看变量、Activity 输入输出和子 Workflow 栈；需要拆分只读观测、Run 控制、Reset 和终止权限时，在 Console 前面的代理按上面的路径分组授权，并记录每次人工操作。`NEXT_PUBLIC_API_URL` 让浏览器直连 Server、绕过这个代理，Server 开启认证后不能使用。
