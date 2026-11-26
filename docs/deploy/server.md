---
title: 部署 Tenuto Server
description: 构建、配置和运行正式 Tenuto Server
---

# 部署 Tenuto Server

`tenuto-server` 是 Workflow 控制面和状态机进程。生产后端使用 MySQL 保存 Workflow 状态，使用 TaskQ matching 承载任务队列。MySQL 后端下 matching 默认由本进程托管（`:7235`）；外置 matching 部署时设 `TASKQ_MATCHING_ENABLED=false`。

Workflow Task 默认也由本进程执行，部署只需 `tenuto-server` 和 Activity Worker。业务代码由 sdkgen 编译成 WLang Program 存在服务端，Workflow Task 只需要 Program 和 History，无需业务 Go 代码。

## 构建

```bash
make build-server \
  VERSION=1.0.0 \
  COMMIT="$(git rev-parse --short HEAD)"
```

产物位于：

```text
bin/tenuto-server
```

`go.mod` 的 `toolchain go1.27.4` 指定构建用的 Go 版本：本机 Go 较旧时，`go` 命令（`GOTOOLCHAIN=auto`，缺省值）自动下载这个版本。go1.27.4 修复了 Server 经过的标准库公告（`net/http` 的 GO-2026-6859 等）；设置 `GOTOOLCHAIN=local` 的构建环境需要自备 Go 1.27.4 或更高。

## 必需配置

```bash
export WORKFLOW_HTTP_ADDR=:7400
export WORKFLOW_BACKEND=mysql-taskq
export WORKFLOW_NODE_ID=tenuto-server-01
export WORKFLOW_DB_DSN='workflow:secret@tcp(mysql:3306)/workflow?charset=utf8mb4'
export WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64='<openssl rand -base64 32>'
export WORKFLOW_TASK_TOKEN_SECRET='<openssl rand -base64 32>'

export TASKQ_DB_DSN='taskq:secret@tcp(mysql:3306)/taskq?charset=utf8mb4'
export TASKQ_SERVICE_NAME=matching
# export TASKQ_MATCHING_ADDR=:7235  # 内嵌 matching 监听地址；多副本时设为各副本可达 IP
# export TASKQ_MATCHING_ADVERTISE_ADDR=10.0.0.5:7235  # 其他副本拨号用的地址，与监听地址不同时设置（代理、NAT）
export TASKQ_WORKFLOW_QUEUE=workflow-tasks
export TASKQ_ACTIVITY_QUEUES_JSON='{"default/default":{"physical":"workflow-activity-default"},"orders/payments":{"physical":"workflow-activity-orders-payments"}}'
```

`WORKFLOW_TASK_TOKEN_SECRET` 至少包含 32 字节，并在所有 Server 副本间保持一致。`WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64` 同样需要跨副本共享。

两个 DSN 的 `parseTime=true`、`loc=UTC`、`interpolateParams=true` 与 `clientFoundRows=false` 由 Server 设置，DSN 里写成别的值也不生效（`migrate` 相同）：

| 参数 | 作用 |
| --- | --- |
| `parseTime=true` | `DATETIME` 列读成时间值 |
| `loc=UTC` | 写入与读取的时间都按 UTC 解释 |
| `interpolateParams=true` | 驱动在客户端把参数插进 SQL 文本，每条带参数的语句一次往返。不插值时每条语句依次发 prepare、execute、close |
| `clientFoundRows=false` | upsert 撞键、没有改动列时 affected rows 为 0。仓储按这个值区分新写入与去重命中（事件去重、state 写入） |

经 `app.WithDatabase` 注入的 gorm 句柄不经过这一步，Server 启动时在 Workflow 库上执行一次回滚的 upsert 探测：句柄带 `clientFoundRows=true` 时启动失败。

连接字符集用 `utf8mb4`（表都是 `utf8mb4`）。`charset` 或 `collation` 是 GBK、GB18030、Big5、SJIS、CP932 这类多字节字符集时 Server 启动失败：它们的字符尾字节可能是反斜杠，客户端插值无法安全转义。

## 内嵌 Workflow Worker

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKFLOW_WORKER_ENABLED` | `true` | 在本进程内执行 Workflow Task；`false` 时由独立 workflow worker 消费 |
| `WORKFLOW_WORKER_CONCURRENCY` | `8` | Workflow Task 并发度 |
| `WORKFLOW_WORKER_TASK_TIMEOUT` | `10s` | 内嵌 Worker 单个 Workflow Task 的执行超时；Server 按它为该 Worker 领取的 Workflow Task 计时 |
| `WORKFLOW_WORKER_CACHE_MAX_ENTRIES` | `1000` | Sticky Execution Cache 的 session 上限 |
| `WORKFLOW_WORKER_CACHE_MAX_BYTES` | `268435456` | Sticky Execution Cache 的估算内存上限（256 MiB） |
| `WORKFLOW_WORKER_CACHE_IDLE_TTL` | `5m` | session 空闲逐出时间 |

Worker Identity 取 `WORKFLOW_NODE_ID`，该值在副本间必须唯一，Sticky Execution 依赖它路由。Build ID 取 Server 版本号。

设 `WORKFLOW_WORKER_ENABLED=false` 后，部署方自行运行 `Role: worker.RoleWorkflow` 的 Worker（包 `github.com/donutnomad/tenuto/sdk/worker`，见 [运行 Worker](/develop/workers)）消费 Workflow Task；未启动独立 worker 时 Run 停在等待 Workflow Task 的状态。内嵌 worker 与独立 worker 同时存在时共同消费同一 workflow 队列。

独立 workflow worker 部署先升级全部 Server，再升级 Worker。旧 Server 不识别 `CmdUpsertSearchAttributes`，新 Worker 首次走到 GetVersion 的 Workflow Task 会报 `workflow task command result missing` 并持续重试。

Workflow 定义由携带 `worker.Options.Workflows` 的 Worker 启动时登记，任何 `Role` 的 Worker 都可以携带。

## 可选 OpenTelemetry

```bash
export TENUTO_OTEL_ENABLED=true
export TENUTO_OTEL_TRACE_SAMPLE_RATIO=1
export TENUTO_OTEL_METRIC_EXPORT_INTERVAL=60s
export TENUTO_OTEL_LOG_LEVEL=INFO
export TENUTO_OTEL_SHUTDOWN_TIMEOUT=5s
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

Collector 使用 TLS 或认证 headers 时，通过标准 `OTEL_EXPORTER_OTLP_*` 变量配置。Server 退出时会在 `TENUTO_OTEL_SHUTDOWN_TIMEOUT` 内 flush traces、metrics 和 logs。

## 可选 CPU Profile

```bash
export WORKFLOW_PPROF_ADDR=127.0.0.1:6060
go tool pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
go tool pprof http://127.0.0.1:6060/debug/pprof/mutex
```

`WORKFLOW_PPROF_ADDR` 缺省关闭。开启后进程另开一个只提供 `/debug/pprof/` 的监听（CPU、heap、goroutine、mutex、block、trace），业务 HTTP 端口不提供这些路径。这个监听不鉴权，只绑定本机或内网地址；启动时记一条 WARN。

## 数据库 Schema

数据库要求 MySQL 8.0 或更高：全部表（含 TaskQ 的表）使用 `utf8mb4_0900_bin` 排序规则，标识按字节比较，区分大小写、重音与尾随空格；MySQL 5.7 没有这个排序规则，迁移失败。

Server 启动时不建表也不改表。表结构由二进制内嵌的版本化迁移维护，首次部署与每次升级都在启动新版本 Server 之前执行一次：

```bash
./bin/tenuto-server migrate
```

或使用 `make migrate`。`migrate` 只读 `WORKFLOW_DB_DSN` 与 `TASKQ_DB_DSN`（默认与 `WORKFLOW_DB_DSN` 相同），其余 Server 配置不需要：

| 库 | 执行的迁移 |
| --- | --- |
| Workflow 库（`WORKFLOW_DB_DSN`） | Tenuto 的迁移，建出并升级 `tenuto_*` 表 |
| TaskQ 库（`TASKQ_DB_DSN`） | TaskQ 的迁移，建出并升级 `taskq_*` 表 |

迁移记录在各自库的 `tenuto_schema_migrations` 表里（迁移编号、文件名、内容的 SHA-256、执行时刻），两个库是同一个库时按 `component`（`tenuto`、`taskq`）区分。

- **权限**：`migrate` 需要 DDL 权限（CREATE、ALTER、INDEX、DROP）。Server 运行时只读写数据，可以只给执行 `migrate` 的环境配置有 DDL 权限的账号。
- **幂等与并发**：已执行的迁移不再执行。多个进程同时执行 `migrate` 时，MySQL 命名锁（`GET_LOCK`，按库区分）让它们依次进行，后到的进程看到迁移已执行后直接退出。`migrate` 可以作为发布流水线里的一步或 Kubernetes Job 运行，也可以作为每个副本的 init container 运行。
- **只向前**：没有回滚迁移。每个迁移保持上一个版本的 Server 可用（先加表、加列，下一个版本再删除旧列），滚动升级期间旧副本可以继续运行和重启，回滚二进制不需要回滚库。
- **中途失败**：MySQL 的 DDL 隐式提交，一个迁移不能整体回滚。`migrate` 每执行成功一条语句记录一次进度；某条语句失败时报出迁移名与语句序号（如 `tenuto migration 0002_add_x failed at statement 2 of 3`），之前的语句已经生效。排除原因（例如清理违反新唯一键的数据）后重新执行 `migrate`，从失败的那条语句继续。

Server 启动时比较库里的迁移记录与本二进制：

| 库的状态 | Server 行为 |
| --- | --- |
| 已执行完本二进制的全部迁移 | 正常启动 |
| 没有迁移记录表，或有未执行、中途失败的迁移 | 启动失败，报出当前版本、需要的版本与待执行的迁移，提示运行 `tenuto-server migrate` |
| 已执行迁移的内容与本二进制不同（checksum 不一致） | 启动失败 |
| 库比本二进制新（新版本已执行 `migrate`） | 记警告 `database schema is newer than this binary` 并正常启动 |

升级顺序：先执行新版本的 `tenuto-server migrate`，再滚动替换 Server 副本。

仓库里的 `server/infra/sql/schema.sql` 是 Workflow 库当前的完整表结构，供阅读；部署用 `migrate`。

## 启动

```bash
./bin/tenuto-server
```

或使用：

```bash
make run-server
```

## 健康检查

| 路径 | 用途 |
| --- | --- |
| `GET /health/live` | 进程存活 |
| `GET /health/ready` | 数据库、matching（内嵌模式含 matching 自身在服务）、恢复和路由就绪。数据库检查用每个连接池里一条专用连接 Ping，不在业务连接池上排队：池被业务请求占满时 readiness 照常通过，数据库失联时失败 |

Tenuto Server 在完成 Run 恢复、pending task 补投递和 sticky assignment 校正后开放 readiness。

## 优雅退出

收到 `SIGTERM` 后，Server 会撤销 readiness、结束进行中的长轮询（`poll` 应答 204，`wait-closed` 应答 204，`update` 返回当前阶段，客户端照常重来）、取消内嵌 workflow worker 的 poll 与在途 Workflow Task、停止 reconciliation、Schedule 与 Batch Operation 驱动与 retention GC（处理到一半的 Batch 页不保存，由下一任驱动从游标重做），等各自的任务退出后释放 `tenuto_leader_lock` 里持有的锁，其他副本在下一次选主轮询（Schedule 与 Batch 驱动 5s，reconciliation 与 GC 10s）时接任，不等租约到期、排空 HTTP 请求、等待内嵌 workflow worker 退出（在途 lease 完成 ack/nack）、关闭 Timer、TaskQ Frontend、内嵌 matching（释放 7235）、membership 观察与数据库连接。被取消的 Workflow Task 经 nack 或超时重新投递，由其他副本执行。

其余在途请求（Signal、Cancel、Activity 完成等写请求）不随停机取消，在 `WORKFLOW_HTTP_SHUTDOWN_TIMEOUT` 内照常做完并应答结果。请求在生效之前被取消（例如客户端断开时仍在等同一个 Workflow 的锁）时应答 `503` 与 `Retry-After`，没有写入任何内容；进入事务的请求要么提交并应答成功，要么不提交。Go SDK 把 2xx 空应答体当作传输错误重试，不当作成功。
