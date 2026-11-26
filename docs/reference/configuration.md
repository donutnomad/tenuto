---
title: 配置项
description: Tenuto Server 与 TaskQ 环境变量参考
status: Stable
---

# 配置项

<FeatureStatus status="Stable" />

## Server 基础配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKFLOW_HTTP_ADDR` | `:7400` | HTTP 监听地址 |
| `WORKFLOW_BACKEND` | `mysql-taskq` | `mysql-taskq` 或 `memory` |
| `WORKFLOW_NODE_ID` | hostname | Server 节点唯一标识 |
| `WORKFLOW_STARTUP_TIMEOUT` | `30s` | 数据库、membership、路由与内嵌 Workflow Worker 的启动期限；启动恢复在后台运行，不计入 |
| `WORKFLOW_RECONCILE_INTERVAL` | `30s` | 周期补偿的间隔：按到期索引只处理有到期或待处理事项的 Running Run（截止时间、timer、Activity 超时在两个周期内到期，或有 Scheduled Activity、open / 待调度的 Workflow Task、blocked），补投任务、接管到期定时器；MySQL 部署只有选为 leader 的副本执行 |
| `WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL` | `10m` | 全量补偿的间隔：按游标逐页检查全部 Running Run，补齐没有到期索引的崩溃窗口（子 Run 补建、子 Run 终态传播、终态写回 Run、父关闭策略、子 Run 取消请求补发），并删除到期超过一天、没有 Running Run 指定（Target Worker）的 Activity Worker 登记；与周期补偿同一 leader 执行 |
| `WORKFLOW_TASK_TOKEN_SECRET` | 生产后端必填 | Task Token HMAC Secret，至少 32 字节；所有 Server 副本配置同一值，任一副本签发的 Token 才能被其他副本校验；memory 后端提供开发默认值 |
| `WORKFLOW_WORKER_ENABLED` | `true` | 在本进程内执行 Workflow Task；`false` 时由独立 workflow worker 消费 |
| `WORKFLOW_WORKER_CONCURRENCY` | `8` | 内嵌 workflow worker 并发度 |
| `WORKFLOW_WORKER_TASK_TIMEOUT` | `10s` | 内嵌 Worker 的 Workflow Task 执行超时，Server 按它计时 |
| `WORKFLOW_WORKER_CACHE_MAX_ENTRIES` | `1000` | Sticky Execution Cache 的 session 上限 |
| `WORKFLOW_WORKER_CACHE_MAX_BYTES` | `268435456` | Sticky Execution Cache 的估算内存上限 |
| `WORKFLOW_WORKER_CACHE_IDLE_TTL` | `5m` | Sticky Execution Cache 空闲逐出时间 |
| `WORKFLOW_QUERY_CONCURRENCY` | `8` | 本进程同时重放 History 求值 Query 的上限；超出的 Query 立即应答 503 与 `Retry-After`，Go SDK 退避重发。`__stack_trace` 不占名额 |
| `WORKFLOW_ARTIFACTS` | 空 | 启动时加载的生成产物目录，逗号分隔，递归查找 `workflow.json`；空表示不加载 |
| `WORKFLOW_ARTIFACTS_NAMESPACE` | `default` | `WORKFLOW_ARTIFACTS` 中产物登记到的 Namespace |
| `WORKFLOW_PPROF_ADDR` | 空 | 非空时在这个 `host:port` 上另开 `net/http/pprof` 监听（`/debug/pprof/`），并开启 mutex（1/100）与 block（阻塞超过 100µs）采样；空表示关闭。调试用，不鉴权、不走业务 HTTP，只绑定本机或内网地址，不要暴露到公网 |
| `WORKFLOW_ARTIFACTS_PUBLISH` | `false` | `true` 时启动加载把 latest 指向这些产物；`false` 只登记，旧 Worker 重启不会把 latest 拉回旧版本。取值按 Go `strconv.ParseBool` 解析，非法取值（如 `ture`）使启动失败 |

## HTTP

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKFLOW_HTTP_SHUTDOWN_TIMEOUT` | `20s` | 优雅退出期限：收到 `SIGTERM` 时在途的非长轮询请求在这个期限内照常做完并应答结果 |
| `WORKFLOW_HTTP_MAX_IN_FLIGHT` | `4 × WORKFLOW_DB_MAX_OPEN_CONNS`（缺省 200） | `/runtime/*` 上同时处理的非长轮询请求数上限，必须为正整数。超出时立即应答 `503` 与 `Retry-After: 1`，请求没有被处理，可以原样重发；长轮询（`workflow-task/poll`、`activity/poll`、`wait-closed`）不计入；`update` 与 `update-with-start` 在拿锁与写库阶段计入，进入等结果的长等待之前归还名额 |
| `WORKFLOW_HTTP_READ_HEADER_TIMEOUT` | `5s` | Header 读取超时 |
| `WORKFLOW_HTTP_READ_TIMEOUT` | `30s` | 读完整个请求（含 body）的期限；`0` 表示不限制，不可为负 |
| `WORKFLOW_HTTP_WRITE_TIMEOUT` | `2m` | 从读完请求头到写完响应的期限；必须大于长轮询上界 `1m`，或设为 `0` 不限制，否则启动失败 |
| `WORKFLOW_HTTP_IDLE_TIMEOUT` | `2m` | Keep-alive 空闲超时 |
| `WORKFLOW_HTTP_TLS_CERT_FILE` | 空 | Server Certificate |
| `WORKFLOW_HTTP_TLS_KEY_FILE` | 空 | Server Private Key |
| `WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` | 空 | 非空时开启 mTLS：HTTP 监听要求并校验由这份 CA（PEM）签发的客户端证书；需要同时配置证书与私钥，否则启动失败 |
| `WORKFLOW_HEALTH_ADDR` | 空 | 非空时另开一个明文 HTTP 监听（`host:port`），只提供 `/health/live` 与 `/health/ready`，供开启 mTLS 后的编排平台探针使用 |
| `WORKFLOW_HTTP_AUTH_TOKEN` | 空 | 非空时 `/runtime/*` 与 `/admin/*` 要求 `Authorization: Bearer <token>`，`/health/*` 不校验。多个 token 以逗号分隔，任一个通过（轮换）；每个至少 16 字节、只含可见 ASCII。见[安全部署](../deploy/security.md#bearer-token) |
| `WORKFLOW_HTTP_AUTH_TOKEN_FILE` | 空 | 从文件读取 token（逗号或换行分隔），与 `WORKFLOW_HTTP_AUTH_TOKEN` 二选一；只在启动时读取 |

## Workflow 数据库

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKFLOW_DB_DSN` | 生产后端必填 | MySQL DSN；`tenuto-server migrate` 在这个库上执行 Tenuto 的迁移。`parseTime=true`、`loc=UTC`、`interpolateParams=true` 由 Server 设置；字符集用 `utf8mb4`，GBK、Big5、SJIS 等不能与客户端插值同用的字符集启动失败（见[部署 Tenuto Server](../deploy/server.md#必需配置)）。DSN 没有给出 `readTimeout` / `writeTimeout` 时，Server 设为 `60s` / `30s`：数据库失联或主从切换时，挂在连接上的读写在这个期限内以连接错误返回（应答 503）；`readTimeout` 大于 `innodb_lock_wait_timeout` 的缺省值 50s，锁等待照常以 `1205` 返回。`tenuto-server migrate` 不设这两项 |
| `WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64` | 生产后端必填 | Base64 编码的 History Page Token HMAC Key，解码后至少 32 字节 |
| `WORKFLOW_DB_MAX_OPEN_CONNS` | `50` | 最大连接数；Workflow 与 TaskQ 两个连接池各自的上限，所有副本合计最多 `副本数 × 2 × 该值`，要小于 MySQL 的 `max_connections`（见[数据库连接数](../deploy/production.md#数据库连接数)） |
| `WORKFLOW_DB_MAX_IDLE_CONNS` | `10` | 最大空闲连接数，同样作用于两个连接池；不能大于 `WORKFLOW_DB_MAX_OPEN_CONNS` |
| `WORKFLOW_DB_CONN_MAX_LIFETIME` | `30m` | 连接最大生命周期 |
| `WORKFLOW_DB_CONN_MAX_IDLE_TIME` | `5m` | 空闲连接生命周期 |

## TaskQ

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TASKQ_DB_DSN` | Workflow DSN | TaskQ MySQL DSN；`tenuto-server migrate` 在这个库上执行 TaskQ 的迁移。参数要求同 `WORKFLOW_DB_DSN` |
| `TASKQ_SERVICE_NAME` | `matching` | matching 服务名 |
| `TASKQ_WORKFLOW_QUEUE` | `workflow-tasks` | Workflow physical queue |
| `TASKQ_WORKFLOW_QUEUE_PARTITIONS` | 空 | 启动时把 Workflow physical queue 的读写分区数设为该值，取 `1`–`1024`；空表示不覆盖 |
| `TASKQ_ACTIVITY_QUEUES_JSON` | 空 | 逻辑路由到 physical queue 的映射，值为 `{"physical":"<队列名>","partitions":<分区数>}`，`partitions` 取 `1`–`1024`，可省略；未配置的路由使用 `activity:<namespace>/<task_queue>` |
| `TASKQ_POLL_TIMEOUT` | `30s` | Frontend 长轮询期限；内嵌 Workflow Worker 的一次 Workflow Task 长轮询也持续这么久 |
| `TASKQ_MEMBERSHIP_POLL_INTERVAL` | `3s` | membership 刷新周期 |
| `TASKQ_MEMBERSHIP_REPLICA_POINTS` | `200` | 一致性哈希虚拟节点数 |
| `TASKQ_MATCHING_ENABLED` | `true` | 在本进程内托管 taskq matching；`false` 表示连接外置 matching |
| `TASKQ_MATCHING_ADDR` | `:7235` | 内嵌 matching 的 gRPC 监听地址；host 是具体 IP 时该 IP 即广播地址，host 为空或主机名时广播本机非回环 IP |
| `TASKQ_MATCHING_ADVERTISE_ADDR` | 空 | 写进成员表、供其他副本拨号的 matching 地址（`host:port`，端口 1–65535，host 按 `TASKQ_MATCHING_ADDR` 的规则解析成 IP）；空表示由 `TASKQ_MATCHING_ADDR` 推导。其他副本经代理、NAT 或端口映射才能到达本副本时设置 |

## Retention GC

终态 Run 的数据回收默认关闭。`WORKFLOW_RETENTION` 为 `0` 时其余 GC 变量不生效；开启时只支持 `mysql-taskq` 后端，memory 后端配置非零值启动失败。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WORKFLOW_RETENTION` | `0` | Run 进入终态到可回收之间的最小间隔；`0` 关闭 GC，开启时至少 `1h` |
| `WORKFLOW_RETENTION_JITTER` | `30m` | 叠加在到期时刻上的随机抖动上界，避免同一时刻关闭的大批 Run 同时回收 |
| `WORKFLOW_GC_SCAN_INTERVAL` | `30s` | 回收扫描两轮之间的间隔，也是一轮扫描的时长上限：一轮一批批处理到期的 History tree，直到没有到期任务或用满这个时长 |
| `WORKFLOW_GC_BATCH` | `100` | 每次读取到期删除任务的条数；只影响每批的大小，一轮扫描处理的 tree 数不受它限制 |
| `WORKFLOW_GC_SCAVENGE_INTERVAL` | `24h` | 孤儿兜底扫描的间隔，回收主路径够不到的残留（没有 tree 的终态 Run、Run 已删除但事件仍在） |
| `WORKFLOW_GC_SCAVENGE_MIN_AGE` | `168h` | 兜底扫描只回收比该值更老的数据，至少 `1h` |
| `WORKFLOW_GC_SCAVENGE_BATCH` | `200` | 兜底扫描单轮每类孤儿的处理上限 |

## TaskQ TLS

| 变量 | 说明 |
| --- | --- |
| `TASKQ_TLS_ENABLED` | 启用 gRPC TLS |
| `TASKQ_TLS_CA_FILE` | CA Certificate |
| `TASKQ_TLS_SERVER_NAME` | TLS Server Name |
| `TASKQ_TLS_CERT_FILE` | mTLS Client Certificate |
| `TASKQ_TLS_KEY_FILE` | mTLS Client Private Key |

## OpenTelemetry

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TENUTO_OTEL_ENABLED` | `false` | 启用 OTLP traces、metrics 和 logs |
| `TENUTO_OTEL_TRACE_SAMPLE_RATIO` | `1` | Parent-based trace 采样比例，范围 `(0, 1]` |
| `TENUTO_OTEL_METRIC_EXPORT_INTERVAL` | `60s` | metrics 周期导出间隔 |
| `TENUTO_OTEL_LOG_LEVEL` | `INFO` | stdout 与 OTLP logs 的最低级别 |
| `TENUTO_OTEL_SHUTDOWN_TIMEOUT` | `5s` | traces、metrics 和 logs flush/close 期限 |

OTLP 连接使用 OpenTelemetry 标准环境变量，例如 `OTEL_EXPORTER_OTLP_ENDPOINT`、`OTEL_EXPORTER_OTLP_HEADERS`、`OTEL_EXPORTER_OTLP_INSECURE` 和各 signal 专用的 `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`、`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`、`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`。`OTEL_SERVICE_NAME` 覆盖默认服务名 `tenuto-server`，`OTEL_RESOURCE_ATTRIBUTES` 补充部署资源属性。

## 后端模式

`WORKFLOW_BACKEND=memory` 用于本地开发和自动化测试，Workflow state 与 queue 都保存在进程内。`WORKFLOW_BACKEND=mysql-taskq` 启用 MySQL 持久化和 TaskQ 路由，并要求配置数据库 DSN、History Token Key 与 Task Token Secret；Activity Queue 映射 `TASKQ_ACTIVITY_QUEUES_JSON` 可选，未配置的路由使用 `activity:<namespace>/<task_queue>`。

TLS Certificate 与 Private Key 需要成对配置。启用任一 TaskQ TLS 文件或 Server Name 时，同时设置 `TASKQ_TLS_ENABLED=true`。
