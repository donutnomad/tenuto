---
title: 生产环境拓扑
description: Tenuto Server、MySQL、TaskQ、Worker 与 Console 的生产部署建议
---

# 生产环境拓扑

上线前的配置清单、保证与已知限制见 [生产就绪与验证结果](/operations/production-readiness)。

推荐将控制面、Activity Worker 和 Console 独立部署。MySQL 后端下 TaskQ matching 与 Workflow Task 执行默认随 Tenuto Server 进程托管，不再单列实例。需要独立扩缩 Workflow Task 算力时，设 `WORKFLOW_WORKER_ENABLED=false` 并部署独立 Workflow Worker Pool。

```text
Ingress / mTLS Gateway
          │
     Tenuto Server × N（内嵌 matching :7235 与 workflow worker）
       │          │
       │          ├── Workflow MySQL
       │          └── TaskQ MySQL
       │
       ├── Payments Activity Pool
       └── Notification Activity Pool

Tenuto Console → Gateway → Tenuto Server
```

## Server 副本

- 所有副本共享 Workflow 数据库和 TaskQ 数据库。
- Task Token Secret 与 History Page Token Key 跨副本一致。
- `WORKFLOW_NODE_ID` 在副本间唯一，它同时是分区租约的持有者。
- 每个副本的成员表广播地址互不相同：默认取本机非回环 IP；多网卡或容器网络中把 `TASKQ_MATCHING_ADDR` 设为该副本可达的 IP（如 Pod IP）；其他副本要经代理、NAT 或端口映射才能到达时，把 `TASKQ_MATCHING_ADVERTISE_ADDR` 设为它们拨号用的 `host:port`，监听地址不变。
- 副本之间必须能互访 matching 端口（默认 7235）；frontend 会把分区流量转给持有该分区的 peer。
- readiness 成功后接入 Runtime 流量。
- Schedule leadership 由数据库协调。
- 同一个 Run 可以同时被多个副本处理（Worker 的完成上报、Signal、set-policy、定时器到期分别落在不同副本）。副本之间的互斥由数据库行锁负责：每个写事务先以 `SELECT ... FOR UPDATE` 锁住该 Run 的 execution state 行，再在锁下读取 Run，依据锁下读到的状态、策略、配置错误标记与当前内容版本做判定。另一个副本已提交的改动因此总在判定之内，例如 set-policy 改成 pinned 之后，任何副本领取的 Workflow Task 都不会再采用 latest。进程内的 per-workflow 锁只减少本副本内的争用，不承担跨副本的正确性。数据库保持 MySQL 默认的 REPEATABLE READ 隔离级别。
- 周期 reconciler 经数据库选主（与 Schedule、保留期 GC 同一张 leader 表，lock 名 `workflow:reconciler`），同一时刻只有一个副本做补偿，副本数不放大扫描量。周期补偿（`WORKFLOW_RECONCILE_INTERVAL`，默认 30s）按到期索引只读有到期或待处理事项的 Run，代价与这些事项数成正比，与 Running 的 Run 总数无关；全量补偿（`WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL`，默认 10m）按游标逐页检查全部 Running 的 Run，补齐崩溃在子 Run 建好之前，或终态传播、父关闭策略、子 Run 取消请求发出之前留下的后续动作。
- Batch Operation 的后台驱动同样经数据库选主（lock 名 `workflow:batch-driver`）：任一副本受理的 Batch 都写入数据库，由 leader 副本分页处理；leader 下线后新 leader 从记录保存的游标继续。
- Server 重启不会把仍在 TaskQ 里的 Workflow Task 与 Activity 任务再入队一次；启动恢复只重新投递派发记录显示未入队（入队失败、租约过期）的任务。启动恢复在后台运行，与正常服务并发，不计入 `WORKFLOW_STARTUP_TIMEOUT`：Running 的 Run 多或分区正在易主时，副本照常就绪，恢复完成时记 `startup recovery finished`，未完成的部分由 reconciler 继续补偿。
- 副本在长轮询期限到期、调用方取消或多个 matching owner 竞速时都不丢投递：TaskQ 已签发的租约总会被读回，没人要的立即放回队列，不等 60s 可见性超时。写入任务时 matching owner 宕机（成员表约 20s 后才剔除它），入队换到其他 owner 的分区。已入队、超过等待时长（30s 宽限期起按派发次数翻倍，最长 8 分钟）仍没有被领取的 open Workflow Task 与 Scheduled Activity attempt 由 reconciler 再投递一份，多出的投递在领取时丢弃，日志 `task dispatched but not claimed; redelivering`。
- 空闲时的数据库访问：TaskQ 分区的行都已投递或确认时，poll、等待中的长轮询与可见性扫描不读库；内嵌 Workflow Worker 的一次 Workflow Task 长轮询持续 `TASKQ_POLL_TIMEOUT`（默认 30s），不再每秒重新开始。仍按节拍运行的有：sticky 期限扫描（每个副本最多每秒一次，随 Workflow Task 轮询开始触发）、TaskQ 分区 GC（每个分区每 5s，`range_id` CAS 同时用于发现 ownership 丢失）、分区锁续约（每个分区每秒一次）与锁成员行刷新（每个分区每 5s，过期清理每 15s）、成员表心跳、Schedule 与 Batch 驱动、leader 的 reconciler。
- 等待结果的请求跨副本唤醒：Update 等 `Completed` 与 `WorkflowRun.Get` 用的 `wait-closed` 在受理请求的副本上等待，完成 Update 或结束 Run 的通常是另一个副本。有等待者的副本每 100ms 用一条按主键的批量查询读取被等待 Run 的 `next_event_id`，变化时唤醒等待者重读，因此其他副本提交的结果最多晚 100ms 被看到；没有等待者的副本不读库。副本重启时进行中的等待随连接断开，调用方对同一个句柄再调用一次。见 [跨副本唤醒](/develop/updates#跨副本唤醒)。
- 副本可以直接缩容或永久下线：leader 副本的 reconciler（`WORKFLOW_RECONCILE_INTERVAL`，默认 30s）按持久化状态接管下线副本的定时器。过期超过 30s 宽限期的 durable timer、Activity 超时、Run 超时和失败 WFT 的退避会被触发，接管最长延迟约为宽限期加一个 reconcile 周期。

## 数据库连接数

每个副本有两个连接池：Workflow 库与 TaskQ 库各一个（`TASKQ_DB_DSN` 指向同一个库时同样是两个池）。`WORKFLOW_DB_MAX_OPEN_CONNS`（缺省 50）是**每个**池的上限，所以所有副本合计最多打开

```text
副本数 × 2 × WORKFLOW_DB_MAX_OPEN_CONNS + migrate 与运维工具的余量  <  MySQL max_connections
```

三副本用缺省值时最多 300 条，超过 MySQL 缺省的 `max_connections`（151）。超出时新连接得到 `Error 1040: Too many connections`，Runtime API 对幂等调用应答 503（SDK 重试），对 Reset 等调用应答 502（见 [HTTP API 失败应答](/reference/http-api#失败应答)）。按上式调低 `WORKFLOW_DB_MAX_OPEN_CONNS` 或调高 MySQL 的 `max_connections`；同一个 MySQL 还服务其他应用时把它们的连接一起算进去。`tenuto-server migrate` 用 2 条连接。

长轮询不占数据库连接：Workflow Task 与 Activity 的 poll、`wait-closed`、Update 等待都在内存里等通知，有任务或结果时才执行短查询。稳态连接数随请求并发与事务时长变化，三副本 soak（每秒 2 个 Workflow）实测 MySQL 上合计 40–90 条；`tenuto.persistence.pool.connections`（按池区分 `in_use` / `idle`）与 `tenuto.persistence.pool.wait.count` 的增长说明池已是瓶颈，见 [可观测性](/operations/observability#指标)。

## Task Queue 规划

Workflow physical queue 与每个 Activity physical queue使用独立名称。逻辑路由键支持两种形式：

```text
namespace/task_queue
task_queue
```

显式 Namespace 路由适合多租户隔离，共享 fallback 适合通用 Worker Pool。

## Worker Pool

按 Activity 的资源特征拆分 Worker Pool：

- 网络密集型：高并发、较长连接池。
- CPU 密集型：并发度接近 CPU 配额。
- 长时间任务：独立队列和足够的 Activity 槽位。
- 高风险外部调用：独立 Task Queue 和并发槽位。

内嵌 workflow worker 以 `WORKFLOW_NODE_ID` 为 Identity 并默认启用 Execution Cache，支持 Sticky Execution；并发度与缓存见 [部署 Tenuto Server](./server.md)。独立 Workflow Worker 同样使用稳定 Identity、Build ID 和 Execution Cache。

## 网络与安全

- 开启内置认证：`WORKFLOW_HTTP_AUTH_TOKEN`（或 `_FILE`）要求 Bearer token，`WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` 要求客户端证书，两者可同时使用；需要按调用方分权时再在网关按路径分组授权，见 [安全部署](./security.md)。同一端口上的 `/admin/partitions` 只对运维网段开放。
- Server HTTP TLS 使用 `WORKFLOW_HTTP_TLS_CERT_FILE` 与 `WORKFLOW_HTTP_TLS_KEY_FILE`。开启 mTLS 后配置 `WORKFLOW_HEALTH_ADDR`，探针改探这个明文端口。
- Console 持有 Server 的 token（`TENUTO_SERVER_AUTH_TOKEN`）且没有用户登录，放在你自己的 SSO 或鉴权代理之后。
- TaskQ gRPC TLS 使用 `TASKQ_TLS_ENABLED=true` 及 CA、Server Name、Client Cert 配置。
- 内嵌 matching 的 gRPC 端口（7235）只在副本之间可达，不暴露给 Worker 或外部网络。
- `WORKFLOW_PPROF_ADDR`（缺省关闭）的 pprof 监听不鉴权：只在排查时开启，绑定本机或运维网段。
- MySQL 使用独立账号、最小权限和加密连接。Server 运行账号只需读写数据；DDL 权限只给执行 `tenuto-server migrate` 的账号。

## 发布顺序

每次升级先执行新版本的 `tenuto-server migrate`，再滚动替换 Server 副本（见 [数据库 Schema](/deploy/server#数据库-schema)）。迁移保持上一个版本可用，滚动期间旧副本照常运行；库落后于二进制时新副本启动失败。

替换副本期间，Go SDK（Client 与 Worker）对连接被拒绝、连接中断、网关 502/503/504 自动重试：调用方的 `ctx` 有 deadline 时重试到 deadline，没有时最多 1 分钟；`WorkflowRun.Get`、Update 句柄的 `Get` 与 Worker 的长轮询断开后重发、继续等待。Reset、Schedule 的 Create 与 Trigger、批量操作的启动只在请求确定没有到达时重试。网关在副本下线前停止转发新请求、对没有可用副本的请求应答 503，可以让这些调用也被重试。见 [瞬时错误与重试](/sdk/client#瞬时错误与重试)。

涉及新 WLang capability 时使用以下顺序：

1. 发布兼容新 capability 的 Tenuto Server。
2. 内嵌 workflow worker 随第 1 步的 Server 一起生效；使用独立 Workflow Worker 时，发布携带新 capability 列表的 Worker。
3. 检查 `/runtime/workflow/capabilities`。
4. 注册并启动使用新 Program artifact 的 Workflow。

使用独立 Workflow Worker（`WORKFLOW_WORKER_ENABLED=false`）时，先升级全部 Server，再升级 Worker：旧 Server 不识别 `CmdUpsertSearchAttributes`，新 Worker 首次走到 GetVersion 的 Workflow Task 会报 `workflow task command result missing` 并持续重试。默认内嵌部署不受影响。

旧 Run 固定引用自己的 Program Version，滚动升级期间继续由具备对应 capability 的 Worker 执行。
