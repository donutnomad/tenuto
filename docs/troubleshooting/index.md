---
title: 故障排查
description: Tenuto Server、Worker、Run、Activity 和 Trace 常见问题
status: Stable
---

# 故障排查

<FeatureStatus status="Stable" />

按故障对象进入专题页：

- [Update 一直等待](/troubleshooting/updates)：Handler 注册、Await 条件、终态竞态与 Client Context。
- [Activity 执行异常](/troubleshooting/activities)：排队、Attempt、Heartbeat、Timeout 与异步完成。
- [Worker 路由与 Sticky 故障](/troubleshooting/worker-routing)：Build ID、Task Queue、缓存命中与 owner fallback。
- [Schedule、History 与 Reset 异常](/troubleshooting/schedules-reset)：定时触发、History 容量和 Reset generation。

## Server readiness 失败

检查：

1. `WORKFLOW_DB_DSN` 与 `TASKQ_DB_DSN` 连通性。
2. Workflow 和 TaskQ 的 schema 迁移是否已执行。启动日志报 ``run `tenuto-server migrate` before starting the server`` 时，用新版本的二进制执行 `tenuto-server migrate`；报 checksum 不一致时说明库执行过的迁移与二进制内嵌的迁移不同，核对部署的版本。
3. `TASKQ_SERVICE_NAME` 对应的 matching 节点是否已注册。内嵌模式（默认）下，启动日志应有 `embedded matching started`，日志中的 `broadcast_addr` 是其它副本可拨号的地址；readiness 返回 503 且错误为 `embedded matching is not serving` 时检查 7235 是否被占用。
4. `TASKQ_WORKFLOW_QUEUE` 与 Activity physical queues 是否存在。
5. Token Secret 长度与 Base64 History Key。

Server 日志会在 readiness 开放前输出具体启动阶段错误。

## 请求返回 500、502 或 503

Runtime API 对 Server 自身的故障返回 500，响应体固定为 `internal server error`。MySQL 或任务队列暂时不可用（连接数打满 `Error 1040`、锁等待超时、死锁、连接中断、matching 分区换主）时返回 503 `service temporarily unavailable`（带 `Retry-After`），Go SDK 自动重试；同样的失败发生在 Reset、Schedule 的 Create 与 Trigger、批量操作的启动上时返回 502，请求可能已经生效，先读状态确认再重发。具体原因在 Server 日志里，按 `runtime http request failed` 搜索，每条记录带请求方法、路径和原始错误（500 是 ERROR，502、503 是 WARN）。503 持续出现时按 [数据库连接数](/deploy/production#数据库连接数) 核对连接池上限与 MySQL 的 `max_connections`。状态码的完整含义见 [Runtime HTTP API](/reference/http-api#失败应答)。

Server 日志的 WARN `task queue ack failed` 是 Worker 确认租约时任务队列没有收到（TaskQ 分区换主期间 `max retries exceeded`、旧 ownership epoch 的 `stale ack token`）。确认发生在结局落库之后，`ack` 照常应答 200；任务在可见性超时后重新投递，Server 按落库的状态丢弃它，Activity 不会再执行一次。只在分区换主时零星出现是正常的；持续出现时检查 matching 副本是否反复失去分区租约。

## Run 长时间 running

在 Console 查看 Current Waiting：

- Activity：检查 Task Queue、Build ID、Worker heartbeat、Attempt 和超时。
- Timer：检查 deadline 和 Server Timer 恢复。
- Child Workflow：进入 Child Run 查看状态。
- Workflow Task：检查 Workflow Worker 和 capability。
- Signal / Update：确认外部事件名称和目标 Run。
- Run Detail 出现 `ConfigError` 横幅：Run 因 Activity 契约不一致、`auto_upgrade` 换版前发现 History 里的载荷读不回新类型（`auto upgrade to ... refused: payloads recorded in history cannot be read by the candidate program: ...`；解码处的名字或类型无法静态确定时为 `... refused: the payload check cannot prove that payloads recorded in history stay readable: ...`，消息带 WLang 路径，见[换版前的载荷检查](/develop/versioning#换版前的载荷检查)），或准备 Workflow Task 时遇到每次都会重现的失败（`workflow task cannot be prepared: ...`，例如 Run 采用的 Program 版本读不到）而暂停。准备失败先修复原因再恢复。用横幅上的按钮恢复；恢复会关闭暂停时的 Workflow Task（History 里 `WorkflowTaskFailed`，`cause` 为 `run_resumed`）并开启新的一个。

## Activity 重复执行

Activity retry 会产生多个 Attempt。业务写操作应使用稳定幂等键。Trace 中选择 Call，再查看 Attempts 和错误原因。

## Worker 无法领取任务

检查 Namespace、Task Queue、Worker Role、Worker 上报的契约、Activity Type 注册和 TaskQ physical queue 映射。Worker 页面会显示最近 heartbeat 与 TTL。

Activity Task 只派给 Namespace、Task Queue 与 Run 的目标 Worker（设置了时）都相符的 poller，与 poller 的 Build ID 无关，内存后端与 TaskQ 后端规则相同。Worker 开始执行前按它这个 build 上报的契约对照 Program 的声明，不一致的领取被拒绝，任务约 1s 后重新投递。Worker 正常 `Stop()` 时立刻注销路由；进程崩溃时路由在心跳 TTL 之后到期。

## Workflow capability 不匹配

新 Program artifact 需要 Worker 支持对应 WLang capability。检查：

```bash
curl -sS -X POST http://tenuto-server:7400/runtime/workflow/capabilities \
  -H 'Content-Type: application/json' -d '{}'
```

按 Server → Worker → 新 Workflow 的顺序完成升级。

## Trace 无法加载

检查：

1. Run 配置中包含 WLang Program 和源码 metadata。
2. `console/public/replay.wasm` 与当前 Server artifact capability 对齐。
3. Event History 分页完整加载。
4. 浏览器 Console 中的 WASM 初始化错误。

重新构建 WASM：

```bash
make wasm
pnpm --dir console build
```

## Sticky 命中率低

检查 Worker Identity 和 Build ID 稳定性、IdleTTL、MaxEntries、MaxEstimatedBytes、部署滚动频率和 targeted queue schedule-to-start 延迟。

## History 达到硬限制

Workflow 应在 warning 阈值依据 `workflow.GetInfo(ctx)` 执行 Continue-As-New。将跨段状态压缩为业务 cursor 和必要聚合值。
