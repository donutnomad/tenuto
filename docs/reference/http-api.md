---
title: Runtime HTTP API
description: Workflow、Workflow Task 与 Activity Runtime 端点参考
status: Internal
---

# Runtime HTTP API

<FeatureStatus status="Internal" note="由同一发布版本的 Server、SDK、Worker 和 Console 协同使用" />

Tenuto 的 Go Client 使用 JSON Runtime HTTP API。业务系统通过 SDK 调用，Console 与 Worker 使用同一协议。Runtime API 按版本协同发布，内部请求结构和端点允许随 Server、SDK、Worker 与 Console 同步演进。

## 失败应答

所有 Runtime 端点共用一套状态码。4xx 与 504 的响应体是纯文本错误信息；500、502、503 的响应体是固定文本，具体原因记录在 Server 日志的 `runtime http request failed` 一行（字段 `method`、`path`、`err`；500 记 ERROR，502、503 记 WARN）。

| 状态码 | 含义 | 例子 |
| --- | --- | --- |
| 400 | 请求本身的问题：参数不合法，或请求与目标当前的状态不相容。改正请求后重试 | `workflow type is empty`、`invalid cron_expression: ...`、对已结束的 Run 发 Signal（`workflow <run> is completed`）、过期的 task token、标识超过长度上限（`workflow id has 200 characters, the limit is 191`，上限见下文[标识长度](#标识长度)） |
| 401 | Server 配置了 `WORKFLOW_HTTP_AUTH_TOKEN`，请求没有带 `Authorization: Bearer <token>` 或 token 不对；应答带 `WWW-Authenticate: Bearer realm="tenuto"` | `unauthorized` |
| 404 | 目标不存在 | Run、Workflow ID 没有 current Run（`no current run`）、Workflow Config、Definition、Schedule、Batch Operation |
| 409 | 与已有记录或并发写入冲突。重新读取后重试 | Schedule ID 已存在、Schedule 被并发修改、current Run 已被其他请求切换、Run ID 已存在、取消已结束的 Batch Operation |
| 415 | POST 的 `Content-Type` 不是 `application/json`（可带 `charset` 等参数），没有请求体的端点也一样 | 表单或 `text/plain` 提交：`Content-Type must be application/json` |
| 500 | Server 故障：数据库返回的非瞬时错误（缺表、数据超长等）或代码错误。原因看 Server 日志 | `internal server error` |
| 502 | 同 503 的瞬时失败发生在重发会再执行一次的端点上：`reset`、`create-schedule`、`trigger-schedule`、`start-batch`。失败不说明请求有没有部分生效（例如 Reset 的事务已提交、随后入队时连接数打满），先读状态确认再决定是否重发 | `service temporarily unavailable; the request may have taken effect` |
| 503 | Server 依赖的 MySQL 或任务队列暂时不可用：连接数打满（`Error 1040`、`1203`）、锁等待超时（`1205`）、死锁（`1213`）、连接没有建立或在途中断开（`2006`、`2013`、`invalid connection`、读写超时）、TaskQ matching 分区换主（gRPC `Unavailable`，或 TaskQ 的入队、`nack` 重试用完 `max retries exceeded`）；在途的非长轮询请求数超过 `WORKFLOW_HTTP_MAX_IN_FLIGHT`；请求在生效之前被取消。带 `Retry-After: 1`，请求可以原样重试，Go SDK 自动重试 | `service temporarily unavailable`、`too many requests in flight; retry`、`request cancelled before taking effect; retry` |
| 504 | 请求在 Server 端等待超时 | `context deadline exceeded` |

成功应答总带 JSON 响应体（长轮询的 204 除外）；2xx 空响应体说明应答没有完整送达，Go SDK 把它当作传输错误。

Update validator 的拒绝是 400，响应体是 `protocol.Failure` 的 JSON。长轮询到期没有任务时返回 204，没有响应体：请求的等待到期与队列（TaskQ）的长轮询先到期都是这个应答。

## 标识长度

标识按字符计数（不是字节），超过上限的请求应答 400：

| 标识 | 上限 |
| --- | --- |
| Namespace、Task Queue、Build ID、Schedule 时区 | 64 |
| Activity Type、Worker identity | 128 |
| Workflow ID、Workflow Type、Activity ID、Timer ID、Update ID、marker 名与 ID、Schedule 描述与 cron 表达式 | 191 |
| 启动与 Signal 的 `request_id` | 128 字节 |
| Batch Operation 的 `reason` | 1024 |

子 Workflow 的 Workflow ID 是 `<父 Run ID>/<ChildID>`，整体不超过 191。Workflow 代码发出的命令（Timer ID、子 Workflow ID 与 Type、Activity ID、Activity Type、Task Queue、marker 名与 ID）超长时，Workflow Task 以 `bad_start_timer_attributes`、`bad_start_child_workflow_attributes`、`bad_schedule_activity_attributes` 或 `bad_record_marker_attributes` 失败，Run 显示 Blocked，修复代码后恢复。

MySQL 表（含 TaskQ 的表）的排序规则是 `utf8mb4_0900_bin`：标识按字节比较，区分大小写、重音与尾随空格，`order-ab12` 与 `order-AB12`、`Prod` 与 `prod`、`t` 与 `t ` 是不同的标识，与内存后端一致。

## Namespace 与寻址

按 Run ID 或 Workflow ID 寻址的端点都带 `namespace`：`get-run`、`wait-closed`、`history`、`history-page`、`query`、`signal`、`update`、`search-attributes`、`cancel`、`terminate`、`reset`、`reset-activity`、`activity-states`、`get-config`、`set-policy`。省略 `namespace` 等同于 `default`。

- 给了 `run_id` 时，Run 必须属于请求的 Namespace，同时给了 `workflow_id` 时还必须属于这个 Workflow ID；不满足时返回 404 `run not found`，与 Run 不存在相同，Run 不受影响。
- 只给 `workflow_id` 时，在请求的 Namespace 里解析它的 current Run。

Workflow Worker 的 `config`、`events`、`status` 不接受 Run ID，按请求里的 `task_token` 读取这个 Workflow Task 所属的 Run：只有持有该任务租约的 Worker 能读取。

## Workflow Runtime

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/runtime/workflow/register` | 注册 Workflow Config 并激活为 latest |
| POST | `/runtime/workflow/artifacts` | 登记 Workflow Config；缺省不改变 latest |
| POST | `/runtime/workflow/set-policy` | 修改单个 Run 的版本策略并重新调度 |
| POST | `/runtime/workflow/set-worker-target` | 设置 Workflow 的 Activity Worker target |
| POST | `/runtime/workflow/start` | 启动 Workflow；请求体见「启动请求体」 |
| POST | `/runtime/workflow/start-version` | 按 Config Version 启动 Workflow |
| POST | `/runtime/workflow/get-run` | 获取 Run |
| POST | `/runtime/workflow/wait-closed` | 长轮询等待 Run 结束；请求体见「等待 Run 结束」 |
| POST | `/runtime/workflow/list` | 查询 Run List；给出 `run_ids` 时已结束的 Run 也返回，结束的 Run 带 `closed_at`。`limit` 缺省 50、最大 1000，`offset` 不能为负，`run_ids` 最多 1000 个，越界返回 400。应答 `{items, total, has_more}`：`total` 是满足条件的 Run 总数，`has_more` 表示这一页之后还有 Run；`filter.skip_total: true` 时不计算总数（`total` 为 0），Run 很多时这一页更快，翻页看 `has_more` |
| POST | `/runtime/workflow/history` | 读取完整 Event History |
| POST | `/runtime/workflow/history-page` | 分页读取 Event History |
| POST | `/runtime/workflow/signal` | 发送 Signal |
| POST | `/runtime/workflow/signal-with-start` | 原子解析 current Run 或启动后发送 Signal |
| POST | `/runtime/workflow/query` | 执行 Query |
| POST | `/runtime/workflow/update` | 发送 Update |
| POST | `/runtime/workflow/update-with-start` | 原子解析 current Run 或启动后发送 Update |
| POST | `/runtime/workflow/search-attributes` | 更新 Search Attributes |
| POST | `/runtime/workflow/cancel` | 取消 Run |
| POST | `/runtime/workflow/terminate` | 终止 Run |
| POST | `/runtime/workflow/reset` | Reset Workflow |
| POST | `/runtime/workflow/reset-activity` | Reset Activity |
| POST | `/runtime/workflow/dispatch-pending` | 触发 pending task 对账与补投递 |
| POST | `/runtime/workflow/capabilities` | 查询 Runtime capability |

### 定义登记与 Run 策略请求体

`/runtime/workflow/register` 与 `/runtime/workflow/artifacts` 使用同一个请求体，响应是 `WorkflowConfigRef` `{namespace, workflow_type, version}`：

```json
{
  "namespace": "orders",
  "workflow": { "type": "order-flow", "...": "生成器写出的 workflow.json 全文" },
  "activate_if_new": false
}
```

- `register` 登记并把该内容激活为 latest，`activate_if_new` 不参与。
- `artifacts` 在 `activate_if_new` 为 `false`（缺省）时只登记，latest 保持不变；为 `true` 时采用 Worker 启动登记的语义：新内容首次出现或尚无 latest 时激活，同 hash 重复登记保持 latest。
- `tenuto-workflow-gen generate -url` 调用 `artifacts`，加 `-publish` 调用 `register`。

`/runtime/workflow/set-policy`：

```json
{
  "namespace": "orders",
  "run_id": "01a1...",
  "policy": "pinned"
}
```

`policy` 取 `auto_upgrade` 或 `pinned`。目标 Run 已结束时返回 400，body 为 `cannot set policy: workflow <run_id> is <status>`。

`policy` 取 `auto_upgrade` 或 `pinned`，其他取值返回 400（`unknown workflow policy ...`）。调用清除该 Run 的 `config_error` 并重新调度 Workflow Task，响应是更新后的 Run。

## Visibility 与管理

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/runtime/workflow/list-namespaces` | Namespace 列表 |
| POST | `/runtime/workflow/get-config` | Workflow Config 详情 |
| POST | `/runtime/workflow/list-configs` | Workflow Definition 列表 |
| POST | `/runtime/workflow/list-workers` | Worker 列表 |
| POST | `/runtime/workflow/list-task-queues` | Task Queue 列表 |
| POST | `/runtime/workflow/run-stats` | 一个 Namespace 内按状态聚合的 Run 数量 |
| POST | `/runtime/workflow/activity-states` | Activity mutable state |
| POST | `/runtime/workflow/register-activity-worker-version` | 注册 Activity Worker Build ID |
| POST | `/runtime/workflow/heartbeat-activity-worker-version` | 刷新 Activity Worker Build ID TTL；登记不存在（从未登记，或到期超过一天后被清理）时应答 404，Worker 应重新调用 `register-activity-worker-version` |
| POST | `/runtime/workflow/deregister-activity-worker-version` | 注销 Activity Worker 的路由，`{namespace, identity, build_id}`；`Worker.Stop()` 调用 |
| POST | `/runtime/workflow/start-batch` | 受理批量操作：`{"req":{namespace,type,filter,reason,reset_type}}`，返回 202 与 `pending` 记录，后台异步处理；`type` 未知或 Reset 缺 `reset_type` 时返回 400，不产生记录 |
| POST | `/runtime/workflow/get-batch` | `{namespace, operation_id}`，返回记录的状态与进度；不存在时 404 |
| POST | `/runtime/workflow/list-batches` | `{namespace, offset, limit}`，按创建时间从新到旧分页，`{items, total}`；`limit` 缺省 50 |
| POST | `/runtime/workflow/cancel-batch` | `{namespace, operation_id}`，取消 `pending` / `running` 的批量操作并返回最新记录；已取消时原样返回，已结束时 409 |

批量操作记录的字段：`id`、`namespace`、`type`、`filter`、`reset_type`、`reason`、`status`（`pending` / `running` / `completed` / `failed` / `cancelled`）、`total_count`、`succeeded_count`、`failed_count`、`skipped_count`、`errors`（最多 20 条 `{run_id, message}`）、`created_at`、`started_at`、`finished_at`、`updated_at`。各计数对每种类型的含义与选择、恢复语义见 [Batch Operation](/operations/batch-operations)。

## Schedule

```text
/runtime/workflow/create-schedule
/runtime/workflow/get-schedule
/runtime/workflow/update-schedule
/runtime/workflow/delete-schedule
/runtime/workflow/pause-schedule
/runtime/workflow/resume-schedule
/runtime/workflow/list-schedules
/runtime/workflow/trigger-schedule
```

`create-schedule` 的请求体用 `workflow_type` 指定要启动的 Workflow Type；`list-schedules` 的 `filter.workflow_type` 按它过滤。每次触发启动的 Run 的 Workflow ID 是 `schedule-<Schedule ID>-<名义触发时间>`，搜索属性带 `TemporalScheduledById`（Schedule ID）与 `TemporalScheduledStartTime`（名义触发时间，UTC RFC 3339 秒级）；`list` 与 `start-batch` 的 `filter.search_attributes` 写 `{"TemporalScheduledById": "<Schedule ID>"}` 选出这个 Schedule 的全部 Run。这两个键与 `TemporalChangeVersion` 由 Server 维护，`search-attributes` 写入它们返回 400。

## 启动请求体

`workflow_type` 是 `@Workflow` 的 `type`，选择定义；`workflow_id` 标识这次执行，留空时服务端生成 UUIDv7 文本并在响应里返回。

| 路径 | 请求体 | 响应 |
| --- | --- | --- |
| `start` | `{namespace, workflow_type, workflow_id?, input, request_id?}` | `{run_id, workflow_id}` |
| `start-version` | `{ref, workflow_id?, input}` | `{run_id, workflow_id}` |
| `signal-with-start` | `{namespace, workflow_type, workflow_id?, start_input, signal_name, signal_input, request_id?}` | `{run_id, workflow_id, event_id}` |
| `update-with-start` | `{namespace, workflow_type, workflow_id?, start_input, update_name, update_id, update_args, wait_stage}` | Update 阶段与结果，含 `run_id`、`workflow_id` |

`start-version` 的 `ref` 是 `{namespace, workflow_type, version}`，指定一份已登记的配置内容版本；版本不存在时返回 404。

`start` 在该 Workflow ID 已有进行中的 Run 时返回 `current run conflict`；`signal-with-start` 与 `update-with-start` 复用进行中的 Run。三者在 current Run 已结束时都启动新 Run，新 Run 使用该 Workflow Type 当前 active 的 Program。

`request_id`（至多 128 字节，超出返回 400）让调用方安全地重发应答丢失的请求。`start` 的 `request_id` 记在 `WorkflowStarted` 里：Workflow ID 的 current Run（运行中或已结束）由同一个 `request_id` 启动时，应答这个 Run，不报冲突、不启动新 Run。`signal` 与 `signal-with-start` 的 `request_id` 记在 `WorkflowSignaled` 里：同一个 Run 上同一个 `request_id` 只记一条，重发应答第一次的 `event_id`。`signal-with-start` 启动新 Run 时 `request_id` 同时记在 `WorkflowStarted` 里；同一个 request ID 的重发：current Run（运行中或已结束）记过这条 Signal，或 current Run 的链（continue-as-new、失败重试）由这个 request ID 启动时，返回第一次的 Run 与 `event_id`，不启动新 Run、不再记 Signal。Go SDK 为每次调用生成 request ID，见 [瞬时错误与重试](/sdk/client#瞬时错误与重试)。

## 载荷中的数字

请求体里的用户载荷（`input`、`start_input`、`signal_input`、Schedule 的 `input`、Activity 的 `output` 与心跳 `details`、Workflow 命令的输入输出）按数值原样进入 History：整数解码为 int64，超出 int64 范围的正整数为 uint64，带小数点或指数的为 float64。超过 2^53 的整数不经过浮点数，精度不丢失；超出 uint64 或 int64 下限的整数返回 400。

## Workflow Task Runtime

Workflow Worker 使用 `/runtime/workflow-task/*`：

```text
poll · ack · nack · config · events · status
complete · activation · complete-activation · timed-out · failed
```

`config`、`events`、`status` 的请求体是 `{task_token, program_version?}`，读取 token 所属的 Run。`complete` 提交按完整 History 重放得到的命令；`activation` 与 `complete-activation` 是 Execution Cache 命中时的增量激活与提交。两组端点都只接受 Workflow Task 的 token。

`failed` 的请求体是 `{task_token, cause, reason}`，`cause` 只接受 Worker 的失败原因：`non_deterministic_error`（重放与 History 不一致）、`execution_session_invalid`（Execution Cache 的 session 校验失败，下一次从完整 History 恢复）。`unhandled_events`、`run_resumed`、`reset_workflow` 由 Server 写入（Workflow Task 需要重做，不计连续失败、不退避），Worker 上报它们或其他取值返回 400。

## Update 请求体

`/runtime/workflow/update`：

```json
{
  "namespace": "orders",
  "workflow_id": "order-1001",
  "run_id": "",
  "name": "change-limit",
  "update_id": "req-1",
  "wait_stage": "Completed",
  "args": [{"encoding": "json/plain-v1", "data": "NTAwMA=="}]
}
```

`/runtime/workflow/update-with-start`：

```json
{
  "namespace": "orders",
  "workflow_type": "order-flow",
  "workflow_id": "order-1001",
  "start_input": {"order_id": "order-1001"},
  "update_name": "change-limit",
  "update_id": "req-1",
  "update_args": [],
  "wait_stage": "Accepted"
}
```

`args` / `update_args` 按 Handler 业务参数的顺序排列，零参数 Handler 传空数组。`wait_stage` 取 `Accepted` 或 `Completed`（区分大小写），省略为 `Accepted`；其他取值返回 400。响应是 `{update_id, run_id, accepted_event_id, stage, outcome}`；`run_id` 是接受这次 Update 的 Run，之后按它等待结果；`outcome` 在 Update 完成后出现，只含 `success`（结果 Payload）或 `failure`（`message`、`type`、`timeoutType`、`nonRetryable`、`details`、`cause`）之一。

## 等待 Run 结束

`/runtime/workflow/wait-closed` 的请求体是 `{namespace, workflow_id?, run_id, timeout_millis?}`。Run 已结束或在等待期间结束时应答 `{run, close_event}`：`run` 与 `get-run` 的 `run` 相同，`close_event` 是这个 Run 的结束事件（`WorkflowCompleted`、`WorkflowFailed`、`WorkflowCancelled`、`WorkflowTerminated` 或 `WorkflowContinuedAsNew`）。等待到期、或 Server 停止时 Run 仍在运行，应答 204 且没有响应体，调用方再发一次。`timeout_millis` 省略时等 30s，最长 60s。

只等请求寻址的这一个 Run：`WorkflowContinuedAsNew` 的 `nextRunId` 与带 `newExecutionRunId` 的 `WorkflowFailed` 指向下一个 Run，调用方对它再发一次（SDK 的 `WorkflowRun.Get` 就是这样跟随执行链）。Run 由受理请求的 Server 副本结束时立即应答，由其他副本结束时最多晚 100ms，见 [跨副本唤醒](/develop/updates#跨副本唤醒)。

## Activity Runtime

Activity Worker 使用 `/runtime/activity/*`：

```text
poll · ack · nack · begin · heartbeat
cancellation · complete · fail · release
```

`begin` 把 Attempt 记为 started 并答复 `{execute, reason, started_time}`。`execute` 为 `true` 时 `started_time` 是 Server 记下的 Attempt 开始时间（`activity.Info.StartedTime`），Worker 立即 `ack` 队列租约，然后执行 Activity；之后 Attempt 的存活由 StartToClose、ScheduleToClose 与 Heartbeat 超时判定。Attempt 已开始、已结束或任务已过期时 `execute` 为 `false`（`reason` 如 `attempt_already_started`、`activity_closed`、`stale_task`），Worker `ack` 丢弃这份投递。Run 收到取消请求不影响 `begin`：Workflow 代码可能在取消之后调度清理 Activity。

`ack` 只清理队列，结局在此之前已经落库（`begin` 记下 started，或 `begin` 判定不执行），因此任务队列的确认失败不让请求失败：TaskQ 分区换主期间重试用完、租约属于旧的 ownership epoch（`stale ack token`）时 `ack` 仍应答 200，Server 记 WARN `task queue ack failed`。任务在可见性超时（TaskQ 默认 60s）后重新投递，`begin` 按 started 返回 `attempt_already_started`，Worker 确认丢弃。`/runtime/workflow-task/ack` 相同：Worker 在结果上报之后确认，重新投递的 Workflow Task 已关闭，Server 在 `poll` 里确认丢弃。`nack` 发生在结局落库之前，任务队列暂时不可用时应答 503，Worker 重试。

`heartbeat` 记录进度并答复 `{cancel_requested, reason}`：

- Workflow 请求取消这个 Activity 时 `reason` 为 `activity_cancel_requested`。Worker 取消 Activity 的 `ctx`；Activity 以取消错误或 `context.Canceled` 结束时，Worker 用 `fail` 上报 `type` 为 `CanceledError` 的失败（带取消错误的 details），Server 记 `ActivityCancelled`；Activity 返回成功或其他错误时照常上报。
- Run 已关闭（`run_closed`、`run_cancelled`、`run_terminated`）、Activity 已结束（`activity_closed`）或这次 Attempt 已过期（`stale_task`）时 Worker 取消 `ctx`，之后不上报这次 Attempt 的结果。

`cancellation` 只做上面的取消检查，不记录心跳。

`release` 交还一个已开始、因 Worker 停止而没有结果的 Attempt，请求体与 `begin` 相同（`{task_token, task}`）。Server 不记失败，把 Attempt 恢复为待派发、序号不变、stamp 递增（被交还的执行晚到的结果按过期丢弃），从交还时刻起算 Schedule-To-Start 并立即重新派发；Attempt 已结束、已过期或尚未开始时什么也不做。`Worker.Stop()` 调用它，见[运行 Worker](/develop/workers#优雅退出)。

`poll` 返回的租约是 `{task, token}`：`token` 是队列的租约凭据，`ack` / `nack` 原样带回；`task` 的字段是 snake_case（`type`、`namespace`、`workflow_id`、`workflow_type`、`run_id`、`activity_id`（即 `activity.Info.ActivityID`）、`scheduled_event_id`、`scheduled_time`、`attempt`、`stamp`、`activity_type`、`task_queue`、`input`、`task_token`、`last_heartbeat_details`、超时字段以纳秒计等）。

## Task Token

Workflow Task 和 Activity Task 使用 Server 签发的 HMAC Task Token。Token 绑定 Namespace、Run、Scheduled Event、Attempt、Stamp、Task Queue 和 Worker 信息。Worker 应原样回传 Token。

## 鉴权

Server 内置两种可选的身份认证，缺省关闭：`WORKFLOW_HTTP_AUTH_TOKEN` 要求 `/runtime/*` 与 `/admin/*` 带 `Authorization: Bearer <token>`（`/health/*` 不校验），`WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` 在 TLS 握手阶段要求客户端证书。两者只认证、不授权：通过认证的调用方可以读写全部 Namespace、登记 Program、冒充 Worker。按调用方或 Namespace 授权由网关承担，路径分组与配置见 [安全部署](/deploy/security)。

```bash
curl -X POST -H 'Content-Type: application/json' -H "Authorization: Bearer $TENUTO_AUTH_TOKEN" \
  -d '{}' http://localhost:7400/runtime/workflow/list-namespaces
```

## 分区管理

`/admin/partitions` 与 Runtime API 在同一个 HTTP 端口上，认证与 Runtime API 相同（配置了 token 时同样要求 Bearer token），只对运维网段开放；memory 后端应答 501。

| 方法 | 请求 | 应答 |
| --- | --- | --- |
| GET | `?queue=<物理 queue 名>` | `{queue, read, write}` |
| POST | `{"queue": "<物理 queue 名>", "partitions": <n>}`，`Content-Type: application/json` | `{queue, read, write, previousRead, previousWrite}` |

POST 把读写分区数都设为 `n`，只增不减：`n` 小于当前值时 409；`n` 取 1–1024，越界 400；`Content-Type` 不是 JSON 时 415。

## Activity worker target

`POST /runtime/workflow/set-worker-target` accepts `{namespace, workflow_type, target_worker_identity}` and returns the new `WorkflowConfigRef`. An empty target restores default scheduling. The target must be an online compatible Activity worker in the namespace. The endpoint returns HTTP 400 with an explanatory message for unavailable or incompatible targets.

The saved version applies to new runs, including future schedule starts. Existing runs and activity retries retain their original target. Workflow orchestration keeps its existing workflow-task routing. `list-configs` exposes `target_worker_identity` and `target_worker_status`; `activity-states` exposes the persisted `targetWorkerIdentity` alongside the actual `workerIdentity` and `workerBuildId` after start; an activity state carries no build of its own.

### Reset 边界

`reset` 与 `reset-activity` 的请求体带 `namespace`（省略为 `default`），按「Namespace 与寻址」的规则定位 Run。`reset-activity` 的请求体是 `{namespace, run_id, activity_id, reset_heartbeat, reason}`：`activity_id` 指定 Run 里一个尚未关闭的 Activity（同一 Run 里未关闭的 Activity 的 ID 互不相同），没有时返回 400。

`reset_event_id` 接受 WorkflowTaskStarted、Completed、Failed、TimedOut。Started 保留至该事件；关闭事件映射到同轮 Started。新运行保留此前已提交轮次的历史，重新执行所选轮次及后续步骤，沿用原运行配置版本与目标 worker。首次/末次 WorkflowTask 选择遵循相同映射。

`/runtime/workflow/reset` 要求 `reset_type`（`first_workflow_task` 或 `last_workflow_task`）或 `reset_event_id`，二者选一；缺少定位时返回 400。Batch Reset 只接受 `reset_type`，缺少时创建被拒绝（400）。

Reset 截止点必须满足此前调度的 activity/timer 均已闭合。仍有待处理项目时接口返回 400，错误包含截止事件 ID、项目类型及调度事件 ID；可以选择其调度前或完成后的工作流任务边界。校验发生在终止/替换当前运行之前，失败请求保留当前运行。终止被 reset 的运行与创建新运行在同一个事务里提交：写入失败时原运行保持运行，没有新运行。Console Reset 弹窗直接展示该错误。并行活动示例：A 已完成、B 仍待处理时，A 完成后的边界暂时无法 Reset；B 完成后的边界可执行 Reset。
