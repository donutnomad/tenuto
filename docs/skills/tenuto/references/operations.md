# 运维操作：诊断、恢复、Reset、Batch

Go 代码用 `sdk/client` 的 Client `c`（`client.Dial`，Namespace 取自它）；没有对应 Go 方法的运维操作直接请求 HTTP，路径都在 `POST /runtime/workflow/` 下。

## 诊断一个不前进的 Run

1. 读 Run：`c.DescribeWorkflowExecution(ctx, workflowID, runID)`（`Status`、`ConfigError`、`UpgradeError`、`Blocked`）或 HTTP `get-run`（另带 `terminal_failure`）。看 `status`、`config_error`、`upgrade_error`、`blocked`（`cause`、`reason`、`consecutive_failures`；只有 `running` 的 Run 会是 blocked，Run 结束后清空）、`terminal_failure`。
2. 按下表定位。
3. 需要看 Run 的逻辑进度：Query `__stack_trace`（`c.QueryWorkflow(ctx, workflowID, runID, "__stack_trace")`）。Server 直接回答，返回 `steps`（每个 Activity 的 `activity_id`、`activity_type`、`status`）、`waiting`（当前等待点的 `type`：`workflow_task`/`activity`/`timer`/`child_workflow`，及 `activity_id`、`child_id`、`activity_type`、`timer_id`）和 `summary`；粒度为 Activity，不含协程或源码行。
4. 需要看发生过什么：`c.GetWorkflowHistory` / `history-page`；Activity 的尝试次数与最近失败用 HTTP `activity-states`（`{namespace, run_id}`）。

| 观察 | 状态 | 处理 |
| --- | --- | --- |
| `config_error` 以 `auto upgrade to ... refused: payloads recorded in history` 开头 | 暂停：History 里已有的载荷在 latest 的类型下读不回，Run 没有换版 | `pinned` 在当前内容上跑完，或发布保留旧字段的版本后 `auto_upgrade`（`versioning.md`「换版前的载荷检查」） |
| `config_error` 以 `auto upgrade to ... refused: the payload check cannot prove` 开头 | 暂停：有位置或类型无法静态确定的解码处，且这类载荷的类型在 latest 里变了，Run 没有换版 | `pinned` 在当前内容上跑完，或把消息里 WLang 路径处的名字改成常量、写明变量类型后发布再 `auto_upgrade` |
| `config_error` 以 `workflow task cannot be prepared: ` 开头 | 暂停：准备 Workflow Task 时遇到每次都会重现的失败，Server 不再重投这个任务 | 修复原因后「恢复暂停的 Run」 |
| `upgrade_error` 非空，`auto upgrade to ... failed ...; retrying with backoff` | 不阻塞：留在旧内容，按退避重试升级 | 发布兼容内容，或 `SetRunPolicy(..., "pinned")` |
| `blocked.cause` 为 `non_deterministic_error`，reason 含 `nondeterministic workflow: ...` | Workflow Task 反复失败 | 修复代码（恢复命令序列或补 `GetVersion`）后重新发布，下一次重试自动继续 |
| `blocked.cause` 为 `workflow_panic`，reason 含 `[E_RUNTIME]`、`[E_NIL_RECEIVER]`、`[E_PANIC]`、`[E_SYMBOL]`、`[E_TYPE] type.assert cannot pass`、`[E_BUDGET]` 或 `workflow executor panic` | Workflow 代码或 Update handler panic、超出执行预算（Temporal 缺省的 BlockWorkflow），Run 保持 Running | 修复代码后重新发布，下一次重试从 History 恢复并继续 |
| `blocked.cause` 为 `payload_size_limit_exceeded` | Workflow Task 的命令带的载荷（Activity / 子 Workflow / Continue-As-New 输入、Marker、Workflow 结果、Update 结果）超过 `WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`，或一次 Workflow Task 的命令合计超过 3.5 MiB | 缩小载荷（大数据放外部存储、传引用）后重新发布 |
| `blocked.cause` 为 `activity_route_unavailable` | 没声明队列，路由目录里没有可用的 Activity Worker 或路由歧义 | 启动注册了该 `type` 的 Worker 或声明队列，自动继续 |

Blocked 期间到达的 Signal、Update、Activity 结果、Timer 触发与取消请求只写入 History，等退避（1s 起、最长 1 分钟）到期的那次重试一并处理，不会每来一个事实就重试一次；Terminate 立即生效，set-policy 立即调度。
| `status` 是 `running`、无错误、History 末尾是 `ActivityScheduled` | Activity 没被领取或正在执行；Server 日志反复 `activity claim refused` 时在线 Worker 的契约与声明不兼容 | 核对队列与 Worker（`workers.md`）；看 Activity 状态；契约不兼容时让契约兼容的 build 上线，或 `SetRunPolicy(..., "auto_upgrade")` |
| `status` 是 `running`、无错误、`__stack_trace` 的 `waiting.type` 是 `timer`，或 `waiting` 为空（等 Signal、`Await` 条件） | 正常等待 | 推进它等待的事件 |
| `status` 是 `failed` | 终态 | `terminal_failure`，或 `c.GetWorkflow(...).Get` 的错误 `errors.As` 到 `*apperror.Error` 看 Type 与 details；需要重跑时 Reset |

`WorkflowTaskFailed` 的 `cause`：Worker 上报的只有 `non_deterministic_error`、`execution_session_invalid`（Execution Cache 的 session 校验失败，下一次从完整 History 恢复）与 `workflow_panic`（Workflow 代码越界、nil 解引用、`panic` 或解释器 panic），计入连续失败并退避；`unhandled_events`、`run_resumed`、`reset_workflow` 由 Server 写入，表示 Workflow Task 需要重做，不计连续失败、不退避。`POST /runtime/workflow-task/failed` 收到其他取值（含后三个）返回 400。`unhandled_events` 只在一批含结束 Workflow 的命令（Complete、Fail、Cancel、ContinueAsNew）、而 Workflow Task 执行期间到达了 Worker 没有看到的事实时写入；其他命令照常提交，新事实交给下一个 Workflow Task。

错误原文与成因：

| 原文 | 成因 |
| --- | --- |
| `activity "T" contract mismatch on worker "W" (namespace "N" queue "Q" build "B"): <input\|output> expected "X" actual "Y"`（Server 日志 `activity claim refused` 的 `err`） | 领取时 Program 声明的契约（expected）与领取 Worker 的上报（actual）不兼容，这次领取被拒绝，任务稍后重新投递；两侧都有 wire 结构时末尾括号里列出不兼容的字段路径 |
| `auto upgrade to V failed at workflow task N: non_deterministic_error: ...; retrying with backoff` | `auto_upgrade` 的 Run 用新 Program 重放旧 History 失配 |
| `auto upgrade to V refused: payloads recorded in history cannot be read by the candidate program: <位置>: <旧类型> -> <新类型>: <字段路径>: <原因>` | 换版前的载荷检查：History 里该位置的载荷在 latest 的类型下读不回（字段删除、改名、改类型） |
| `auto upgrade to V refused: the payload check cannot prove that payloads recorded in history stay readable: <current\|candidate> program: <载荷> decoded into <类型> at <WLang 路径> (<原因>); <种类> types differ between the two versions` | 换版前的载荷检查：该解码处的名字或类型无法静态确定，而这种载荷的类型在两个版本间变了 |
| `nondeterministic workflow: history event is A, replay command is B` | 同一位置的命令种类、ActivityID 或 ActivityType 不同 |
| `nondeterministic workflow: missing replay command for A` | History 里该 Workflow Task 有的命令，代码在这个 Workflow Task 里没有产生（删除、跳过了步骤，或挪到了之后的 Workflow Task） |
| `nondeterministic workflow: extra replay command B` | 代码在这个 Workflow Task 里多产生了 History 里没有的命令（在已走过的位置之前加了步骤，或从之后的 Workflow Task 挪了过来） |
| `workflow task cannot be prepared: workflow config not found: ...` | Run 采用的 Program 版本在 Server 上读不到（被删除或从未登记）；重新登记同一份内容或 set-policy `auto_upgrade` 换到 latest |
| `workflow task cannot be prepared: ... invalid branch token ...` | Run 保存的 History branch 数据损坏 |
| `activity route unavailable: activity S type T is not registered in activity route catalog` | 这个 Namespace 没有 Worker 上报过该 type |
| `activity route unavailable: ... has no active route` | 上报过，心跳已过期（Worker 不在线） |
| `symbol "pkg.Func" not found` | Workflow 调用了解释器没有绑定的 Go 函数（`go-subset.md`） |
| `parse activity timeout "..."` | `ActivityOptions` 的超时字符串格式错误 |
| `current run conflict` | `StartWorkflow` 指定的 Workflow ID 已有活跃 Run |
| `unknown workflow policy ...` | `SetRunPolicy` 的取值不是 `auto_upgrade` / `pinned` |
| `cannot set policy: workflow <run_id> is <status>` | `SetRunPolicy` 的目标 Run 已结束；策略只能在 `running` 的 Run 上修改 |

### 运维请求本身失败时

Runtime API 的状态码区分原因：400 是请求的问题（body 是原因，改正后重试），404 是 Run、Schedule、Batch Operation 等目标不存在（Run 不属于请求的 Namespace 时同样是 404 `run not found`），409 是与已有记录或并发写入冲突（Schedule ID 或 Run ID 已存在、同一定义版本以不同内容重复登记、取消已结束的 Batch；重新读取后重试），500 是 Server 故障——body 固定为 `internal server error`，原因在 Server 日志的 `runtime http request failed` 行；503 是 MySQL 或任务队列暂时不可用（连接数打满 1040、锁等待 1205、死锁 1213、连接中断、matching 分区换主、TaskQ 入队与 nack 重试用完 `max retries exceeded`），以及在途的非长轮询请求数超过 `WORKFLOW_HTTP_MAX_IN_FLIGHT`（缺省 `4 × WORKFLOW_DB_MAX_OPEN_CONNS`；Update 只在拿锁与写库阶段计入）、请求在生效之前被取消，带 `Retry-After: 1`，SDK 自动重试；同样的失败落在 Reset、Schedule 的 Create / Trigger、批量操作启动上是 502（可能已经生效，先读状态再重发）。连接池上限是每个副本 Workflow 与 TaskQ 两个池各 `WORKFLOW_DB_MAX_OPEN_CONNS`（缺省 50），`副本数 × 2 × 该值` 加 migrate 与工具余量要小于 MySQL `max_connections`（缺省 151）；长轮询不占数据库连接。DSN 没有给出 `readTimeout` / `writeTimeout` 时 Server 设为 `60s` / `30s`；readiness 用每个池里一条专用连接 Ping，不和业务请求排队，所以 `WORKFLOW_DB_MAX_OPEN_CONNS` 最小为 2（小于 2 时启动失败）。内嵌 matching 的 gRPC 是明文，`TASKQ_TLS_ENABLED=true` 只能配外置 matching（`TASKQ_MATCHING_ENABLED=false`），否则启动失败。标识超长返回 400（按字符计：Namespace、Task Queue、Build ID 64；Activity Type、Worker identity 128；Workflow ID / Type、Activity ID、Timer ID、Update ID、marker 名与 ID 191；启动与 Signal 的 request ID 128 字节）；Workflow 代码发出的超长 Timer ID、子 Workflow ID / Type、Activity Type、Task Queue、marker 名与 ID 让 Workflow Task 以 `bad_start_timer_attributes`、`bad_start_child_workflow_attributes`、`bad_schedule_activity_attributes`、`bad_record_marker_attributes` 失败，Run 显示 Blocked。MySQL 表（含 TaskQ 的表）是 `utf8mb4_0900_bin`，标识区分大小写、重音与尾随空格，要求 MySQL 8.0+。

停机（`SIGTERM`）：长轮询立即结束（poll / wait-closed 应答 204，Update 返回当前阶段）；其余在途请求在 `WORKFLOW_HTTP_SHUTDOWN_TIMEOUT` 内照常做完并应答结果，生效之前被取消的请求应答 503。SDK 把 2xx 空应答体当作传输错误。

Activity 与 Workflow Task 的 `ack` 在结局落库之后发出（`begin` 记下 started 或判定不执行；WFT 结果已上报），任务队列确认失败（分区换主时 `max retries exceeded`、旧 ownership epoch 的 `stale ack token`）不让 `ack` 失败：应答 200，Server 记 WARN `task queue ack failed`，任务在可见性超时（60s）后重投，`begin` 返回 `attempt_already_started` 由 Worker 丢弃，Activity 不重复执行。

## 恢复暂停的 Run（ConfigError）

```go
err := c.UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{
    WorkflowID: workflowID, RunID: runID,
    Policy:     workflow.PolicyAutoUpgrade, // 或 workflow.PolicyPinned
})
```

HTTP `set-policy`：`{"namespace":"orders","run_id":"...","policy":"auto_upgrade"}`。调用清除 `ConfigError`，关闭暂停时停在半路的 Workflow Task（History 里一条 `WorkflowTaskFailed`，`cause` 为 `run_resumed`），并立即调度一个新的 Workflow Task；`auto_upgrade` 在这个新任务上换到 latest。

| 选择 | 前提 | 结果 |
| --- | --- | --- |
| `pinned` | 当前内容能继续执行 | 留在当前内容继续执行 |
| `auto_upgrade` | latest Program 能重放该 Run 已有的 History，且能读回 History 里已有的载荷 | 下一个 Workflow Task 换到 latest |

准备 Workflow Task 失败（`workflow task cannot be prepared: ...`）暂停的 Run 恢复方式相同：先修复原因（例如重新登记那一版 Program），再 set-policy；原因仍在时 Run 再次暂停。存储读取出错、超时这类暂时性失败不暂停，任务按队列租约重投。

前提不成立时错误会再次出现：`pinned` 再次记 `ConfigError`；`auto_upgrade` 记 `UpgradeError` 并留在旧内容。Activity 契约不兼容不暂停 Run，不在这里处理：任务留在队列里，契约兼容的 build 上线后自动继续，或 set-policy `auto_upgrade` 让新 build 领取。Console 的对应按钮：`Pin current content`、`Resume with latest (AutoUpgrade)`。成因与发布顺序见 `versioning.md`。

## Workflow Reset

Reset 从 History 的一个 Workflow Task 边界重新执行：原 Run 进入终态（`WorkflowTerminated`，reason `reset`），创建新 Run（新 Run ID，同一 Workflow ID），重置点之前的 History 保留，之后的代码重新执行。用于代码修复后重跑、清除错误的后续决策。

```go
newRunID, err := c.ResetWorkflowExecution(ctx, client.ResetWorkflowExecutionOptions{
    WorkflowID: workflowID, RunID: runID, // RunID 空 = current Run
    Reason:     "fix deployed, rerun from last decision",
    ResetType:  client.ResetTypeLastWorkflowTask, // 或 ResetTypeFirstWorkflowTask；或 EventID
})
```

HTTP `reset`：`{"run_id":"...","reason":"...","reset_type":"last_workflow_task"}`，响应 `{"run_id":"<new>"}`。

重置点二选一：

| 字段 | 取值 |
| --- | --- |
| `reset_type` | `first_workflow_task`（从头重跑）、`last_workflow_task`（重做最后一轮决策） |
| `reset_event_id` | 一个 Workflow Task 边界事件的 ID：`WorkflowTaskStarted` / `Completed` / `Failed` / `TimedOut`；后三种映射到同一轮的 `WorkflowTaskStarted` |

`ReapplyExcludeTypes`（`reapply_exclude_types`）：Reset 缺省把重置点之后收到的 Signal 与 Update 重新应用到新 Run；传 `client.ResetReapplyExcludeTypeSignal`（`"signal"`）、`client.ResetReapplyExcludeTypeUpdate`（`"update"`）或两者排除它们。重新应用的 Update 在新 Run 上按 Update ID 去重，同一个 Update ID 再发来时返回已接受的记录。

`RequestID`（`request_id`，可选，最长 128 字节）：同一个 Run 最近一次 Reset 来自同一个 `RequestID` 时不再 fork，返回那次 Reset 出的 Run ID（对应 Temporal 的 `request_id`）；带它的 Reset 遇到 502 / 连接中断可以直接重发。批量 Reset 自动为每个 Run 生成确定性的 `request_id`（Batch ID + Run ID）。

校验在终止原 Run 之前完成，失败返回 400，原 Run 不变：

| 错误 | 成因 |
| --- | --- |
| `reset requires reset_type (first_workflow_task or last_workflow_task) or reset_event_id` | 没有给重置点 |
| `reset_type and reset_event_id are mutually exclusive` | 两个都给了 |
| `unknown reset type: ...` | `reset_type` 取值错误 |
| `reset_event_id N is not a valid reset point: must be a workflow task boundary event (WorkflowTaskStarted/Completed/Failed/TimedOut)` | 事件 ID 不是 Workflow Task 边界 |
| `cannot reset: pending child workflows exist at reset point` | 重置点之前启动的子 Workflow 在重置点上尚未结束（按 History 判定，重置点之后才关闭的也算）：选更早的边界，或选它关闭之后的边界 |

重置点上未结束的 Activity 与 Timer 不阻止 Reset（与 Temporal 一致）：未开始的 Activity 在新 Run 里以 Attempt 1 重新派发，Schedule-To-Start / Schedule-To-Close 从 reset 时刻起算；已开始的 Activity（History 里有当前 attempt 的 `ActivityStarted`，只有 `MaxAttempts: 1` 的 Activity 开始时就写；有重试的在 attempt 结束时才补写，所以执行中或退避中的按未开始处理）收到不可重试的 `ResetWorkflowError`，`ActivityFailed.lastHeartbeatDetails` 是原 Run 在 reset 时的最新心跳；已请求取消、开始只是 transient 的写 `ActivityCancelled`；Timer 按原 deadline 触发，reset 时已过期的立即触发。原 Run 上仍在执行的 attempt 的完成被拒绝。未关闭的子 Workflow 仍拒绝 Reset（见上表）。细节见文档 `operations/reset.md#分叉点上未结束的-activity-与-timer`。

Activity Reset（单个调用）：Attempt 置 1，Schedule-To-Start 从 reset 时刻起算，Schedule-To-Close 仍从最初调度时间起算，Reset 不延长整个调用的期限（与 Temporal `ResetActivity` 相同）。

Reset 之后：

- 重置点之后的 Activity 重新执行。新 Run 的 `RunID` 变化，Workflow ID 与重新执行的 Activity 的 `ActivityID` 不变：用业务 ID 或 `WorkflowID`+`ActivityID` 构造的幂等键与 Reset 前相同，下游返回第一次的结果；用 Workflow 里 `RandomUUID` 生成、且生成位置在重置点之后的键是新值，下游会真正再执行一次（见 `activities.md#幂等`）。
- 新 Run 的搜索属性：按 `reset_type` 时是启动时的属性；按事件时是启动属性加上该事件及之前的写入。
- 新代码仍需要能重放保留下来的那段 History。
- Reset 标记（新 Run History 里 `cause` 为 `reset_workflow` 的 `WorkflowTaskFailed`）不计入 `blocked.consecutive_failures`，新 Run 从未阻塞状态开始。
- Reset 一个运行中的子 Run：父 Run 继续等待，它的子关联改指新 Run，新 Run 的结果照常传播给父 Run。
- 终止原 Run、创建新 Run、切换 current、重置点上未结束的 Activity / Timer 在新 Run 里的 state 与补写的关闭事件、重置点之后的 Signal / Update 在同一个事务里写入；写入失败时 Reset 整体失败，原 Run 保持运行，没有新 Run。

## Activity Reset

把当前 Run 里一个尚未关闭（排队、执行中或重试退避中）的 Activity Call 的 Attempt 重置回 1 并立即重新派发（例如下游已修复、不想等退避）。已关闭的 Activity 用 Workflow Reset。`sdk/client` 没有对应方法，用 HTTP：

```bash
curl -sS -X POST "$TENUTO_URL/runtime/workflow/reset-activity" -H 'Content-Type: application/json' \
  -d '{"namespace":"orders","run_id":"<run-id>","activity_id":"charge-12","reset_heartbeat":true,"reason":"downstream recovered"}'
```

`activity_id` 是该 Call 的 ActivityID（`ActivityOptions.ActivityID`，或缺省的 `#a1` 这类位置 ID；从 History 的 `ActivityScheduled` 或 `activity-states` 取得）。同一 Run 里未关闭的 Activity 的 ID 互不相同，所以寻址唯一；没有这个 ID 的未关闭 Activity 时返回 400。Attempt 从 1 重新开始，旧的执行中任务与异步完成 Token 失效。

## Batch

对一个过滤器选中的全部 Run 执行取消、终止或 Reset。Batch 是持久化的后台任务：`StartBatchOperation` 只受理并返回 `pending` 记录（HTTP 202），Server 的后台驱动分页处理，调用方轮询进度：

```go
op, err := c.StartBatchOperation(ctx, client.StartBatchOperationOptions{
    Type: client.BatchOperationTypeTerminate, // Cancel / Terminate / Reset
    Filter: client.ListWorkflowOptions{
        WorkflowID: "order-flow",
        Statuses:   []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusRunning}, // 为空时只选运行中的 Run
    },
    Reason: "bad release rollback",
    // ResetType: client.ResetTypeLastWorkflowTask, // Type 为 Reset 时必填
})

current, err := c.DescribeBatchOperation(ctx, op.ID) // 轮询直到 State 不是 pending / running
page, err := c.ListBatchOperations(ctx, client.ListBatchOperationsOptions{Limit: 20})
stopped, err := c.StopBatchOperation(ctx, op.ID) // 已结束的 Batch 返回 client.ErrConflict
```

HTTP：`start-batch` 的 body 是 `{"req":{"namespace","type","filter","reason","reset_type"}}`，应答 202；`get-batch`、`cancel-batch` 是 `{namespace, operation_id}`；`list-batches` 是 `{namespace, offset, limit}`，从新到旧。

- 选择：过滤器匹配的 Run，`page_token`、`limit`、`order_desc` 不参与；给了 `run_ids` 时目标就是这些 Run（含已结束的）。只处理 Batch 创建时（那一毫秒）及之前创建的 Run，Reset 产生的新 Run 与之后启动的 Run 不在内，Batch 必然结束。驱动按 `(created_at, id)` 游标翻页，每页读取当时仍匹配的 Run：处理到之前离开过滤器的 Run 不处理。先用同一个过滤器 `c.ListWorkflow` 预览（Batch 的 `Statuses` 为空时只选运行中的 Run，`ListWorkflow` 为空时不过滤状态）。
- 状态：`pending` → `running` → `completed` / `failed`（有失败且没有成功），`pending` / `running` → `cancelled`。计数 `succeeded_count`、`failed_count`、`skipped_count`；`total_count` 首页时取匹配数，结束时改为实际处理数；`errors` 最多 20 条 `{run_id, message}` 样本。时间 `created_at`、`started_at`、`finished_at`、`updated_at`。
- `cancel` 的 succeeded 是「已请求取消」：Batch 在全部请求发出后就是 `completed`，Run 可能仍在运行。已结束的 Run 对 cancel / terminate 计为 skipped；Run 在 Batch 创建后已被 Reset 过对 reset 计为 skipped。
- 取消 Batch：`pending` / `running` → `cancelled`，驱动在下一页之前停下，正在处理的那一页仍计入，已执行的动作不回滚。重复取消原样返回；取消已结束的 Batch 返回 409。
- 恢复：每页处理完后计数与游标一起保存；Server 崩溃、重启或换 leader 后从游标继续，未保存的那一页重做一次。重做不会重复生效（已终止的跳过、重复取消请求不写事件、已 Reset 的跳过），所以计数准确，但崩溃前已完成的动作可能记成 skipped。
- 多副本：驱动经数据库选主（lock `workflow:batch-driver`），只有 leader 处理；leader 正常停机时停下驱动后释放锁，其他副本不等租约到期即接任；记录带版本号，租约切换瞬间的两个驱动中落后的一方保存失败后停下，最多重复执行一页；重复执行不重复生效，Reset 由确定性的 `request_id` 去重，同一个 Run 不会被 reset 两次。
- Batch Reset 只支持 `reset_type`；`type` 不是 `cancel` / `terminate` / `reset` 时返回 `unknown batch operation type: ...`（400）。两种情况都不产生记录。

| 类型 | 语义 | 场景 |
| --- | --- | --- |
| `cancel` | 协作式停止 | 业务撤销 |
| `terminate` | 立即终态 | 故障隔离、失控执行 |
| `reset` | 从 Workflow Task 边界创建新 Run | 代码修复后批量重跑 |

## 资源查看

`sdk/client` 不提供资源列表，用 HTTP（`POST /runtime/workflow/<路径>`）：

| 路径 | 内容 |
| --- | --- |
| `list-workers` | 在线 Worker、Identity、BuildID、队列（到期超过一天、没有 Running Run 指定的登记由全量补偿删除；Worker 的心跳得到 404 时重新登记） |
| `list-configs` | 已登记的定义与版本 |
| `list-task-queues`、`list-namespaces` | 队列与命名空间 |
| `activity-states` | Run 内每个 Activity Call 的状态 |
| `capabilities` | Server 支持的能力 |

Console（缺省端口 7401）提供同样的信息与操作：Run 列表与 History、Trace 调试台、Reset、Batch、`ConfigError` 红色横幅与 `UpgradeError` 黄色横幅及对应的恢复按钮、失败 Run 的 Failure 卡片（`terminal_failure` 与 timeout 类型）、Pending 页签（取自 `activity-states` 的当前尝试、Worker、心跳、最近失败与下一次重试时间）。事件详情里的 `json/plain-v1` 载荷解码为 JSON 显示。Schedule 列表显示 Overlap Policy。浏览器只访问 Console 的 `/runtime/workflow/*`，由 Console 服务端转发到 `TENUTO_SERVER_URL`，并加上 `TENUTO_SERVER_AUTH_TOKEN` 与 `TENUTO_SERVER_TLS_*` 的客户端证书（只在服务端读取）；Console 没有用户登录，对外开放时放在 SSO 之后。Server 连不上时页面提示 `Cannot connect to Tenuto Server at <地址>.` 并给出 Retry；Server 应答 401 时提示 Console 的凭证被拒（`Tenuto Server rejected the Console's credentials (HTTP 401).`，检查 `TENUTO_SERVER_AUTH_TOKEN`）；Server 返回的其他错误（含 500）照原文显示。侧边栏导航叫 Runs（Run 列表）与 Definitions（Workflow 定义），宽度 768px 以下收进菜单按钮。

## 生产就绪

文档 `operations/production-readiness` 汇总上线判断依据：

- 保证：Activity 至少一次（必须幂等）；Workflow 状态转换在一个事务里提交（History 与 mutable state 同事务，execution state 行锁跨副本互斥，first-wins 与 generation stamp 拦下旧任务）；确定性重放；`pinned` / `auto_upgrade` 与换版前的载荷检查；只向前的 schema 迁移；保留期 GC。
- 必需配置：MySQL 8 且每次升级先 `tenuto-server migrate`；Server 强制的 DSN 参数；`副本数 × 2 × WORKFLOW_DB_MAX_OPEN_CONNS + 余量 < max_connections`；Bearer token 与 / 或 mTLS（mTLS 时探针用 `WORKFLOW_HEALTH_ADDR`）；Console 放在 SSO 之后、token 只在服务端；Task Token Secret 与 History Page Token Key 跨副本一致；NAT / Kubernetes 下设 `TASKQ_MATCHING_ADVERTISE_ADDR`；`WORKFLOW_RETENTION`；OTel；`terminationGracePeriodSeconds` 大于 `WORKFLOW_HTTP_SHUTDOWN_TIMEOUT` 加 OTel flush；pprof 关闭；Go ≥ 1.27.4。
- 载荷上限：单个载荷 `WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`（默认 2 MiB，最大 3 MiB），Runtime 请求体 4 MiB（超出 413）；一次 Workflow Task 的命令合计 3.5 MiB；入口超限 400，命令超限时 Workflow Task 以 `payload_size_limit_exceeded` 失败，Activity 结果或心跳超限时 Attempt 以不可重试的 `PayloadSizeLimitExceeded` 失败（`operations/capacity-limits#载荷大小`）。
- 已知限制：单个 MySQL 主库是扩展与可用性上限（没有分片，数据库 HA 由运维方负责）；TaskQ 分区易主（副本加入或离开）时投递停顿 2–3 s；WLang 只限制单个值，`MaxAllocBytes` 不执行，没有每次执行的总内存上限；没有内置多租户授权，Namespace 不是权限边界；长时间分区没有测量数据。
- 实测数字（一台笔记本、共享 MySQL、3 副本 + 2 Worker 同机，是下限）与复现命令、通过标准都在该页，引用时以该页为准。

## 多副本故障注入与压测

仓库的 `cmd/tenuto-cluster` 在一个 MySQL 上启动 N 个 `tenuto-server` 副本、M 个 Activity Worker 进程与进程内负载均衡器，从仓库根目录运行；只创建、删除 `-db` 指定的库（以 `tenuto_e2e` 或 `tenuto_bench` 开头、事先不存在）：

```bash
GOWORK=off go build -o bin/tenuto-cluster ./cmd/tenuto-cluster
./bin/tenuto-cluster failover -dsn 'user:pw@tcp(127.0.0.1:3306)/' -db tenuto_e2e_1 -workflows 100
./bin/tenuto-cluster soak     -dsn 'user:pw@tcp(127.0.0.1:3306)/' -db tenuto_e2e_soak -duration 2h -keep
./bin/tenuto-cluster upgrade  -dsn 'user:pw@tcp(127.0.0.1:3306)/' -db tenuto_e2e_up -server-bin-a A -server-bin-b B -workflows 900
./bin/tenuto-cluster bench    -dsn 'user:pw@tcp(127.0.0.1:3306)/' -db tenuto_bench_1 -rate 20 -duration 60s
```

- 工具建一个空库，用构建出的 `tenuto-server migrate` 执行 Tenuto 与 TaskQ 的迁移，再启动副本（与生产部署的建表路径相同）。工具从不重启、不改配置 MySQL 服务端。
- 代理（`-proxies`，故障模式缺省开、`bench` 缺省关）：每个副本到 MySQL 的连接经 `127.0.0.1:<base+301+i>` 的进程内代理；副本 i 以 `TASKQ_MATCHING_ADVERTISE_ADDR=127.0.0.1:<base+201+i>`（它的 matching 代理）登记成员表，其他副本经代理连进来。代理用 `lsof` 识别连接来自哪个副本。负载均衡器每秒探测各副本 `/health/ready`，失败的不分新连接。
- `-auth`：副本以随机生成的 `WORKFLOW_HTTP_AUTH_TOKEN` 运行，Worker 子进程（环境变量 `TENUTO_AUTH_TOKEN`）与工具的 Client 带上它。
- `failover`：运行中按 `-faults`（`at:fault`）注入故障。进程故障 `kill-leader`、`kill-replica`、`kill-worker`、`pause-replica`（`[:down]`）；经代理的故障 `db-drop:<target>`（关闭现有连接）、`db-blackhole:<target>:<d>`（接受连接但不转发、不发 RST）、`db-latency:<target>:<ms>:<d>`、`partition:<replica>:<d>`（与其他副本之间的 matching 流量停滞）、`isolate:<replica>:<d>`（partition 加 db-blackhole）、`api-blackhole:<replica>:<d>`（负载均衡器转给它的 HTTP 停滞，readiness 照常通过）；target 是 `random`、`leader`、序号或 `replica-<i>`，`db-*` 另可 `all`。结束后核对结果、History 不变式、填充载荷、重放、Activity 执行记录（完成的 attempt 有执行记录、同一 attempt 不执行两次）与库内状态，写 `summary.json`，有违规时退出码 1。重复执行是至少一次语义允许的，只报告分布与推断的原因。
- 规模参数（`failover`、`soak`、`upgrade`）：`-workflows`（10000 以上，History 逐个读取核对后丢弃）、`-activities`（顺序 Activity 数即 History 长度，最多 1000）、`-payload-bytes`（每个 Activity 输入输出的填充）、`-children`（子 Workflow 扇出）；`bench` 有 `-workflows`（启动上限）、`-payload-bytes`，History 长度是 `-steps`。
- `soak`：`-duration` 内按 `-start-rate` 持续启动，每 `-fault-every`（2m）从 `-fault-set` 随机注入一个故障；Run 结束 10s 后逐个核对（同 failover，含执行记录），卡住的 Run（15m）、故障结束 5m 后仍有过期 Run、无故障退出的进程都立即失败。每分钟向 `soak.jsonl` 写进程 RSS/CPU（不采 goroutine 数；需要时读副本 pprof 端口的 `/debug/pprof/goroutine?debug=1`）、每表行数、TaskQ 未投递数、补投次数与工具自身内存；`soak-summary.json` 给出每次故障的恢复时间（故障结束到过期 Run 清零）与最大值、RSS 与行数的增长斜率（RSS 斜率去掉 soak 开始与每次进程启动后的 10 分钟预热：副本启动后 RSS 约 10 分钟从 ~65 MiB 涨到 ~140 MiB 后持平），重复执行按执行期间仍在生效的故障归因（api-blackhole 期间的结果上报卡到 attempt 超时）。`-retention` 缺省 1h（Server 的下限），GC 在运行 1h 之后才回收，`-duration` 超过 retention + 30m 时核对 `tenuto_runs` 确有回收。`-db-max-open-conns`（缺省 20）设每个副本两个连接池的上限，三副本合计 120 条，低于 MySQL 缺省的 `max_connections` 151；0 用 Server 缺省值 50（三副本最多 300 条）。
- `upgrade`：在 A 上启动并持续启动 Workflow，`-upgrade-at` 后执行 B 的 `migrate`，逐个副本摘除、`SIGTERM`、换成 B 启动、就绪后加回；全部 Run 关闭后按 failover 核对。两个提交各用 `git worktree` 构建一个 `tenuto-server`；A、B 缺省相同（演练机制本身）。
- `bench`：开环 `-rate` 或闭环 `-concurrency`，报告启动、完成（调用端与 History；调用端经 `wait-closed` 长轮询，与 History 相差一个 HTTP 往返加至多 100ms）、Workflow Task 与 Activity 调度延迟的 p50/p95/p99/max，吞吐，MySQL 全局计数器差值与进程 CPU/RSS，写 `bench.json`。
- CPU profile：`WORKFLOW_PPROF_ADDR`（缺省关闭）在独立监听上提供 `/debug/pprof/`，并开启 mutex 与 block 采样，不鉴权、只绑定本机或内网；`tenuto-cluster` 给副本 i 设为 `127.0.0.1:<base+401+i>`，`bench` 期间 `go tool pprof 'http://127.0.0.1:17801/debug/pprof/profile?seconds=30'` 采副本 0。
- 进程内负载：`GOWORK=off go test ./cmd/tenuto-cluster -run '^$' -bench Harness -benchtime 200x -cpuprofile cpu.out` 在一个进程里跑内存后端的完整 Server 与 Activity Worker（`seq50`：50 个顺序 Activity；`small`：3 个 Activity 加 10ms Timer），报告 `tasks/s`、`cpu-µs/task`、`allocs/task`；`TENUTO_HARNESS_TX=1` 另报每个任务的持久化事务数。不含 GORM、MySQL 与 TaskQ 的开销。
- GC 吞吐：`TENUTO_TEST_MYSQL=1 GOWORK=off go test ./server/integration -run TestRetentionGCThroughput -count=1 -v` 造 1000 棵已到期的单 Run tree，断言一轮 `RunOnce` 全部删完、每分钟至少 1000 棵，并按表列出每棵 tree 的语句数。
- 语句来源：`TENUTO_TEST_MYSQL=1 GOWORK=off go test ./server/integration -run TestStatementBudgetPerWorkflow -count=1 -v` 在单进程里跑同一负载，按操作、表与入口（`engine.PollWorkflowTask`、`taskq.queue.(*taskRuntime).tryGetTask` 等）列出每个 Workflow 的语句数与空闲时每秒的语句数（不含 `BEGIN`/`COMMIT`），超出预算（300 条 / Workflow；空闲约 26 条/秒，按分区锁续约、锁成员行与 GC 的节拍计算）时失败。不需要 MySQL 的 `TestRepositoryReadsPerWorkflow`（内存后端）统计每个 Workflow 在事务之外读 Run 与 execution state 的次数。
- 每条语句一次往返：Server 给 `WORKFLOW_DB_DSN` 与 `TASKQ_DB_DSN` 设置 `parseTime=true`、`loc=UTC`、`interpolateParams=true`（客户端插值，不再 prepare、execute、close 三步）、`clientFoundRows=false`（仓储按 affected rows 区分新写入与去重命中；`app.WithDatabase` 注入的句柄带 `clientFoundRows=true` 时启动探测失败）；字符集用 `utf8mb4`，GBK、Big5、SJIS 等字符集启动失败。一个事务里 execution state 只加锁读一次、提交前写回一次，与追加几个事件无关。
- 空闲时：TaskQ 分区的行都已投递或确认时 poll 与可见性扫描不读库；内嵌 Workflow Worker 的 Workflow Task 长轮询持续 `TASKQ_POLL_TIMEOUT`（默认 30s），不再每秒重新开始。
- leader 副本是 `tenuto_leader_lock` 里 `lock_name = 'workflow:reconciler'` 且未过期的 `leader_ip`，值等于该副本的 `WORKFLOW_NODE_ID`。
- 投递丢失由 reconciler 补投：open 未 started 的 Workflow Task 或 Scheduled Activity attempt 的派发记录是 `dispatched`、超过等待时长（30s 起按派发次数翻倍，最长 1 小时）没被领取时，leader 再投一份，日志 `task dispatched but not claimed; redelivering`。运行中出现它说明有投递丢失，记下 `run_id` 与 `scheduled_event_id` 查原因；无故障的 `bench` 里不应出现。
- 重启的副本在后台做启动恢复（日志 `startup recovery finished`），不占 `WORKFLOW_STARTUP_TIMEOUT`，恢复慢时副本照常就绪。启动恢复只处理到期索引选出的 Run，只挂两个补偿周期内到期的定时器，再逐页批量检查子 Run 相关的窗口；代价与待处理事项数和页数成正比。Run 缺少配置声明的截止时间（升级到声明超时的 Program）只在它被选为候选时补写。matching owner 被 kill 后、成员表剔除它之前，入队换到其他 owner 的分区，不再报 `frontend: enqueue: max retries exceeded`。
- 已关闭 Workflow Task 的投递由 Server 在领取时确认丢弃，日志里不再出现成片的 `workflow task activation fence is stale`。

参数、核对项与输出见文档 `operations/cluster-testing`。
