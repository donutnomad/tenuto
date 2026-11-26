# 调用端 API

Go 调用端只用 `github.com/donutnomad/tenuto/sdk/client`；非 Go 调用端直接请求 HTTP（见末节）。接口与语义对齐 Temporal Go SDK 的 `client.Client`，只包含 Server 支持的操作。代码位置：`sdk/client/`。

```go
c, err := client.Dial(client.Options{
    HostPort:  "127.0.0.1:7400", // 或完整 URL
    Namespace: "orders",         // 空值为 default，本 Client 全部调用作用于它
})
if err != nil {
    return err
}
defer c.Close()
```

`Options` 还有 `Identity`（缺省 `<pid>@<hostname>`，Worker 的缺省 Identity）、`HTTPClient`（网关要求的其他凭证在它的 Transport 上配置）、`AuthToken`（每个请求带 `Authorization: Bearer`，对应 Server 的 `WORKFLOW_HTTP_AUTH_TOKEN`）、`TLS`（`*tls.Config`：服务端 CA 与 mTLS 客户端证书，与 `HTTPClient` 互斥，设置后无 scheme 的 `HostPort` 按 https）、`Telemetry`、`Retry`。一个 Client 可被多个 goroutine 共用，也传给 `worker.New`（见 `workers.md`）。每个方法在 `ctx` 取消或到期时返回 `ctx` 的错误。

## 瞬时错误重试

SDK 在传输层统一重试瞬时错误（Server 滚动重启、网关切换），与 Temporal Go SDK 一致：200ms 起翻倍、最长 10s、±20% 抖动；`ctx` 有 deadline 时重试到 deadline，没有时最多 1 分钟。`client.Options.Retry`（`client.RetryOptions{Disabled, InitialInterval, MaximumInterval, BackoffCoefficient, MaximumElapsedTime}`）调整，零值取缺省值；从这个 Client 创建的 Worker 用同一个策略。4xx 与 500 不重试。

| 失败 | 可以重发的调用 | 重发会再执行一次的调用 |
| --- | --- | --- |
| 连接没有建立、HTTP 429 / 503 | 重试 | 重试 |
| 连接中断、读应答中断、HTTP 502 / 504 | 重试 | 不重试 |

- 可以重发：`ExecuteWorkflow`（SDK 生成 request ID，`ID` 为空时也生成 Workflow ID；Server 对同一 request ID 返回这次启动开启的 Run 链的 current Run，失败重试、continue-as-new 之后是接替的 Run，链已结束也一样）、`SignalWorkflow` / `SignalWithStartWorkflow`（request ID：同一 Run 上只记一条 Signal；SignalWithStart 的 request ID 同时记在 `WorkflowStarted`，重发时 current Run（含已结束）或链首记过这条 Signal 就返回第一次的 Run 与 `event_id`，不启动新 Run）、Update（`UpdateID` 为空时 SDK 生成）、`CancelWorkflow`、`TerminateWorkflow` 与 Schedule 的 `Delete`（第一次应答丢失时重发报告 Run 已结束 / Schedule 不存在）、`UpsertSearchAttributes`、`UpdateWorkflowExecutionOptions`、Schedule 的 `Update` / `Pause` / `Unpause`、`StopBatchOperation`、全部读取与长轮询（`Get`、Update 句柄的 `Get`、Worker poll），以及 Worker 的任务上报（task token 定界）。
- 再执行一次：`ResetWorkflowExecution`（不带 `RequestID` 时）、Schedule 的 `Create` 与 `Trigger`、`StartBatchOperation`。遇到连接中断 / 502 / 504 时先读状态确认第一次是否生效，再决定是否重发；带 `RequestID` 的 Reset 用同一个 `RequestID` 重发即可。
- 应用代码自己再调用一次 `SignalWorkflow` 是另一个 Signal（新的 request ID）；需要跨进程去重时在 Signal 载荷里带业务幂等键。
- Worker 轮询在重试用尽后再按 100ms 起、最长 10s 退避；一段连续失败只记一条 WARN（`poll failed; retrying with backoff`），其余 DEBUG，恢复时 INFO（`poll recovered`）。

## 启动 Run 并等待结果

```go
run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-o-1"}, "order-flow", OrderInput{OrderID: "o-1"})
if err != nil {
    return err // 已有活跃 Run：errors.Is(err, client.ErrWorkflowExecutionAlreadyStarted)
}
var out OrderOutput
err = run.Get(ctx, &out)
```

- 第三个参数是 Workflow Type，即 `@Workflow(type=...)` 的字符串；没有「传函数」的写法。
- `StartWorkflowOptions.ID` 是 Workflow ID，留空时 SDK 生成 UUID；`run.GetID()`、`run.GetRunID()` 读回。
- 输入至多一个，编码成 JSON object：传 struct 或 map。结果同样是 JSON object。整数原样保留（int64，超出 int64 的为 uint64），超过 2^53 也不丢精度。
- 同一 Namespace + Workflow ID 同时只有一个活跃 Run；current Run 已结束时用同一个 Workflow ID 再次启动会产生新 Run。没有 ID 复用 / 冲突策略选项（`WorkflowIDConflictPolicyUseExisting` 只用于 Update-With-Start）。
- 执行超时、Run 超时、重试策略来自 `@Workflow` 注解生成的配置，启动时不能设置；也不能在启动时带 Search Attributes、Memo、Task Queue。
- 新 Run 使用该 Workflow Type 当前 active 的 Program；`SignalWithStartWorkflow`、Update-With-Start 相同。
- `c.GetWorkflow(ctx, workflowID, runID)` 返回已有执行的句柄；`runID` 为空时从 current Run 开始。

`run.Get` 跟随执行链：Run 以 continue-as-new 结束、或失败后按 Workflow 重试策略开始下一次尝试时，跟到新 Run，直到最后一个 Run 结束。`Get` 经 `wait-closed` 长轮询等 Run 结束（一次最长 20s，到期应答 204 后再发），应答带结束事件；Run 由其他 Server 副本结束时最多晚 100ms 返回。

| 结束方式 | `Get` 返回 |
| --- | --- |
| Completed | `nil`，结果解码进 `valuePtr`（nil 则丢弃） |
| Failed | `*client.WorkflowExecutionError`，`errors.As` 到 `*apperror.Error`：`Type()`、`NonRetryable()`、`Details(&v)`、`Unwrap()` |
| Failed（超时） | 同上，`TimeoutType()` 为 `client.TimeoutTypeWorkflowExecution` 或 `TimeoutTypeWorkflowRun` |
| Cancelled | `workflow.IsCanceledError(err)` / `apperror.IsCanceled(err)` 为 true |
| Terminated | `errors.As` 到 `*client.TerminatedError{Reason}` |

`WorkflowExecutionError.RunID` 是链上最后一个 Run。

## 读取 Run

```go
d, err := c.DescribeWorkflowExecution(ctx, workflowID, runID) // runID 空 = current Run
// d.Status: client.WorkflowExecutionStatusRunning / Completed / Failed / Canceled / Terminated / ContinuedAsNew
// d.RunID、WorkflowType、FirstRunID、PreviousRunID、Attempt、StartTime、Policy、ProgramVersion、ConfigError、UpgradeError、Blocked

events := c.GetWorkflowHistory(ctx, workflowID, runID)
for events.HasNext() {
    event, err := events.Next() // *history.Event，按事件 ID 升序逐页读取
    if err != nil {
        return err
    }
    _ = event
}

page, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    WorkflowID:       "order-o-1",
    Statuses:         []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusRunning},
    SearchAttributes: map[string]string{"region": "sg"},
    Limit:            50,
    NewestFirst:      true,
})
// page.Executions（WorkflowID、RunID、WorkflowType、Status、StartTime、CloseTime、SearchAttributes、HistoryLength、FailureReason）、page.Total、page.HasMore
```

`Statuses` 为空时不按状态过滤；`Limit` 缺省 50、最大 1000。`SkipTotal: true` 时不计算 `Total`（为 0），翻页看 `HasMore`，Run 很多时更快。`DescribeWorkflowExecution` 还带 `ConfigError`（Run 暂停的原因）与 `UpgradeError`（最近一次自动升级失败）；结果用 `Get` 读。

## Signal、Query

```go
err := c.SignalWorkflow(ctx, workflowID, runID, "approve", map[string]any{"ok": true}) // runID 空 = current Run

run, err := c.SignalWithStartWorkflow(ctx, workflowID, "approve", signalArg,
    client.StartWorkflowOptions{}, "order-flow", startInput)

value, err := c.QueryWorkflow(ctx, workflowID, runID, "progress")
var progress map[string]any
err = value.Get(&progress)
```

- Signal 参数是 JSON object，可以为 nil；向已结束的 Run 发 Signal 返回 `client.ErrInvalidArgument`。
- Query 参数至多一个 JSON object；运行中与已结束的 Run 都可以查询。查询类型没有 handler（`unknown queryType`）、首个 Workflow Task 未完成、handler 阻塞或拒绝时返回 `*client.QueryFailedError{QueryType, Message}`。

语义与失败场景见 `messaging.md`。

## Update

```go
handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
    WorkflowID:   workflowID,
    RunID:        "",                                   // 空 = current Run
    UpdateID:     requestID,                            // 稳定 ID 使重试幂等；空则由 SDK 生成
    UpdateName:   "change-limit",
    Args:         []any{int64(5000)},                   // 与 Handler 的业务参数一一对应
    WaitForStage: client.WorkflowUpdateStageCompleted,  // 或 WorkflowUpdateStageAccepted（缺省）
})
if err != nil {
    return err // errors.Is(err, client.ErrUpdateRejected)：Validator 拒绝，errors.As 取 *apperror.Error
}
var result ChangeLimitResult
if err := handle.Get(ctx, &result); err != nil {
    return err // errors.As(err, &appErr) 取 *apperror.Error
}
```

- `WorkflowUpdateStageAccepted`：Update 持久化后返回，`handle.Get` 继续等到 Completed。
- `WorkflowUpdateStageCompleted`：Handler 返回后才返回。
- `handle` 的方法：`WorkflowID()`、`RunID()`、`UpdateID()`、`Get(ctx, valuePtr)`；`Get` 可以多次、并发调用，`ctx` 到期后句柄仍可再 `Get`。只给 Workflow ID 时 `RunID()` 是接受这次 Update 的 Run，`Get` 的轮询固定发往它，continue-as-new 之后不会在新 Run 上重复执行。
- Run 在 Update 完成之前结束时 `Get` 返回 `*apperror.Error`。
- 没有 `GetWorkflowUpdateHandle`：Server 没有只读的 Update 轮询端点，用相同 `UpdateID` 再次 `UpdateWorkflow` 取结果。

## Update-With-Start

没有活跃 Run 时先启动再投递 Update，一次调用完成；有活跃 Run 时直接投递。

```go
start := c.NewWithStartWorkflowOperation(
    client.StartWorkflowOptions{
        ID:                       "create-" + input.RecordID, // 这个 Run 的 Workflow ID，必填
        WorkflowIDConflictPolicy: client.WorkflowIDConflictPolicyUseExisting, // 必填
    },
    "approval-workflow", // Workflow Type
    input,               // 零个或一个启动参数，编码成 JSON object
)

handle, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
    StartWorkflowOperation: start,
    UpdateOptions: client.UpdateWorkflowOptions{ // WorkflowID、RunID 留空
        UpdateName:   "create-record",
        WaitForStage: client.WorkflowUpdateStageCompleted,
    },
})
if err != nil {
    return err
}
var result CreateResult
if err := handle.Get(ctx, &result); err != nil {
    return err
}
```

- current Run 已结束时启动新 Run，用已记录的 Update ID 重试时返回当时的结果。
- 一个启动规格只执行一次；`start.Get(ctx)` 返回 `WorkflowRun`（`GetID()`、`GetRunID()`）。
- Workflow 侧的 Handler 写法见 `messaging.md`。

## 控制与运维

| 操作 | 方法 | 参考 |
| --- | --- | --- |
| 取消、终止 | `CancelWorkflow(ctx, workflowID, runID)`、`TerminateWorkflow(ctx, workflowID, runID, reason)` | `cancellation-timeouts.md` |
| 改 Run 的版本策略 | `UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{WorkflowID, RunID, Policy: workflow.PolicyPinned})` | `versioning.md` |
| 搜索属性 | `UpsertSearchAttributes(ctx, workflowID, runID, attrs)` | `search-attributes.md` |
| Reset | `ResetWorkflowExecution(ctx, client.ResetWorkflowExecutionOptions{WorkflowID, RunID, Reason, ResetType, EventID, ReapplyExcludeTypes, RequestID})`，返回新 Run ID；`RequestID` 相同的重复调用返回第一次的 Run ID | `operations.md` |
| Batch | `StartBatchOperation`（异步受理，返回 pending 记录）、`DescribeBatchOperation`、`ListBatchOperations`、`StopBatchOperation` | `operations.md` |
| Schedule | `c.ScheduleClient()`：`Create`、`GetHandle`、`List`；句柄 `Describe`、`Update`、`Pause`、`Unpause`、`Trigger`、`Delete` | `schedules.md` |

没有对应 Go 方法、只能经 HTTP 的：Reset Activity、按内容版本启动（`start-version`）、资源列表（Worker、Program、Task Queue、Namespace、Activity 状态）、能力查询。

## 错误

Server 的非 2xx 应答是 `*client.ServiceError{StatusCode, Message}`，用 `errors.Is` 判断：400 `ErrInvalidArgument`（Update 的 validator 拒绝是 `ErrUpdateRejected`）、401 `ErrUnauthenticated`（token 缺失或不对，不重试）、404 `ErrNotFound`、409 `ErrConflict`（启动类调用同时是 `ErrWorkflowExecutionAlreadyStarted`）、504 `ErrDeadlineExceeded`、503 与 502 `ErrUnavailable`（Server 依赖的数据库或任务队列暂时不可用；503 自动重试，502 只出现在 Reset、Schedule Create / Trigger、批量操作启动上，可能已生效）、500 `ErrInternal`。重试用尽后的网络错误与 `ctx` 的错误原样返回；TLS 握手被拒（证书不受信任）不重试。

## HTTP

全部是 `POST`，JSON body，路径前缀 `/runtime/workflow/`。失败时 body 是纯文本错误信息：请求本身的问题（参数不合法、Run 已结束等）返回 400，找不到资源（Run、current Run、Config、Schedule、Batch Operation）返回 404，与已有记录或并发写入冲突（Schedule ID 或 Run ID 已存在、同一定义版本以不同内容重复登记、并发修改）返回 409，Server 端等待超时返回 504；数据库或任务队列暂时不可用返回 503（带 `Retry-After`，非幂等端点 `reset`、`create-schedule`、`trigger-schedule`、`start-batch` 返回 502）；Server 故障返回 500，body 固定为 `internal server error`，原因看 Server 日志的 `runtime http request failed` 行。健康检查 `GET /health/live`、`GET /health/ready`。

| 路径 | 请求体 | 响应 |
| --- | --- | --- |
| `start` | `{namespace, workflow_type, workflow_id?, input, request_id?}`；current Run 由同一 `request_id` 启动时应答它 | `{run_id, workflow_id}` |
| `get-run` | `{namespace, workflow_id, run_id}` | `{run:{status, output, policy, config_error, upgrade_error, workflow_config_version, ...}, blocked:{blocked, cause, reason, consecutive_failures}, terminal_failure}` |
| `wait-closed` | `{namespace, workflow_id, run_id, timeout_millis?}` | Run 结束时 `{run, close_event}`；等待到期（缺省 30s，最长 60s）或 Server 停止时 Run 仍在运行则 204，再发一次 |
| `list` | `{filter}` | Run 列表 |
| `history`、`history-page` | Run 定位字段 | Event 列表 / 分页 |
| `signal` | `{namespace, workflow_id, run_id, name, input, request_id?}`；同一 Run 上同一 `request_id` 只记一条 | `{event_id}` |
| `signal-with-start` | `{namespace, workflow_type, workflow_id?, start_input, signal_name, signal_input, request_id?}`；同一个 request ID 的重发：current Run（运行中或已结束）记过这条 Signal，或 current Run 的链（continue-as-new、失败重试）由这个 request ID 启动时，返回第一次的 Run 与 `event_id`，不启动新 Run、不再记 Signal | `{run_id, workflow_id, event_id}` |
| `query` | `{namespace, workflow_id, run_id, name, input}` | `{query_result, query_rejected, query_error}` |
| `update` | `{namespace, workflow_id, run_id, name, update_id, wait_stage, args:[{encoding:"json/plain-v1", data:<base64 JSON>}]}` | Update 阶段与结果 |
| `update-with-start` | `{namespace, workflow_type, workflow_id?, start_input, update_name, update_id, update_args, wait_stage}` | Update 阶段与结果，含 `workflow_id` |
| `search-attributes` | `{namespace, workflow_id, run_id, search_attributes}`；保留键 `TemporalChangeVersion`、`TemporalScheduledById`、`TemporalScheduledStartTime` 返回 400 | — |
| `cancel`、`terminate` | `{namespace, workflow_id, run_id, reason}` | — |
| `set-policy` | `{namespace, run_id, policy}` | 更新后的 Run |
| `reset` | `{namespace, run_id, reason, reset_type, reset_event_id, reapply_exclude_types, request_id}`；`request_id` 可选，重复的请求返回第一次的 `run_id` | `{run_id}` |
| `reset-activity` | `{namespace, run_id, activity_id, reset_heartbeat, reason}`；`activity_id` 指定一个未关闭的 Activity | — |
| `start-batch`、`get-batch`、`list-batches`、`cancel-batch` | 见 `operations.md` | Batch 记录（`start-batch` 应答 202） |
| `create-schedule` 等 | 见 `schedules.md` | Schedule |
| `list-configs`、`list-workers`、`list-task-queues`、`list-namespaces`、`capabilities`、`activity-states` | 过滤字段 | 资源列表 |
| `register`、`artifacts` | 生成器产物（由 `tenuto-workflow-gen -url` 调用） | `{namespace, workflow_type, version}` |

所有 POST 必须带 `Content-Type: application/json`（没有请求体的 `capabilities` 等也一样，发 `-d '{}'`），否则 415。按 Run ID 或 Workflow ID 寻址的端点（上表带 `run_id` / `workflow_id` 的，以及 `get-config`、`activity-states`、`history`、`history-page`）都带 `namespace`，省略即 `default`。给出的 `run_id` 不属于请求的 Namespace（或不属于给出的 `workflow_id`）时返回 404 `run not found`，与 Run 不存在相同。`wait_stage` 取 `Accepted` 或 `Completed`，其他取值返回 400；Update 的结果只在 `outcome` 里。

```bash
curl -sS -X POST http://127.0.0.1:7400/runtime/workflow/start \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"default","workflow_type":"startup-greeting","workflow_id":"greeting-1","input":{"name":"tenuto"}}'

curl -sS -X POST http://127.0.0.1:7400/runtime/workflow/get-run \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"default","workflow_id":"greeting-1","run_id":""}'
```

`update` 的 `args` 每项是一个 Payload，`data` 是参数 JSON 的 base64；Go 调用端用 `sdk/client` 省去这层编码。Console 使用的运维端点（`list-configs`、`list-workers`、`get-config`、`register` 等）在 `sdk/client` 里没有对应方法，用 HTTP 调用。异步完成 Activity 的 `POST /runtime/activity/complete` 见 `activities.md`。

精确的请求与响应结构以代码为准：`protocol/runtimeapi`（路径与请求应答类型）、`server/http/runtimehttp/api_workflow.go`（路由）、`sdk/client`（Go 客户端，错误映射在 `sdk/client/errors.go`）。
