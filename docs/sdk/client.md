---
title: Go Client
description: 用 sdk/client 启动 Workflow、等待结果、发送 Signal/Query/Update、管理 Run、Schedule 与批量操作
status: Stable
---

# Go Client

<FeatureStatus status="Stable" />

应用代码经 `github.com/donutnomad/tenuto/sdk/client` 访问 Server。接口与语义对齐 Temporal Go SDK 的 `client.Client`，只包含 Server 支持的操作；完整签名见 [Go Client API](/reference/client-api)。同一个 `Client` 可以被多个 goroutine 共用，也交给 [`sdk/worker`](/develop/workers) 创建 Worker。

## 连接

```go
import "github.com/donutnomad/tenuto/sdk/client"

c, err := client.Dial(client.Options{
    HostPort:  "tenuto-server:7400", // 或完整 URL：https://tenuto.example.com
    Namespace: "orders",             // 空值为 default
})
if err != nil {
    return err
}
defer c.Close()
```

| 字段 | 缺省 | 含义 |
| --- | --- | --- |
| `HostPort` | 必填 | runtime HTTP 地址；没有 scheme 时按 `http`，配置了 `TLS` 时按 `https` |
| `Namespace` | `default` | 本 Client 全部调用作用的 Namespace |
| `Identity` | `<pid>@<hostname>` | 调用方标识；从这个 Client 创建的 Worker 以它作为缺省 Identity |
| `HTTPClient` | Dial 自建 | 发送请求的 `*http.Client`；网关要求其他凭证（按请求刷新的 token、签名头）时在这里配置 Transport |
| `AuthToken` | 空 | 每个请求带 `Authorization: Bearer <AuthToken>`，对应 Server 的 `WORKFLOW_HTTP_AUTH_TOKEN`；与 `HTTPClient` 同时设置时加在它的 Transport 之外 |
| `TLS` | 空 | Dial 自建客户端的 `*tls.Config`：`RootCAs` 校验 Server 证书，`Certificates` 是 mTLS 客户端证书；与 `HTTPClient` 互斥 |
| `Telemetry` | 空 | OpenTelemetry providers，覆盖 HTTP 传播与 Update spans |
| `Retry` | 见下节 | 瞬时错误的重试策略；从这个 Client 创建的 Worker 的轮询与上报使用同一个策略 |

Server 开启内置认证（见[安全部署](/deploy/security)）时：

```go
cert, err := tls.LoadX509KeyPair("client.pem", "client-key.pem") // 只在 mTLS 时需要
if err != nil {
    return err
}
c, err := client.Dial(client.Options{
    HostPort:  "tenuto.internal:7400",               // 配置了 TLS，按 https 连接
    AuthToken: os.Getenv("TENUTO_AUTH_TOKEN"),        // WORKFLOW_HTTP_AUTH_TOKEN 中的一个
    TLS:       &tls.Config{RootCAs: serverCAs, Certificates: []tls.Certificate{cert}},
})
```

从这个 Client 创建的 Worker 沿用同一个 token 与证书。401 与 TLS 握手被拒是配置错误，SDK 不重试。

`Dial` 不访问 Server：地址错误或 Server 不可达在第一次调用时报告。每个方法都在 `ctx` 取消或到期时返回 `ctx` 的错误。

## 瞬时错误与重试

Server 副本滚动重启、网关切换时，请求会遇到连接被拒绝、连接中断或网关的 502/503/504；Server 依赖的 MySQL 或任务队列暂时不可用（连接数打满、锁等待超时、死锁、连接中断、分区换主）时，Server 对可以重发的调用应答 503（带 `Retry-After`），对重发会再执行一次的调用应答 502。SDK 在传输层的同一个位置重试这些瞬时错误，与 Temporal Go SDK 的 gRPC 重试一致：指数退避（200ms 起，每次翻倍，最长 10s）加 ±20% 抖动；`ctx` 有 deadline 时重试到 deadline 为止，没有 deadline 时从第一次失败起重试 1 分钟，之后返回最后一次的错误。4xx 与 500 是确定的结果，不重试。

```go
c, err := client.Dial(client.Options{
    HostPort: "tenuto-server:7400",
    Retry: client.RetryOptions{
        InitialInterval:    100 * time.Millisecond,
        MaximumInterval:    5 * time.Second,
        MaximumElapsedTime: 30 * time.Second, // ctx 没有 deadline 时
        // Disabled: true 关闭重试，每个调用只发一次
    },
})
```

| 失败 | 可以重发的调用 | 重发会再执行一次的调用 |
| --- | --- | --- |
| 连接没有建立（拒绝连接、DNS 解析失败） | 重试 | 重试 |
| HTTP 429、503 | 重试 | 重试 |
| 连接中断、读应答中断、HTTP 502、504 | 重试 | 不重试，错误返回给调用方 |

请求可能已经被 Server 处理的失败只对可以重发的调用重试。各调用的归类：

| 调用 | 归类 | 原因 |
| --- | --- | --- |
| `ExecuteWorkflow` | 可以重发 | SDK 为每次调用生成 request ID（`ID` 为空时同时生成 Workflow ID），Server 认出重发的启动，返回第一次启动的 Run，Run 已经结束也一样 |
| `SignalWorkflow`、`SignalWithStartWorkflow` | 可以重发 | SDK 为每次调用生成 request ID，同一个 Run 上同一个 request ID 只记一条 `WorkflowSignaled` |
| `UpdateWorkflow`、`UpdateWithStartWorkflow`、Update 句柄的 `Get` | 可以重发 | `UpdateID` 为空时 SDK 生成，Server 按 Update ID 去重 |
| `CancelWorkflow` | 可以重发 | 已请求过取消时返回成功 |
| `TerminateWorkflow`、Schedule 的 `Delete` | 可以重发 | 效果相同；第一次的应答丢失时，重发报告 Run 已结束（`ErrInvalidArgument`）或 Schedule 不存在（`ErrNotFound`） |
| `UpsertSearchAttributes`、`UpdateWorkflowExecutionOptions`、Schedule 的 `Update` / `Pause` / `Unpause`、`StopBatchOperation` | 可以重发 | 写入相同的值 |
| `QueryWorkflow`、`DescribeWorkflowExecution`、`GetWorkflowHistory`、`ListWorkflow`、`WorkflowRun.Get`、Schedule 与批量操作的读取 | 可以重发 | 只读；`Get` 的长轮询断开后重发，继续等待 |
| `ResetWorkflowExecution` | 再执行一次 | 每次 Reset 分叉出一个新 Run |
| Schedule 的 `Create` | 再执行一次 | Schedule ID 由 Server 生成，重发会建第二个 Schedule |
| Schedule 的 `Trigger` | 再执行一次 | 每次触发启动一个 Run |
| `StartBatchOperation` | 再执行一次 | 每次受理一个新的批量操作 |

「再执行一次」的调用遇到连接中断、502、504 时，调用方先读状态（`ListWorkflow`、`List`、`ListBatchOperations`）确认第一次是否生效，再决定是否重发。

Worker 的长轮询、任务上报（完成、失败、心跳）都由 task token 定界，按可以重发的调用重试。重试用尽后轮询循环再按 100ms 起、最长 10s 的指数退避重新轮询：一段连续失败只在开始时记一条 WARN（其后的失败记 DEBUG），恢复时记一条 INFO。

## 启动 Workflow 并等待结果

```go
run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-1001"},
    "place-order", OrderInput{OrderID: "order-1001", Amount: 8850})
if err != nil {
    return err
}
var out OrderOutput
if err := run.Get(ctx, &out); err != nil {
    return err
}
```

- 第三个参数是 Workflow Type，即 `@Workflow(type=...)` 的取值。
- `args` 至多一个，编码成 JSON 对象（结构体或 map）作为 Workflow 的输入；结果同样是 JSON 对象，`Get` 解码进 `valuePtr`。整数保留精确值，2^53 以上不丢精度。
- `StartWorkflowOptions.ID` 是 Workflow ID，留空时 SDK 生成一个 UUID。同一个 Workflow ID 同时至多一个运行中的 Run：已有运行中的 Run 时返回 `ErrWorkflowExecutionAlreadyStarted`；上一个 Run 结束之后可以用同一个 ID 再启动。规则见 [启动 Run](/develop/workflows#启动-run)。
- 执行超时、Run 超时与 Workflow 重试策略来自 `@Workflow` 注解生成的配置，不在启动时设置。

`GetWorkflow(ctx, workflowID, runID)` 返回已有执行的句柄，不访问 Server；`runID` 为空时 `Get` 从 Workflow ID 的当前 Run 开始。

### Get 跟随执行链

Run 以 continue-as-new 结束，或失败后按 Workflow 的重试策略开始下一次尝试时，`Get` 跟到新 Run，直到链上最后一个 Run 结束：

1. 以 `POST /runtime/workflow/wait-closed` 长轮询等待 Run 结束，应答带结束后的 Run 与结束事件。Server 一次最多等 20s（`Get` 请求的时长），到期时 Run 仍在运行则应答 204，`Get` 再发一次。Run 由受理请求的 Server 副本结束时立即返回，由其他副本结束时最多晚 100ms（见 [跨副本唤醒](/develop/updates#跨副本唤醒)）。
2. 按结束事件决定：`WorkflowContinuedAsNew` 的 `nextRunId`、带 `newExecutionRunId` 的 `WorkflowFailed` 指向下一个 Run，`Get` 回到第 1 步。
3. 其余结束事件给出结果或错误。

| 结束事件 | `Get` 的返回 |
| --- | --- |
| `WorkflowCompleted` | `nil`，结果解码进 `valuePtr` |
| `WorkflowFailed` | `*WorkflowExecutionError`，原因是 `*apperror.Error`（Type、NonRetryable、Details、Cause 与 Workflow 返回的应用错误相同） |
| `WorkflowFailed`（超时） | 同上，`TimeoutType()` 是 `client.TimeoutTypeWorkflowExecution` 或 `TimeoutTypeWorkflowRun` |
| `WorkflowCancelled` | `*WorkflowExecutionError`，原因是取消错误：`workflow.IsCanceledError` 与 `apperror.IsCanceled` 为 true，Details 是 Workflow 给出的 details |
| `WorkflowTerminated` | `*WorkflowExecutionError`，原因是 `*client.TerminatedError{Reason}` |

```go
err := run.Get(ctx, &out)
var appErr *apperror.Error
switch {
case err == nil:
case workflow.IsCanceledError(err):
    // Workflow 确认了取消
case errors.As(err, &appErr) && appErr.Type() == "PaymentDeclined":
    var detail DeclineDetail
    _ = appErr.Details(&detail)
}
```

`WorkflowExecutionError.RunID` 是链上最后一个 Run。

## Signal、Signal-With-Start 与 Query

```go
err := c.SignalWorkflow(ctx, "order-1001", "", "approval-result", map[string]any{"approved": true})

run, err := c.SignalWithStartWorkflow(ctx, "order-1001", "approval-result", map[string]any{"approved": true},
    client.StartWorkflowOptions{}, "place-order", OrderInput{OrderID: "order-1001"})

value, err := c.QueryWorkflow(ctx, "order-1001", "", "progress")
var progress Progress
err = value.Get(&progress)
```

- `runID` 为空时作用于 Workflow ID 的当前 Run。Signal 参数是 JSON 对象，可以为 `nil`；向已结束的 Run 发 Signal 返回 `ErrInvalidArgument`。每次调用带一个 SDK 生成的 request ID，重试的重发在同一个 Run 上只记一条 Signal；应用代码自己再调用一次是另一个 Signal。
- `SignalWithStartWorkflow` 在 Server 上原子完成：Workflow ID 有运行中的 Run 时把 Signal 投给它，否则用 `workflowType` 与参数启动新 Run 再投递。语义见 [Signal-With-Start](/sdk/signal-with-start)。
- Query 把历史重放到最近一个已完成的 Workflow Task 再调用 handler，运行中与已结束的 Run 都可以查询。查询类型没有 handler、Run 还没有完成第一个 Workflow Task、handler 阻塞或拒绝时返回 `*client.QueryFailedError`。参数与结果都是 JSON 对象。

## Update

```go
handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
    WorkflowID:   "order-1001",
    UpdateName:   "change-limit",
    Args:         []any{int64(500)},
    WaitForStage: client.WorkflowUpdateStageCompleted,
})
if err != nil {
    return err // validator 拒绝：errors.Is(err, client.ErrUpdateRejected)
}
var result ChangeLimitResult
err = handle.Get(ctx, &result)
```

`Args` 按 handler 业务参数的顺序排列，每个编码成一个 `json/plain-v1` payload。`UpdateID` 为空时 SDK 生成一个 UUID，句柄的 `UpdateID()` 返回它。`WaitForStage` 为 `Accepted`（缺省）时在 Update 写入历史后返回句柄，`Completed` 时等 handler 返回。`handle.Get` 等 handler 的结果：Server 的长轮询在软超时时返回 Accepted，`Get` 接着再等，直到完成或 `ctx` 结束；`ctx` 结束后同一个句柄可以再次 `Get`。handler 返回错误、或 Run 在 Update 完成之前结束时，`Get` 返回 `*apperror.Error`。只给 Workflow ID 发出的 Update 固定在接受它的 Run 上，continue-as-new 之后不会在新 Run 上再执行一次。

Update-With-Start 见 [Update-With-Start](/sdk/update-with-start)，handler 的写法见 [Update](/develop/updates)。

## 取消与终止

```go
err := c.CancelWorkflow(ctx, "order-1001", "")
err = c.TerminateWorkflow(ctx, "order-1001", "", "operator action")
```

取消是交给 Workflow 代码的请求：根 Context 被取消，Workflow 可以清理、补偿，终态由它的返回值决定（返回取消错误为 Cancelled，返回 `nil` 为 Completed），见 [取消与超时](/develop/cancellation-timeouts)。终止立即关闭 Run，Workflow 代码不再运行，`reason` 写进 `WorkflowTerminated`。

## 读取 Run

```go
description, err := c.DescribeWorkflowExecution(ctx, "order-1001", "")
// description.Status、RunID、WorkflowType、Attempt、Policy、Blocked ...

events := c.GetWorkflowHistory(ctx, "order-1001", "")
for events.HasNext() {
    event, err := events.Next()
    if err != nil {
        return err
    }
    log.Println(event.ID, event.EventType())
}

page, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    Statuses:         []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusRunning},
    SearchAttributes: map[string]string{"business_id": "merchant-42"},
    Limit:            50,
    NewestFirst:      true,
})
```

- `DescribeWorkflowExecution` 返回状态、执行链（`FirstRunID`、`PreviousRunID`）、重试序号、超时到期时刻、搜索属性、版本策略与 Program 内容版本，以及版本管理的 `ConfigError`（Run 暂停的原因）与 `UpgradeError`；Workflow Task 连续失败而停滞时 `Blocked` 非 nil。
- `GetWorkflowHistory` 按事件 ID 升序逐页读取（每页 200 个），读取失败时 `HasNext` 返回 true、`Next` 返回错误。`runID` 为空时先解析当前 Run，后续页都读这个 Run。
- `ListWorkflow` 的条件之间是「与」：`WorkflowID`（含同一 ID 的全部 Run）、`Statuses`（为空时不按状态过滤）、`SearchAttributes`（每个键完全相等），`Offset` / `Limit` 分页（`Limit` 缺省 50、最大 1000，超出返回 400），`Total` 是满足条件的总数。

## Reset 与版本策略

```go
newRunID, err := c.ResetWorkflowExecution(ctx, client.ResetWorkflowExecutionOptions{
    WorkflowID: "order-1001",
    Reason:     "replay after fix",
    ResetType:  client.ResetTypeLastWorkflowTask, // 或 EventID：一个 Workflow Task 事件的 ID
})

err = c.UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{
    WorkflowID: "order-1001",
    Policy:     workflow.PolicyPinned,
})
```

Reset 从所选 Workflow Task 处分叉出新 Run，旧 Run 被终止，分叉点之后的 Signal 与 Update 缺省重新应用到新 Run；规则见 [Reset](/operations/reset)。`UpdateWorkflowExecutionOptions` 覆盖单个 Run 的版本策略（[版本管理](/develop/versioning)）。

## Schedule

```go
schedules := c.ScheduleClient()
handle, err := schedules.Create(ctx, client.ScheduleOptions{
    WorkflowType:   "nightly-report",
    Args:           []any{ReportInput{Region: "eu"}},
    CronExpression: "0 2 * * *",
    TimeZone:       "Europe/Berlin",
    OverlapPolicy:  client.ScheduleOverlapPolicySkip,
})
description, err := handle.Describe(ctx)
err = handle.Pause(ctx)
err = handle.Unpause(ctx)
run, err := handle.Trigger(ctx) // 立即触发一次，返回启动的 WorkflowRun
err = handle.Delete(ctx)
```

Schedule ID 由 Server 生成。`Update` 只改写非 nil 的字段；`List` 按 Workflow Type 与是否暂停过滤。Schedule 启动的 Run 带搜索属性 `TemporalScheduledById`（Schedule ID）与 `TemporalScheduledStartTime`（名义触发时间），列出一个 Schedule 的全部 Run：

```go
runs, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalScheduledById": handle.GetID()},
    NewestFirst:      true,
})
```

语义见 [调度](/develop/scheduling)。

## 批量操作

```go
op, err := c.StartBatchOperation(ctx, client.StartBatchOperationOptions{
    Type:   client.BatchOperationTypeTerminate,
    Filter: client.ListWorkflowOptions{SearchAttributes: map[string]string{"business_id": "merchant-42"}},
    Reason: "merchant offboarded",
})
op, err = c.DescribeBatchOperation(ctx, op.ID)
```

Server 在后台处理，`StartBatchOperation` 返回受理时的 `pending` 记录；`Filter.Statuses` 为空时只选运行中的 Run。`StopBatchOperation` 停止 pending 或 running 的批量操作，已处理的 Run 不回滚；已结束的批量操作返回 `ErrConflict`。见 [Batch Operation](/operations/batch-operations)。

## 错误

Server 的非 2xx 应答映射成 `*client.ServiceError{StatusCode, Message}`，用 `errors.Is` 匹配：

| HTTP 状态 | 匹配的错误 |
| --- | --- |
| 400 | `ErrInvalidArgument`（Update 的 validator 拒绝除外） |
| 400 + Failure JSON（update、update-with-start） | `ErrUpdateRejected`，并可 `errors.As` 到 `*apperror.Error` |
| 401 | `ErrUnauthenticated`（`AuthToken` 缺失或不对，不重试） |
| 404 | `ErrNotFound` |
| 409 | `ErrConflict`；启动类调用（ExecuteWorkflow、SignalWithStartWorkflow）同时是 `ErrWorkflowExecutionAlreadyStarted` |
| 504 | `ErrDeadlineExceeded` |
| 503、502 | `ErrUnavailable`（重试用尽后返回；502 出现在重发会再执行一次的调用上，请求可能已经生效，重发前先读状态确认） |
| 500 | `ErrInternal`（细节只在 Server 日志里） |

重试用尽后的网络错误与 `ctx` 的错误原样返回。

## 与 Temporal Go SDK 的差别

| Temporal | 这里 |
| --- | --- |
| `ExecuteWorkflow(ctx, options, workflowFunc, args...)` | 第三个参数是 Workflow Type 字符串；生成器不产出「函数 → Type」的映射 |
| `StartWorkflowOptions` 的 TaskQueue、超时、RetryPolicy、SearchAttributes、Memo、ID 复用策略 | 不支持：超时与重试来自注解生成的配置；Workflow Task 不分队列；同 ID 只拒绝运行中的 Run |
| `CancelWorkflow` / `TerminateWorkflow` 的 details | 不支持 details |
| `GetWorkflowUpdateHandle`、`DescribeWorkflow`、`CountWorkflow`、`ListOpenWorkflow` 等 | 没有对应操作：Server 没有只读的 Update 轮询端点；其余由 `DescribeWorkflowExecution` 与 `ListWorkflow` 覆盖 |
| 自定义 DataConverter、Interceptors | 不支持：载荷编码由 Server 与 Worker 固定为 JSON |
| `CompleteActivity`、`RecordActivityHeartbeat`（client 侧） | 不支持：异步完成与心跳由 Worker 进程内的 Activity 完成 |
