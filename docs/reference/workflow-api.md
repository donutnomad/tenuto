---
title: Workflow Go API
description: sdk/workflow 的公开函数、Future、并发原语和约束
status: Stable
---

# Workflow Go API

<FeatureStatus status="Stable" />

`github.com/donutnomad/tenuto/sdk/workflow` 中的函数在源码翻译期作为 marker，由 WLang runtime 在执行和重放时绑定。

## 执行与等待

| API | 返回 | 说明 |
| --- | --- | --- |
| `ExecuteActivity(ctx, activity, input, opts...)` | `ActivityFuture` | 调度 Activity；`opts` 是 `ActivityOptions`，只作用于这一次调用；`ActivityOptions.ActivityID` 是这次调用在 Run 内的 ID，见 [ActivityID](/sdk/activity-options#activityid) |
| `ActivityFuture.Get(ctx, out)` | `error` | 等待 Activity 结果 |
| `Sleep(ctx, duration)` | `error` | Durable Timer |
| `Await(ctx, condition)` | `error` | 等待纯内存条件；条件不满足而 ctx 已取消时返回取消错误 |
| `ExecuteChildWorkflow(ctx, type, input, opts...)` | `ChildWorkflowFuture` | 启动 Child Workflow；`ChildWorkflowOptions.ParentClosePolicy` 选择父关闭策略，`WaitForCancellation` 选择取消后是否等子 Workflow 结束 |
| `ContinueAsNew(ctx, type, input)` | `error` | 关闭当前 Run 并创建下一段 |
| `GetInfo(ctx)` | `Info` | 当前 activation 的 History 快照 |

## Context 与取消

| API | 说明 |
| --- | --- |
| `Context` | 接口：`Done() Channel`、`Err() error`。Run 收到取消请求时根 Context 被取消 |
| `ctx.Done()` | 作用域取消时关闭的 Channel，可放进 `Selector.AddReceive` |
| `ctx.Err()` | 未取消时 `nil`，取消后 `ErrCanceled` |
| `WithCancel(ctx) (Context, CancelFunc)` | 可取消的子作用域；父作用域取消时一起取消 |
| `NewDisconnectedContext(ctx) (Context, CancelFunc)` | 不随父作用域取消的新作用域，用于取消之后的清理 |
| `ErrCanceled` | `ctx.Err()` 的取消错误 |
| `NewCanceledError(details...) error` | 构造带 details 的取消错误；Workflow 返回它时 Run 以 Cancelled 结束 |
| `IsCanceledError(err) bool` | err 或 cause 链上是否有取消错误 |
| `ActivityOptions.WaitForCancellation` | `false`（缺省）：取消请求发出即返回取消错误；`true`：等 Activity 结束 |

语义与规则见 [取消、超时与终止](/develop/cancellation-timeouts#取消)。

## 消息

| API | 说明 |
| --- | --- |
| `WaitSignal(ctx, name)` | 返回 Signal Future |
| `SetUpdateHandler(ctx, name, handler)` | 注册 Update Handler；没有同名 Handler 的 Update 以 `UnknownUpdate` 完成 |
| `SetQueryHandler(ctx, name, handler) error` | 注册只读 Query Handler；名称为空或 Handler 形状不对时返回错误；Handler 调用阻塞原语或 `Go` 时该次 Query 被拒 |
| `NewApplicationError(message, type, details...)` | 创建稳定业务失败 |
| `IsApplicationError(err, type) bool` | 判断应用错误的 Type |
| `TimeoutTypeOf(err) TimeoutType` | 超时错误的超时种类（`TimeoutTypeScheduleToStart`、`TimeoutTypeStartToClose`、`TimeoutTypeScheduleToClose`、`TimeoutTypeHeartbeat`）；不是超时错误时为空串 |

`ActivityOptions` 的四个超时（`ScheduleToStartTimeout`、`StartToCloseTimeout`、`ScheduleToCloseTimeout`、`HeartbeatTimeout`）与 `RetryPolicy` 的 `InitialInterval`、`MaxInterval` 都是 `time.Duration`。`RetryPolicy` 的零值字段取 Server 的缺省值，见 [Activity 重试与超时](/sdk/activity-options#retry-policy)。

Typed Update Handler 支持：

```go
func(workflow.Context, ...Args) error
func(workflow.Context, ...Args) (Result, error)
```

## 确定性

| API | 说明 |
| --- | --- |
| `Now(ctx)` | History 时间 |
| `SideEffect(ctx, func(ctx) any)` | 固化一次性非确定性值 |
| `MutableSideEffect(ctx, id, func, equals)` | 值变化时记录 Marker |
| `GetVersion(ctx, changeID, min, max) int` | 固化代码版本分支；History 没有记录时返回 `DefaultVersion`（`-1`） |
| `RandomUUID(ctx)` | Marker-backed UUID |
| `RandomInt(ctx, max)` | Marker-backed 随机整数 |

## 并发

| API | 说明 |
| --- | --- |
| `Go(ctx, closure)` | 启动协作式 lane |
| `NewWaitGroup(ctx)` | 创建 WaitGroup |
| `WaitGroup.Go/Wait/Add/Done` | 管理结构化并发 |
| `NewChannel(ctx)` | 非缓冲 Channel |
| `NewBufferedChannel(ctx, size)` | 缓冲 Channel |
| `Channel.Send/Receive/Close` | lane 间通信 |
| `NewSelector(ctx)` | 多路等待 |
| `Selector.AddReceive/AddSend/AddDefault` | Channel case |
| `Selector.AddFuture` | Activity Future case |
| `AddSignalFuture/AddChildFuture` | 其他 Future case |
| `Selector.Select(ctx)` | 执行首个 ready case |

闭包使用内联函数，循环捕获当前值。详细约束见 [并发、Channel 与 Selector](/develop/concurrency)。

## Activity Context

| API | 说明 |
| --- | --- |
Activity 作者用到的 API 在 `sdk/activity` 包：

| API | 说明 |
| --- | --- |
| `activity.GetInfo(ctx) activity.Info` | 读取本次 Attempt 的信息：`WorkflowID`、`WorkflowType`、`RunID`、`Namespace`、`ActivityID`、`ActivityType`、`TaskQueue`、`Attempt`、`ScheduledTime`、`StartedTime`、`Deadline`、`HeartbeatTimeout`、`TaskToken` |
| `activity.RecordHeartbeat(ctx, details)` | 上报心跳与进度；Workflow 请求取消这个 Activity 后，响应让 Worker 取消 `ctx` |
| `activity.GetLastHeartbeatDetails(ctx)` | 读取上一次 Attempt 最后记录的进度 |
| `activity.ErrPending` | 返回它表示结果由外部异步提交 |
| `activity.NewRegistry()`、`activity.RegisterActivity(reg, type, fn)` | 登记实现与契约 |

Activity 使用标准 `context.Context`，Workflow 使用 `workflow.Context`。
