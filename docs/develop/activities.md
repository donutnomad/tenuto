---
title: 编写 Activity
description: 用 @Activity 注解声明、注册强类型 Activity 并从 Workflow 调度
status: Stable
---

# 编写 Activity

<FeatureStatus status="Stable" />

Activity 是 Tenuto 执行外部副作用的边界。它是原生 Go 函数，使用标准 `context.Context`，可以访问数据库、HTTP 客户端、MQ、文件系统、系统时间和任意 Go 库。

## 定义 Activity

```go
type ReserveInput struct {
    OrderID string `json:"order_id"`
}

type ReserveOutput struct {
    ReservationID string `json:"reservation_id"`
}

// @Activity(type="lifecycle.reserve",queue="lifecycle")
func Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
    return ReserveOutput{ReservationID: "reservation-" + in.OrderID}, nil
}
```

签名固定为 `func(context.Context, In) (Out, error)`。输入输出经 JSON 在结构体与运行时 Payload 之间转换，字段名取 `json` tag。

`In` 与 `Out` 是 struct 或 `map[string]any`：Activity 的输入输出在线上是一个 JSON object。单个值包进 struct 的字段：

```go
type LengthInput struct {
    Text string `json:"text"`
}

type LengthOutput struct {
    Length int `json:"length"`
}
```

`string`、`int`、切片、指针等其他形状在生成期报错，手写注册时由 `activity.RegisterActivity` 报错，错误信息写出 Activity Type、输入或输出一侧、Go 类型。

| 注解 key | 必填 | 含义 |
| --- | --- | --- |
| `type` | 是 | Activity Type，Workflow 调度和 Worker 注册共用的稳定名称 |
| `queue` | 否 | Activity Task Queue；省略时定义不声明队列，由 Server 按 Worker 的登记选择（Worker 在 `ActivityTaskQueues` 上提供它，不写时是 `default`），见[Activity 的队列如何决定](/operations/namespaces-task-queues#activity-的队列如何决定) |

实现位于其他服务的 Activity 用 `@RemoteActivity` 标在命名函数类型上，key 与 `@Activity` 相同，见下文「远程 Activity 契约」。

`@Workflow` 编排、`@Activity` 实现与 DTO 可以放在同一个包的多个文件里。完整的注解规则见 [注解与代码生成](/develop/code-generation)。

## 从 Workflow 调度

生成器为每个 `@Activity` 写出桥 `CallX`：

```go
reserved, err := activities.CallReserve(ctx, activities.ReserveInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
    return Output{}, err
}
```

带调用选项：

```go
paid, err := CallChargePayment(ctx, ChargePaymentInput{
    OrderID: in.OrderID,
    Amount:  in.Amount,
}, workflow.ActivityOptions{
    StartToCloseTimeout: 30 * time.Second,
    RetryPolicy: workflow.RetryPolicy{
        MaxAttempts:        3,
        InitialInterval:    time.Second,
        BackoffCoefficient: 2,
        MaxInterval:        30 * time.Second,
    },
})
```

没有设置 `RetryPolicy` 的调用按 Server 的缺省策略不限次数重试（间隔 1s 起按 2 倍增长、最长 100s），直到成功、遇到不可重试的错误或 `ScheduleToCloseTimeout` 到期；设置了部分字段的策略，其余字段取缺省值。见 [Activity 重试与超时](/sdk/activity-options#retry-policy)。所有时长字段都是 `time.Duration`。

桥调用可以写在 `workflow.Go` / `WaitGroup.Go` 的内联闭包里，用于并发执行多条 Activity 链，见 [并发、Channel 与 Selector](/develop/concurrency)。

### 直接使用 ExecuteActivity

`workflow.ExecuteActivity` 返回 Future，调用时立即产生调度命令，`Get` 只等待结果：

```go
f1 := workflow.ExecuteActivity(ctx, activities.CheckBalance, walletA, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
f2 := workflow.ExecuteActivity(ctx, activities.CheckBalance, walletB, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})

var a map[string]any
if err := f1.Get(ctx, &a); err != nil {
    return err
}
var b map[string]any
if err := f2.Get(ctx, &b); err != nil {
    return err
}
```

签名是 `ExecuteActivity(ctx, activity, input, opts...)`。第二个参数 `activity` 指向带 `@Activity` 注解的函数或 remote 契约类型的标记函数时，直接调用与桥 `CallX` 得到同一条引用：Activity Type、Task Queue 与输入输出契约都取自注解。

第二个参数指向没有注解的函数时：

| 项 | 取值 |
| --- | --- |
| Activity Type | 符号文本 `pkg.Func`（如 `activities.CheckBalance`） |
| Task Queue | 这次调用的 `ActivityOptions.TaskQueue`，只作用于这一次调用；省略时由 Server 按 Worker 的登记选择，见[Activity 的队列如何决定](/operations/namespaces-task-queues#activity-的队列如何决定) |
| 契约 | 无声明，Worker 领取时不对照 |
| 输入 | struct（字面量或变量）或 `map[string]any`，按 Program 的类型表编码成 JSON |

Worker 用符号文本作为名称登记这类实现：

```go
activity.RegisterActivity(registry, "activities.CheckBalance", CheckBalance)
```

返回的 Future 可以不调用 `Get`（只调度、不等待结果），可以放进切片后逐个 `Get`，也可以在 `for` 循环里创建。选项、超时与重试见 [Activity 执行生命周期](/develop/activity-execution)。

## 远程 Activity 契约

实现位于另一个进程或另一个团队的 Activity，用命名函数类型声明契约：

```go
// @RemoteActivity(type="payments.settle",queue="payments")
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
```

Workflow 侧调用生成的 `remote.CallSettle(ctx, in)`。拥有该 Activity 的 Worker 用同一个 `queue` 和 `type` 登记实现：

```go
err := activity.RegisterActivityOnQueue(registry, "payments", "payments.settle", Settle)
```

两侧 DTO 可以各自定义，JSON 字段名、类型与可选性一致时 wire 指纹相同。

## 注册 Activity

`activity.Registry` 保存一个 Worker 进程能执行的 Activity，每个类型一个实现、一份契约和它所在的 Task Queue。生成的 `RegisterActivities` 一次登记本包全部本地实现，连同注解里的 `queue`：

```go
import (
    "github.com/donutnomad/tenuto/sdk/activity"
    "github.com/donutnomad/tenuto/sdk/worker"
)

registry := activity.NewRegistry()
if err := sagaflow.RegisterActivities(registry); err != nil {
    return err
}

activityWorker, err := worker.New(c, worker.Options{
    Role:               worker.RoleActivity,
    Activities:         registry,
    // ...
})
```

Worker 在 `Start` 时把 registry 里的全部契约随路由一起上报。`@RemoteActivity` 契约没有本地实现，不进入 `RegisterActivities`。

`ActivityTaskQueues` 为空时，Worker 拉取 registry 里登记的队列：生成的 `RegisterActivities` 带上注解里的 `queue`，手写登记用 `activity.RegisterActivityOnQueue`；有不带队列的 Activity（`activity.RegisterActivity`、`reg.Register`）时再加上 `default`。显式设置 `ActivityTaskQueues` 时必须包含 registry 登记的每个队列，否则 `worker.New` 返回错误；可以额外列出别的队列，例如 Workflow 用 `ActivityOptions.TaskQueue` 改派的队列。

手写登记入口：

| 函数 | 用途 |
| --- | --- |
| `activity.RegisterActivityOnQueue(reg, queue, type, fn)` | 登记强类型实现及其 Task Queue；契约的输入输出指纹从 `fn` 的参数与返回类型算出 |
| `activity.RegisterActivity(reg, type, fn)` | 同上，不指定 Task Queue |
| `reg.Register(type, fn)` | 登记 `func(context.Context, map[string]any) (map[string]any, error)`；输入输出指纹取自 `map[string]any` |
| `reg.Contracts()` | 按类型名排序的全部契约 |
| `reg.Queues()` | 登记时指定的 Task Queue，去重排序 |
| `activity.ContractOf[I, O](type)` | 从 DTO 计算契约，用于与生成产物核对 |

同一个 registry 里每个 Activity Type 只能登记一次，重复登记返回错误。

### 契约对照

Workflow 侧声明了契约的 Activity（桥调用、指向注解函数的直接调用），Worker 领取任务时 Server 把 Run 当前 Program 声明的输入、输出与这个 Worker 的上报逐项对照：指纹相同即兼容；指纹不同时按数据流向比较 wire 结构：`input` 由 Workflow 写、Worker 读，`output` 由 Worker 写、Workflow 读，写方的每个字段读方都要有、类型相同或无损放宽（如 `int32` → `int64`、`T` → `*T`），读方多出的字段取零值。Worker 的上报由 `activity.ContractOf` 从 Go 类型生成，带指纹与 wire 结构；声明侧的结构取自 Program 的类型表。不兼容或 Worker 没有上报该项时拒绝这次领取：Attempt 不开始、不消耗重试次数，任务约 1s 后重新投递，Server 日志记 `activity claim refused`，原因是 `contract mismatch` 错误（写出 Activity Type、Worker identity、期望值与实际值，以及不兼容的字段路径）。Run 不暂停，契约兼容的 Worker 上线后自动领取。调度本身不对照契约：队列确定后照常写入 `ActivityScheduled` 并入队。Workflow 侧没有声明契约的 Activity 不对照。

契约是 `type`、输入指纹、输出指纹三项。指纹只描述结构：结构不变、结果的含义变了时，新含义用一个新的 `type`，旧 `type` 的实现保留到没有 Run 再调用它，见 [需要版本保护的变化](/develop/versioning#需要版本保护的变化)。

## 读取执行上下文

```go
func Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
    info := activity.GetInfo(ctx)
    log.Printf(
        "workflow=%s run=%s activity=%s type=%s attempt=%d",
        info.WorkflowID, info.RunID, info.ActivityID, info.ActivityType, info.Attempt,
    )
    return ReserveOutput{}, nil
}
```

`activity.GetInfo(ctx)` 返回本次 Attempt 的 `activity.Info`，字段含义与 Temporal 相同；`ctx` 不来自 Worker 时返回零值：

| 字段 | 含义 |
| --- | --- |
| `Namespace`、`WorkflowID`、`WorkflowType`、`RunID` | 调度这个 Activity 的 Workflow Run |
| `ActivityID` | Run 里被调度的这一个 Activity，各次重试相同 |
| `ActivityType`、`TaskQueue` | Activity 类型与它所在的 Task Queue |
| `Attempt` | 从 1 开始，每次重试加 1 |
| `ScheduledTime` | Activity 首次被调度的时间 |
| `StartedTime` | 本次 Attempt 开始的时间（Server 记录） |
| `Deadline` | 本次 Attempt 必须结束的时间：`StartToCloseTimeout` 与 `ScheduleToCloseTimeout` 中先到的一个；都未设置时为零值 |
| `HeartbeatTimeout` | 两次心跳之间的最长间隔 |
| `TaskToken` | 异步完成时交给外部系统的凭据，见 [异步 Activity 完成](/develop/async-activity-completion) |

Activity 作者用到的函数都在 `sdk/activity` 包：`GetInfo`、`RecordHeartbeat`、`GetLastHeartbeatDetails`、`GetWorkerStopChannel`、`ErrPending`、`ErrWorkerStopping`。

## 返回错误

Activity 返回普通 `error` 时，失败只带 message。需要类型、不可重试标记或结构化上下文时返回应用错误：

```go
import "github.com/donutnomad/tenuto/sdk/apperror"

// 永久失败，带结构化 detail
return ChargeOutput{}, apperror.NewNonRetryable(
    "card declined", "PaymentDeclined", nil,
    map[string]any{"reason": "insufficient_funds"},
)

// 可重试的临时失败
return ChargeOutput{}, apperror.New("payment provider unavailable", "PaymentUnavailable")
```

Workflow 侧用 `workflow.IsApplicationError(err, "PaymentDeclined")` 分支，见 [ApplicationError](/develop/application-error)。

## Heartbeat 与取消

```go
import "github.com/donutnomad/tenuto/sdk/activity"

func Export(ctx context.Context, in ExportInput) (ExportOutput, error) {
    offset := int64(0)
    if last := activity.GetLastHeartbeatDetails(ctx); last != nil {
        // 上一次 Attempt 记录的进度
    }
    for offset < in.Total {
        if err := ctx.Err(); err != nil {
            return ExportOutput{}, err
        }
        offset += exportBatch(ctx, offset)
        activity.RecordHeartbeat(ctx, map[string]any{"offset": offset})
    }
    return ExportOutput{Count: offset}, nil
}
```

Workflow 取消 Activity 所在的 Context（Run 被取消时的根 Context，或 `workflow.WithCancel` 的作用域）后，Server 在 Heartbeat 响应里返回取消请求，Worker 取消 Activity 的 `ctx`。长任务通过定期 Heartbeat 获得取消信号，在 `ctx.Done()` 后释放资源，返回 `ctx.Err()`（或 `apperror.NewCanceled(details...)`）确认取消，History 记 `ActivityCancelled`。配置了 `StartToCloseTimeout` 时，`ctx` 同时带有该超时的 deadline。

Heartbeat 只更新 Activity 的当前状态，不写 History；Attempt 超时时最后一次详情随 `ActivityTimedOut` 写入 History（`lastHeartbeatDetails`）。

取消只经 Heartbeat 送达：**Activity 要可取消就必须 Heartbeat**。不调用 `RecordHeartbeat`（或没有配置 `HeartbeatTimeout`）的 Activity 收不到取消，只在自己返回或超时时结束。取消请求之后返回成功的结果仍记为完成，返回其他错误记为失败且不再重试。完整规则见 [取消、超时与终止](/develop/cancellation-timeouts#操作被取消时的结果)。

## Worker 停止

Worker 停止时先关闭 `activity.GetWorkerStopChannel(ctx)` 返回的 channel，在宽限期（`worker.Options.GracefulStopTimeout`，缺省 10 秒）内不取消 `ctx`。长任务监听这个 channel，记一次 Heartbeat 保存进度后返回 `activity.ErrWorkerStopping`：Worker 不上报失败，Server 以同一个 Attempt 把它重新派发给其他 Worker，新的执行通过 `GetLastHeartbeatDetails` 接着做。宽限期到期仍在运行的 Activity 的 `ctx` 被取消，按同样的方式交还。详见[运行 Worker](/develop/workers#优雅退出)。

## 幂等

Tenuto 保证 Activity 至少执行一次，不保证只执行一次：Attempt 失败或超时后重试、Worker 停止时交还、结果上报丢失、人工 Reset 都会让同一次操作再执行。每个 Activity 都必须幂等。幂等键是操作名加这一次操作的身份，身份优先取输入里的业务 ID，其次是 Workflow 里用 `workflow.RandomUUID` 生成后随输入传入的 ID，再次是 `activity.GetInfo` 的 `WorkflowID` + `ActivityID`。`Attempt`、时间戳和 Activity 里生成的随机值不能用来构造幂等键。三种来源的写法与稳定范围见 [Activity 幂等](/best-practices/activity-idempotency)。

## 异步完成

Activity 可以返回 `activity.ErrPending` 并由外部系统在未来提交结果，见 [异步 Activity 完成](/develop/async-activity-completion)。
