# Activity

Activity 是执行外部副作用的边界：原生 Go 函数，使用标准 `context.Context`，可以访问数据库、HTTP、MQ、文件系统、系统时间和任意 Go 库。代码位置：`sdk/activity/`、`sdk/workflow/workflow.go`（`ActivityOptions`、`RetryPolicy`）、`internal/executor/activity_runner.go`。

## 定义

```go
type ReserveInput struct {
    OrderID string `json:"order_id"`
}

type ReserveOutput struct {
    ReservationID string `json:"reservation_id"`
}

// @Activity(type="lifecycle.reserve",queue="lifecycle",version=v1)
func Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
    return ReserveOutput{ReservationID: "reservation-" + in.OrderID}, nil
}
```

- 签名固定 `func(context.Context, In) (Out, error)`。
- `In` / `Out` 是 struct 或 `map[string]any`。`string`、`int`、切片、指针在生成期报错，手写注册时 `activity.RegisterActivity` 报 `activity "<type>": input type string is not a JSON object; use a struct or map[string]any (wrap a single value in a struct field)`。
- 字段名取 `json` tag。DTO 支持的字段类型见 `codegen.md` 的「契约与 wire 指纹」。
- `@Workflow`、`@Activity` 与 DTO 可以放在同一个包的多个文件里（示例：`docs/examples/saga-workflow/`）。

## 从 Workflow 调度

桥调用（推荐）：

```go
reserved, err := activities.CallReserve(ctx, activities.ReserveInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
    return Output{}, err
}
```

带选项（来源：`docs/examples/saga-workflow/workflow.go`）：

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

Future（并发调度）：

```go
// func ExecuteActivity(ctx Context, activity any, input any, opts ...ActivityOptions) ActivityFuture
f1 := workflow.ExecuteActivity(ctx, activities.Reserve, activities.ReserveInput{OrderID: a}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
f2 := workflow.ExecuteActivity(ctx, activities.Reserve, activities.ReserveInput{OrderID: b}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})

var r1 activities.ReserveOutput
if err := f1.Get(ctx, &r1); err != nil {
    return Output{}, err
}
var r2 activities.ReserveOutput
if err := f2.Get(ctx, &r2); err != nil {
    return Output{}, err
}
```

`ExecuteActivity` 在调用时立即产生调度命令，`Get` 只等待结果。Future 可以不 `Get`（只调度）、放进切片逐个 `Get`、在 `for` 循环里创建。

`ExecuteActivity` 的第二个参数：

| 指向 | Activity Type | Task Queue | 契约 | 输入 |
| --- | --- | --- | --- | --- |
| 带 `@Activity` 的函数，或 remote 标记函数 `XRemoteActivity` | 注解的 `type` | 注解的 `queue` | 注解声明，调度时对照 | struct 或 `map[string]any` |
| 没有注解的函数 | 符号文本 `pkg.Func`（如 `activities.CheckBalance`） | 这次调用的 `ActivityOptions.TaskQueue`，省略时为 `default` | 无声明，不对照 | struct 或 `map[string]any` |

没有注解的实现用符号文本登记：`activity.RegisterActivity(registry, "activities.CheckBalance", "", CheckBalance)`。新代码一律加 `@Activity`。

## ActivityOptions

```go
type ActivityOptions struct {
    ActivityID             string        // 这次调用在 Run 内的业务 ID；为空时取调用位置（#a1、/0#a2）
    TaskQueue              string        // 覆盖本次调用的 Task Queue
    ScheduleToStartTimeout time.Duration // 每个 Attempt 从可派发到 Worker 开始执行；到期后 Call 直接失败，不重试
    StartToCloseTimeout    time.Duration // 单个 Attempt
    ScheduleToCloseTimeout time.Duration // 整个 Call，含排队与重试
    HeartbeatTimeout       time.Duration // 两次 Heartbeat 的最长间隔
    RetryPolicy            RetryPolicy   // 零值使用缺省策略
}

type RetryPolicy struct {
    MaxAttempts            int           // 含首次执行；0 = 不限次数，1 = 不重试
    InitialInterval        time.Duration // 缺省 1s
    BackoffCoefficient     float64       // 缺省 2.0
    MaxInterval            time.Duration // 缺省 100 × InitialInterval
    NonRetryableErrors     []string      // 失败 message 含任一子串即停止
    NonRetryableErrorTypes []string      // 失败 Type 完全相等（大小写敏感）即停止
}
```

- 超时与间隔是 `time.Duration`（`500 * time.Millisecond`、`30 * time.Second`、`5 * time.Minute`）；零值表示不设置。
- `ActivityID` 对标 Temporal：写进 `ActivityScheduled`，Activity 里是 `activity.GetInfo(ctx).ActivityID`，`reset-activity` 与异步完成按它寻址。可以运行期计算（`fmt.Sprintf("charge-%d", i)`），值必须确定，重放时变了是 `non_deterministic_error`。给某个调用指定 ID 不改变其他调用的缺省 ID。与同一 Run 里仍未关闭的 Activity 重复时 Workflow Task 以 `schedule_activity_duplicate_id` 失败、按退避重试；前一个关闭后同一 ID 可以再用。不能为空，最长 191 个字符。
- 选项只作用于这一次调用：另一处不带选项的调用走 Activity 的缺省路由（`@Activity` 的 `queue`，没有注解时为 `default`）。
- `StartToCloseTimeout` 与 `ScheduleToCloseTimeout` 至少设置一个。生成器拒绝两者都缺少的调用点（`... 没有设置 StartToCloseTimeout 或 ScheduleToCloseTimeout`）；选项来自变量时 Server 拒绝调度命令，Workflow Task 以 `bad_schedule_activity_attributes` 失败、按退避重试。
- Server 记录 Attempt started 后 Worker 立即确认队列租约：已开始的 Attempt 不会因队列重投再执行（重投的副本被丢弃），Worker 消失后由 StartToClose 或 Heartbeat 超时重试；Worker 优雅停止时交还的 Attempt 立即以同一序号重新派发。
- **Activity 默认重试。** 缺省策略与 Temporal 相同，由 Server 补齐：零值 `RetryPolicy` 不限次数重试，部分设置的策略其余字段取缺省值；`ActivityScheduled` 事件记录补齐后的生效策略。只执行一次写 `RetryPolicy{MaxAttempts: 1}`。没有 `ScheduleToCloseTimeout` 也没有 `MaxAttempts` 的 Call 一直重试到成功或遇到不可重试的错误。
- 退避：`interval(n) = min(InitialInterval × BackoffCoefficient^(n-1), MaxInterval)`。
- `ScheduleToCloseTimeout` 到期，或下一次 Attempt 会落在它之后时，Call 直接失败（`retryState` 为 `Timeout`）；`ScheduleToStartTimeout` 到期同样直接失败；`StartToCloseTimeout` 与 `HeartbeatTimeout` 按 RetryPolicy 决定下一次 Attempt。
- 配置了 `StartToCloseTimeout` 时，Activity 的 `ctx` 带同样的 deadline。
- Call 因超时最终失败时，`workflow.TimeoutTypeOf(err)` 返回超时种类（`workflow.TimeoutTypeStartToClose` 等），其他错误返回空串。

停止重试的判定顺序（只看顶层失败；`fmt.Errorf("%w")` 包装后顶层 Type 与标记属于包装层）：

1. 失败带 NonRetryable 标记（`apperror.NewNonRetryable`）。
2. 失败 Type 命中 `NonRetryableErrorTypes`。
3. 失败 message 命中 `NonRetryableErrors`。
4. 已达到 `MaxAttempts`。
5. `ScheduleToCloseTimeout` 已到期或下一次 Attempt 会落在它之后。

Activity 函数 panic 时 Worker 捕获并上报失败：Type `PanicError`，message `activity panicked: <值>`。它按 RetryPolicy 重试；把 `PanicError` 加入 `NonRetryableErrorTypes` 让 panic 直接成为最终失败。

## 注册

```go
registry := activity.NewRegistry()
if err := sagaflow.RegisterActivities(registry); err != nil { // 生成的批量登记，一个参数
    return err
}
// remote 契约的实现由拥有它的进程手写登记，type 与 version 和注解逐字一致
if err := activity.RegisterActivity(registry, "payments.settle", "v1", Settle); err != nil {
    return err
}
```

| 函数 | 用途 |
| --- | --- |
| `activity.NewRegistry() *Registry` | 一个 Worker 进程一个注册表 |
| `<pkg>.RegisterActivities(registry) error` | 生成：登记本包全部本地 `@Activity` 实现与契约 |
| `activity.RegisterActivity[I, O](reg, type, version, fn) error` | 登记强类型实现；指纹从 `I`、`O` 算出 |
| `reg.Register(type, fn) error` | 登记 `func(context.Context, map[string]any) (map[string]any, error)`；版本为空 |
| `reg.Contracts()`、`reg.Types()` | 按类型名排序的契约 / 类型 |
| `activity.ContractOf[I, O](type, version)` | 从 DTO 计算契约 |

同一个注册表里每个 `type` 只能登记一次（`activity type "..." is already registered`）。Worker 在 `Start` 时把全部契约随路由上报。

## 执行上下文、心跳与取消

`sdk/activity` 提供 Activity 作者用到的全部函数：

```go
func Export(ctx context.Context, in ExportInput) (ExportOutput, error) {
    info := activity.GetInfo(ctx)
    log.Printf("workflow=%s activity=%s attempt=%d", info.WorkflowID, info.ActivityID, info.Attempt)
    offset := int64(0)
    if last := activity.GetLastHeartbeatDetails(ctx); last != nil {
        // 上一次 Attempt 最后记录的进度；首次 Attempt 为 nil
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

| API | 说明 |
| --- | --- |
| `activity.GetInfo(ctx) activity.Info` | 本次 Attempt 的信息，字段含义同 Temporal：`Namespace`、`WorkflowID`、`WorkflowType`、`RunID`、`ActivityID`（同一个 Activity 的各次重试相同）、`ActivityType`、`TaskQueue`、`Attempt`（从 1 起）、`ScheduledTime`（首次调度）、`StartedTime`（本次开始）、`Deadline`（StartToClose 与 ScheduleToClose 中先到的）、`HeartbeatTimeout`、`TaskToken`；ctx 不来自 Worker 时为零值 |
| `activity.RecordHeartbeat(ctx, details map[string]any)` | 上报进度；Workflow 请求取消这个 Activity 后，响应让 Worker 取消 `ctx` |
| `activity.GetLastHeartbeatDetails(ctx) map[string]any` | 最后记录的进度：重试的 Attempt 读上一次 Attempt 的，Worker 停止后重新派发的 Attempt 读交还前的 |
| `activity.GetWorkerStopChannel(ctx) <-chan struct{}` | Worker 开始停止时关闭；ctx 不来自 Worker 时为 nil |
| `activity.ErrPending` | 返回它表示结果由外部异步提交 |
| `activity.ErrWorkerStopping` | 察觉 Worker 停止后返回它：Attempt 交还 Server 重新派发，不算失败 |

Heartbeat 不写 History（与 Temporal 相同）：Server 只更新 Activity 状态的 `lastHeartbeat` / `lastHeartbeatAt`，Heartbeat 超时从 `lastHeartbeatAt` 起算。运行中的进度在 `activity-states`（Console Pending 页签）查看；Attempt 超时时最后一次详情写进 `ActivityTimedOut.lastHeartbeatDetails`。

Workflow 取消 Activity 所在的 Context（Run 被取消时的根 Context，或 `workflow.WithCancel` 的作用域）后，正在执行的 Activity 在下一次 Heartbeat 时得到取消信号；Run 进入任一终态、这次 Attempt 已超时或被 Reset 取代时同样如此。**Activity 要可取消就必须 Heartbeat**：取消只经 `RecordHeartbeat` 的响应送达，没有其他通道；不 Heartbeat（或没有 `HeartbeatTimeout`）的 Activity 只在自己返回或超时时结束。收到 Workflow 的取消后返回 `ctx.Err()` 或 `apperror.NewCanceled(details...)` 确认取消（`ActivityCancelled`）；返回成功仍记为完成；返回其他错误记为失败且不再重试。长任务定期 Heartbeat（间隔小于 `HeartbeatTimeout`，间隔也决定取消延迟），在 `ctx.Done()` 后释放资源。规则表见 `cancellation-timeouts.md`。

Worker 停止（`Stop`）时先关闭停止 channel，宽限期（`worker.Options.GracefulStopTimeout`，缺省 10s）内 `ctx` 不取消。长任务监听它，记一次 Heartbeat 保存进度后返回 `activity.ErrWorkerStopping`：

```go
select {
case <-activity.GetWorkerStopChannel(ctx):
    activity.RecordHeartbeat(ctx, map[string]any{"offset": offset})
    return ExportOutput{}, activity.ErrWorkerStopping
case <-ctx.Done():
    return ExportOutput{}, ctx.Err()
default:
}
```

Worker 不上报失败，把 Attempt 交还 Server；Server 以同一个 Attempt 序号立即重新派发，不消耗重试次数，新的执行用 `GetLastHeartbeatDetails` 接着做。宽限期到期仍在运行的 Activity 的 `ctx` 被取消，同样交还。在 Worker 停止之外返回 `ErrWorkerStopping` 按普通失败处理。

## 异步完成

Activity 把 Token 交给外部系统后返回 `activity.ErrPending`，Worker 释放执行槽并保留 Activity 的未完成状态：

```go
func WaitForPayment(ctx context.Context, in PaymentInput) (PaymentResult, error) {
    token := activity.GetInfo(ctx).TaskToken
    if err := callbackStore.Save(in.PaymentID, token); err != nil {
        return PaymentResult{}, err
    }
    return PaymentResult{}, activity.ErrPending
}
```

外部完成有两个入口，Token 相同：

```go
// Server 进程内：(*engine.Server).CompleteActivity，包 server/biz/service/engine
err := tenutoServer.CompleteActivity(ctx, engine.ActivityTaskToken(savedToken),
    map[string]any{"paid": true}, nil)                                       // 成功
err = tenutoServer.CompleteActivity(ctx, engine.ActivityTaskToken(savedToken),
    nil, &protocol.Failure{Message: "payment expired", Type: "PaymentExpired"}) // 失败
```

```bash
# Activity Runtime HTTP 端点（server/http/runtimehttp/api_activity.go 的 Complete）
curl --fail-with-body -sS "$TENUTO_URL/runtime/activity/complete" \
  -H 'Content-Type: application/json' \
  -d '{"task_token":"<token>","output":{"paid":true}}'
# 失败：{"task_token":"<token>","failure":{"message":"payment expired","type":"PaymentExpired"}}
```

- `failure` 为空表示成功，非空表示失败。
- Token 是凭证：绑定 Run、Scheduled Event、Attempt 和 Stamp；加密存储。Activity Reset 后旧 Token 的完成请求按 stale 丢弃。
- 外部回调入口自己负责鉴权、签名验证和幂等；Runtime HTTP 端点只暴露在受信网络。
- 给 pending Activity 配 `ScheduleToCloseTimeout`，外部系统失联时由超时推进恢复流程。

## 幂等

Tenuto 保证 Activity 至少执行一次，不保证只执行一次：Attempt 重试、Worker 交还、结果上报丢失、人工 Reset 都会让同一次操作再执行，取消中的 Activity 也可能已完成一次外部调用。**每个 Activity 都必须幂等**，这是基本要求。

幂等键 = 操作名 + 这一次操作的身份。只按对象取键（`charge:<OrderID>`）粒度过粗，同一订单的第二次同类操作会被当成重复。身份的三种来源：

| 来源 | 写法 | 何时用 |
| --- | --- | --- |
| 1 输入里的业务 ID（首选） | `"charge-installment:" + in.OrderID + ":" + strconv.Itoa(in.InstallmentNo)` | 业务有订单号、期数、退款单号、请求 ID；资金类不能重做的操作只用它 |
| 2 Workflow 生成后随输入传入 | Workflow：`RefundInput{RefundID: workflow.RandomUUID(ctx), ...}`；Activity：`"issue-refund:" + in.RefundID` | 业务没有天然 ID；希望 Reset 越过生成位置后真正重做 |
| 3 `activity.GetInfo` | `"send-receipt:" + info.WorkflowID + ":" + info.ActivityID` | 不改输入；Workflow 不 continue-as-new、不复用 Workflow ID |

没有指定时 `ActivityID` 按调用位置编号、每个 Run 从头编号（指定时是代码给出的值，同样每个 Run 一套）：Activity 重试、Workflow Reset、Workflow 重试后不变（下游把重新执行当成重复）；continue-as-new 的下一代与复用 Workflow ID 的新 Workflow 里，同一位置的另一次操作得到相同的键，所以这两类 Workflow 不能用第 3 种。`RandomUUID` 记在 History 里，重放与重试不变；Reset 回退点越过生成位置、Workflow 重试、continue-as-new 后是新值。

不能进幂等键：`Attempt`、时间戳、Activity 里生成的随机值、单独的 `RunID`。完整示例：`docs/examples/idempotency/example_test.go`。

## 排查

| 现象 | 检查 |
| --- | --- |
| 长时间 Scheduled | `ActivityScheduled` 的 Namespace、Task Queue、Type；Worker 是否在线、是否消费该队列；Server 日志是否反复出现 `activity claim refused: worker contract is incompatible`（在线 build 的契约都与 Run 当前 Program 不一致） |
| 多个 Attempt | 同一 Call 的重试：看 `ActivityAttemptFailed`、`ActivityTimedOut`、退避与 NonRetryable 配置 |
| Heartbeat 超时 | 心跳间隔是否小于 `HeartbeatTimeout`；下游长阻塞 |
| 取消延迟 | History 是否有 `ActivityCancelRequested`（Workflow 取消请求本身不取消 Activity）；Activity 是否定期 Heartbeat 并检查 `ctx`；不 Heartbeat 的 Activity 收不到取消 |
| 重复副作用 | 幂等键是否跨 Attempt 稳定 |
| Run 暂停、`contract mismatch` | 见 `versioning.md` |
