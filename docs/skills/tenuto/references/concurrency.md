# 并发：Go、WaitGroup、Future、Channel、Selector

Workflow 并发由确定性的协作式调度器执行：任一时刻只有一条协程在运行，协程在等待点让出，被事件推进后恢复。共享变量直接读写，不需要锁。代码位置：`sdk/workflow/workflow.go`、`internal/executor/interpreter/host_concurrency.go`、`internal/executor/coroutine/`。

每条协程有稳定路径：主协程为空，`workflow.Go` / `wg.Go` 的第 i 条为 `/i`，嵌套为 `/i/j`，Update Handler 为 `update:<UpdateID>`。协程内 Activity（没有指定 `ActivityID` 时）、Timer、Child、随机值的 ID 由「路径 + 同类原语序号」生成（`#a1`、`/0#a1`、`/1/0#c1`），与兄弟协程推进快慢无关。因此：

- 协程的启动顺序与数量在所有重放中一致；循环启动协程时循环的数据源是顺序稳定的 slice。
- 同一协程内同类原语的调用顺序一致。
- 协程内使用闭包参数 `ctx`。

## 选型

| 场景 | 选择 |
| --- | --- |
| 每条分支只有一个 Activity | Future fan-out：连续 `workflow.ExecuteActivity`，再逐个 `Get` |
| 每条分支是多步顺序链 | `wg.Go` / `workflow.Go` |
| 等待多个事件中最先发生的一个 | Selector |
| 等待其他协程或 Handler 修改的状态 | `workflow.Await` |

## workflow.Go 与 WaitGroup

`workflow.Go` 启动协程后立即返回，不等待、不收集返回值和错误；协程里的运行时错误或非确定性错误让当前 Workflow Task 失败。等待用 `WaitGroup`，结果与错误写入外层变量：

```go
wg := workflow.NewWaitGroup(ctx)

reserveFailed := false
var reserved ReserveInventoryOutput
wg.Go(ctx, func(ctx workflow.Context) {
    out, err := CallReserveInventory(ctx, ReserveInventoryInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
    if err != nil {
        reserveFailed = true
    } else {
        reserved = out
    }
})

quoteFailed := false
var quote QuoteOutput
wg.Go(ctx, func(ctx workflow.Context) {
    out, err := CallQuoteShipping(ctx, QuoteInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
    if err != nil {
        quoteFailed = true
    } else {
        quote = out
    }
})

wg.Wait(ctx)
if reserveFailed || quoteFailed {
    return OrderOutput{}, workflow.NewApplicationError("prepare failed", "PrepareFailed")
}
```

循环 fan-out，每条协程内是有依赖的顺序链：

```go
wg := workflow.NewWaitGroup(ctx)
for _, order := range in.Orders {
    current := order // 先复制循环变量
    wg.Go(ctx, func(ctx workflow.Context) {
        reserved, err := CallReserve(ctx, ReserveInput{OrderID: current.ID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
        if err != nil {
            return
        }
        if _, err := CallSign(ctx, SignInput{ReservationID: reserved.ReservationID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil {
            return
        }
    })
}
wg.Wait(ctx)
```

| API | 行为 |
| --- | --- |
| `workflow.Go(ctx, f func(ctx workflow.Context))` | 启动协程，立即返回 |
| `workflow.NewWaitGroup(ctx) workflow.WaitGroup` | 创建 WaitGroup |
| `wg.Go(ctx, f)` | 计数加 1 并启动协程，协程结束自动 `Done` |
| `wg.Add(ctx, delta)` | 计数加 `delta`；计数为负时 panic |
| `wg.Done(ctx)` | 计数减 1 |
| `wg.Wait(ctx)` | 阻塞到计数归零 |

规则：`f` 写成内联闭包（传包级函数名被拒绝）；协程内可以调用桥、`ExecuteActivity`、`Sleep`、`WaitSignal`、`ExecuteChildWorkflow`，也可以再启动协程。

## Activity Future

```go
futures := []workflow.ActivityFuture{}
for _, item := range in.Items {
    futures = append(futures, workflow.ExecuteActivity(ctx, activities.Reserve, activities.ReserveInput{SKU: item.SKU}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}))
}
for _, future := range futures {
    var out activities.ReserveOutput
    if err := future.Get(ctx, &out); err != nil {
        return Output{}, err
    }
}
```

连续创建的 Future 在同一个 Workflow Task 里并发调度。

## Channel

```go
ch := workflow.NewBufferedChannel(ctx, 2)

workflow.Go(ctx, func(ctx workflow.Context) {
    var o map[string]any
    if e := workflow.ExecuteActivity(ctx, activities.B, input, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}).Get(ctx, &o); e == nil {
        ch.Send(ctx, o)
    }
    ch.Close(ctx)
})

var got map[string]any
more := ch.Receive(ctx, &got)
```

| API | 行为 |
| --- | --- |
| `workflow.NewChannel(ctx)` | 无缓冲 |
| `workflow.NewBufferedChannel(ctx, size)` | 容量 `size` |
| `ch.Send(ctx, v)` | 缓冲已满或没有接收方时阻塞 |
| `ch.Receive(ctx, &out) bool` | 返回 `more`；已关闭且取空时为 `false` |
| `ch.Close(ctx)` | 关闭并唤醒所有等待的接收方 |

- `Receive` 按目标类型接收：目标与发送值同类型（`string`、`int`、struct、slice、map）时得到这个值；目标是 `map[string]any` 时 struct 换成 map、标量包成 `{"value": v}`；目标是接口类型（`var v any`）生成期报错。
- 向已关闭的 Channel 发送、发送阻塞期间 Channel 被关闭，会 panic 并使当前 Workflow Task 失败。
- Channel 的值只在内存中传递，不写入 History。

## Selector

`Select` 等待多个 case 中最先就绪的一个并执行它的回调：

```go
which := ""
f := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
ch := workflow.NewBufferedChannel(ctx, 1)

sel := workflow.NewSelector(ctx)
sel.AddFuture(ctx, f, func(fut workflow.ActivityFuture) {
    which = "future"
})
sel.AddReceive(ctx, ch, func(c workflow.Channel, more bool) {
    var got map[string]any
    c.Receive(ctx, &got)
    which = "channel"
})
sel.Select(ctx)
```

Signal 与 Child Workflow：

```go
sf := workflow.WaitSignal(ctx, "approve")
cf := workflow.ExecuteChildWorkflow(ctx, "child-wf", input)

sel := workflow.NewSelector(ctx)
sel.AddSignalFuture(ctx, sf, func(f workflow.SignalFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
})
sel.AddChildFuture(ctx, cf, func(f workflow.ChildWorkflowFuture) {
    var out map[string]any
    _ = f.Get(ctx, &out)
})
sel.Select(ctx)
```

| 方法 | 就绪条件 | 回调签名 |
| --- | --- | --- |
| `AddFuture(ctx, f, cb)` | Activity 完成或失败 | `func(f workflow.ActivityFuture)` |
| `AddSignalFuture(ctx, f, cb)` | 此刻有未消费的同名 Signal（每次 `Select` 重新判定）；选中时 `f` 取走它，没被选中不占；同名 Signal 被别的 Future 取走后继续等待 | `func(f workflow.SignalFuture)` |
| `AddChildFuture(ctx, f, cb)` | Child Workflow 完成或失败 | `func(f workflow.ChildWorkflowFuture)` |
| `AddReceive(ctx, ch, cb)` | Channel 有值可收或已关闭 | `func(ch workflow.Channel, more bool)` |
| `AddSend(ctx, ch, v, cb)` | Channel 接受了值 | `func()` |
| `AddDefault(ctx, cb)` | 没有任何 case 就绪 | `func()` |

规则：

- 一次 `Select` 只执行一个 case；多个同时就绪时按添加顺序取第一个。处理 N 个事件调用 N 次 `Select`。
- 有 default 且无 case 就绪时立即执行 default 并返回；没有 default 时阻塞。
- 在回调里对 Future 调 `Get` 取结果或错误；`AddReceive` 的回调里调 `ch.Receive` 取值。
- `AddXxx` 返回 Selector，可以链式调用。
- case 的添加顺序属于确定性契约，修改它需要版本保护。
- `Select`、Signal 与 Channel 不受 Context 取消影响。要同时等待取消，加一个 `sel.AddReceive(ctx, ctx.Done(), func(ch workflow.Channel, more bool) { canceled = !more })` case，命中后返回 `ctx.Err()`。

## Await

```go
done := 0
for i := 0; i < 3; i++ {
    workflow.Go(ctx, func(ctx workflow.Context) {
        _ = workflow.Sleep(ctx, time.Second)
        done = done + 1
    })
}
workflow.Await(ctx, func() bool { return done >= 2 })
```

`workflow.Await(ctx, cond func() bool) error` 阻塞当前协程直到 `cond()` 为 `true`，返回 nil；条件不满足而 ctx 已取消时返回取消错误（Temporal 语义）。条件闭包只读 Workflow 内存变量，每次协程被恢复时重新求值；条件需要由后续的 Signal、Timer、Activity 结果或 Update 推进。

## Run 的结束与未完成的协程

主函数返回即结束 Run（`internal/executor/interpreter/executor.go` 的 `emitConvergedCommand`）：返回 nil 进入 `completed`，返回 `error` 进入 `failed`。此时仍在等待的 `workflow.Go` 协程、未完成的 Activity Future、Timer、执行中的 Update Handler 被放弃，结果不再被读取。

- 需要协程的结果：返回前 `wg.Wait`、`Get` 或 `Receive`。
- 「等待两者之一」之后（如 Signal 与超时 Timer 竞争）主函数可以直接返回，另一条协程随 Run 结束被放弃。
- 仍在运行的 Child Workflow 按父关闭策略处理（默认终止，见 `child-and-continue-as-new.md`）。
- 执行中的 Update 以 `AcceptedUpdateCompletedWorkflow` 失败结束，调用端的等待立即返回。

## 调试

内置 Query `__stack_trace` 由 Server 直接回答，Workflow 不需要注册 Handler；返回 Run 的逻辑进度：`steps`（History 中每个 Activity 的 `activity_id`、`activity_type`、状态）、`waiting`（当前等待点的类型、`activity_id`、`child_id`、`activity_type`、`timer_id`）和 `summary`，粒度为 Activity，不区分协程。Console 的 Trace 调试台按 RoutineID 区分同一源码行的多个并发实例。
