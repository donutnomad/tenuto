---
title: 并发、Channel 与 Selector
description: 使用 workflow.Go、WaitGroup、Future、Channel 和 Selector 编写 Go 风格的并发 Workflow
status: Stable
---

# 并发、Channel 与 Selector

<FeatureStatus status="Stable" />

Tenuto 用确定性的协作式调度器执行 Workflow 并发：任一时刻只有一条协程在运行，协程在等待时让出，被外部事件推进后恢复。每条协程有稳定的路径（主协程为空，`workflow.Go` 的第 i 条为 `/i`，嵌套为 `/i/j`），协程内的 Activity、Timer、Child Workflow 的 ID 从该路径派生，与兄弟协程的推进快慢无关。

| Go 原生写法 | Workflow 写法 |
| --- | --- |
| `go func() { ... }()` | `workflow.Go(ctx, func(ctx workflow.Context) { ... })` |
| `sync.WaitGroup` | `workflow.NewWaitGroup(ctx)` |
| `make(chan T)` / `make(chan T, n)` | `workflow.NewChannel(ctx)` / `workflow.NewBufferedChannel(ctx, n)` |
| `select` | `workflow.NewSelector(ctx)` |
| 等待条件 | `workflow.Await(ctx, cond)` |

原生 `go`、`chan`、`select` 与 `sync` 包在生成期报错，错误信息给出对应的 Workflow 写法，见 [Workflow 可用的 Go 语法](/develop/go-subset)。

## workflow.Go 与 WaitGroup

`workflow.Go` 启动一条协程后立即返回，不等待它、不收集它的返回值和错误。协程里出现运行时错误（如写 nil map、下标越界）或非确定性错误时，当前 Workflow Task 失败并按退避重试，Workflow 不会带着半截状态完成。需要等待时使用 `WaitGroup`：

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

| API | 行为 |
| --- | --- |
| `workflow.Go(ctx, f)` | 启动协程，立即返回 |
| `wg.Go(ctx, f)` | 计数加 1 并启动协程，协程结束时自动 `Done` |
| `wg.Add(ctx, delta)` | 计数加 `delta`，计数为负时 panic |
| `wg.Done(ctx)` | 计数减 1 |
| `wg.Wait(ctx)` | 阻塞到计数归零 |

规则：

- `f` 写成内联闭包 `func(ctx workflow.Context) { ... }`。
- 协程内使用闭包参数 `ctx`，它携带该协程的路径。
- 协程的结果和错误写入外层变量，`Wait` 之后由调用方检查。
- 循环里启动协程时先复制循环变量（`current := item`），并保证循环顺序稳定。
- 协程内可以调用桥 `CallX`、`workflow.ExecuteActivity`、`Sleep`、`WaitSignal`、`ExecuteChildWorkflow`，也可以再启动协程。

循环 fan-out：

```go
wg := workflow.NewWaitGroup(ctx)
for _, order := range in.Orders {
    current := order
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

每条协程内部是有依赖的顺序链，协程之间并行推进。

## Activity Future

`workflow.ExecuteActivity` 在调用时立即调度并返回 `workflow.ActivityFuture`，`Get` 只等待结果。连续创建多个 Future 可以在同一个 Workflow Task 里并发调度多个 Activity：

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

- Future 可以存进切片后逐个 `Get`，可以在 `for` 循环里创建，也可以不 `Get`（只调度、不等待结果）。
- 第二个参数指向带 `@Activity` 注解的函数时，Activity Type、Task Queue 与契约取自注解；指向没有注解的函数时 Activity Type 是符号文本（如 `activities.CheckBalance`）。规则见 [编写 Activity](/develop/activities) 的「直接使用 ExecuteActivity」。

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

| 场景 | 选择 |
| --- | --- |
| 每条分支只有一个 Activity | Future fan-out，或 `wg.Go` 里放一个桥调用 |
| 每条分支是多步顺序链 | `wg.Go` / `workflow.Go` |
| 等待多个事件中最先发生的一个 | Selector |

## Channel

Channel 在协程之间传递值：

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
| `workflow.NewChannel(ctx)` | 无缓冲 Channel |
| `workflow.NewBufferedChannel(ctx, size)` | 容量为 `size` 的缓冲 Channel |
| `ch.Send(ctx, v)` | 发送；缓冲已满或没有接收方时阻塞 |
| `ch.Receive(ctx, &out)` | 接收并返回 `more`；Channel 已关闭且取空时返回 `false` |
| `ch.Close(ctx)` | 关闭并唤醒所有等待的接收方 |

规则：

- `Receive` 的目标是 `*map[string]any`。发送 map 或结构体时接收方得到对应的 map；发送标量时接收方得到 `{"value": v}`。
- 向已关闭的 Channel 发送、发送阻塞期间 Channel 被关闭，都会 panic 并使当前 Workflow Task 失败。
- Channel 的值只在内存中传递，不写入 History；重放时由协程重新执行得到相同的传递顺序。

## Selector

Selector 等待多个 case 中最先就绪的一个并执行它的回调：

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

| 方法 | case 就绪条件 | 回调签名 |
| --- | --- | --- |
| `AddFuture(ctx, f, cb)` | Activity 完成或失败 | `func(f workflow.ActivityFuture)` |
| `AddSignalFuture(ctx, f, cb)` | `WaitSignal` 对应的 Signal 已到达 | `func(f workflow.SignalFuture)` |
| `AddChildFuture(ctx, f, cb)` | Child Workflow 完成或失败 | `func(f workflow.ChildWorkflowFuture)` |
| `AddReceive(ctx, ch, cb)` | Channel 有值可收或已关闭 | `func(ch workflow.Channel, more bool)` |
| `AddSend(ctx, ch, v, cb)` | Channel 接受了值 | `func()` |
| `AddDefault(ctx, cb)` | 没有任何 case 就绪 | `func()` |

规则：

- `Select` 每次只执行一个 case。多个 case 同时就绪时按添加顺序取第一个。
- 有 `AddDefault` 且没有 case 就绪时，`Select` 立即执行 default 并返回；没有 default 时阻塞到某个 case 就绪。
- `AddXxxFuture` 的第二个参数是对应原语返回的 Future 变量或其内联调用，种类与方法匹配。
- 在回调里对 Future 调用 `Get` 取得结果或错误；`AddReceive` 的回调里调用 `ch.Receive` 取出值。
- 方法返回 Selector，支持链式写法，也可以逐条调用。
- 处理 N 个事件需要调用 N 次 `Select`。

Signal 与 Child Workflow 的 case：

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

同时等待取消：`ctx.Done()` 返回的 Channel 在 Context 被取消时关闭，放进 `AddReceive`，回调的 `more` 为 `false`。Signal、Channel 与 `Select` 本身不受取消影响：

```go
canceled := false
sel := workflow.NewSelector(ctx)
sel.AddReceive(ctx, ctx.Done(), func(ch workflow.Channel, more bool) { canceled = !more })
sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "approve"), func(f workflow.SignalFuture) {})
sel.Select(ctx)
if canceled {
    return ctx.Err()
}
```

## Await

`workflow.Await(ctx, cond)` 阻塞当前协程直到 `cond()` 返回 `true`，用于等待其他协程或 Handler 修改的状态：

```go
done := 0
for i := 0; i < 3; i++ {
    workflow.Go(ctx, func(ctx workflow.Context) {
        _ = workflow.Sleep(ctx, 1000000000)
        done = done + 1
    })
}
if err := workflow.Await(ctx, func() bool { return done >= 2 }); err != nil {
    return err // ctx 已取消
}
```

`Await` 先求值条件，条件为 `true` 时返回 `nil`；条件为 `false` 而 ctx 已取消时返回取消错误，与 Temporal 相同。

## Run 的结束与未完成的协程

函数体返回即结束 Run：返回 nil 时 Run 进入 `completed`，返回取消错误时进入 `cancelled`，返回其他 `error` 时进入 `failed`。此时仍在等待的 `workflow.Go` 协程、未完成的 Activity Future、Timer 和正在执行的 Update Handler 被放弃，它们的结果不再被读取。

需要协程的结果时，在返回前用 `WaitGroup.Wait`、Future 的 `Get` 或 Channel 接收等它们结束。仍在运行的子 Workflow 按各自的[父关闭策略](/develop/child-workflows#父关闭策略)处理，默认被终止；正在执行的 Update 以 `AcceptedUpdateCompletedWorkflow` 失败结束，调用端的等待立即返回。

## 调试

Trace 调试台按 RoutineID 区分同一源码行的多个并发实例，变量面板显示当前闭包作用域中的变量。内置 Query `__stack_trace` 由 Server 直接回答，返回 Run 的逻辑进度：`steps`（History 中每个 Activity 的 `activity_id`、`activity_type`、状态）、`waiting`（当前等待点的类型、`activity_id`、`child_id`、`activity_type`、`timer_id`）和 `summary`；结果以 Activity 为粒度，不区分协程。
