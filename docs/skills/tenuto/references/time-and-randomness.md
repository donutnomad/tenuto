# 时间、Timer、随机值与 Side Effect

Workflow 重放时每个值都要与首次执行相同。时间取自 Event 时间戳；随机值与 Side Effect 的结果写入 Marker，重放时读回。代码位置：`sdk/workflow/workflow.go`、`internal/executor/interpreter/host_api.go`。

| 需求 | API |
| --- | --- |
| 当前时间 | `workflow.Now(ctx)` |
| 持久化等待 | `workflow.Sleep(ctx, d)` |
| 随机 UUID、随机整数 | `workflow.RandomUUID(ctx)`、`workflow.RandomInt(ctx, max)` |
| 每个调用点固定一个由 Workflow 值算出的结果 | `workflow.SideEffect` |
| 同一 ID 的值随执行推进更新 | `workflow.MutableSideEffect` |
| 系统时间、外部读取、Go 库计算 | Activity |

## Now

```go
now := workflow.Now(ctx)             // time.Time
deadline := now.Add(2 * time.Hour)
if workflow.Now(ctx).After(deadline) { ... }
stamp := now.Format(time.RFC3339)    // 按 UTC 格式化
```

- 返回最近一条已应用 Event 的时间戳；同一个 Workflow Task 内多次调用得到相同的值。
- `time.Time` 值可用的方法：`Add`、`Sub`、`Before`、`After`、`Equal`、`Unix`、`UnixMilli`、`Format`、`String`；其他 `time.Time` 方法与 `time.Duration` 的方法生成期报错。
- `time.Now`、`time.Since`、`time.Until`、`time.Parse`、`time.Unix`、`time.Date` 在生成期被拒绝；解析与构造时间放进 Activity。

## Sleep

```go
// func Sleep(ctx Context, delay time.Duration) error
if err := workflow.Sleep(ctx, 30*time.Minute); err != nil {
    return err
}
```

- 写入 Timer 事件，Server 到期后推进 Run；Worker 重启、Server 重启都不影响到期。
- `delay <= 0` 立即返回，不产生 Timer。
- Timer 的 ID 是「协程路径 + `#tN`」；新增、删除、移动 `Sleep` 调用属于需要版本保护的修改（见 `versioning.md`）。

### 带超时的等待

一条协程 `Sleep`，一条协程等业务事件，主协程 `Await` 两者之一：

```go
approved := false
timedOut := false

workflow.Go(ctx, func(ctx workflow.Context) {
    var decision map[string]any
    _ = workflow.WaitSignal(ctx, "approve").Get(ctx, &decision)
    approved = true
})
workflow.Go(ctx, func(ctx workflow.Context) {
    _ = workflow.Sleep(ctx, 24*time.Hour)
    timedOut = true
})

workflow.Await(ctx, func() bool { return approved || timedOut })
if !approved {
    return Output{}, workflow.NewApplicationError("approval timed out", "ApprovalTimeout")
}
```

主函数返回即结束 Run，另一条仍在等待的协程被放弃。

### 周期循环

```go
for i := 0; i < 500; i++ {
    if _, err := CallPoll(ctx, PollInput{Cursor: cursor}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil {
        return err
    }
    if err := workflow.Sleep(ctx, time.Minute); err != nil {
        return err
    }
}
return workflow.ContinueAsNew(ctx, "poller", PollerState{Cursor: cursor})
```

循环写成有上限的计数循环（`for {}` 被生成器拒绝），到上限后 Continue-As-New。

## RandomUUID 与 RandomInt

```go
requestID := workflow.RandomUUID(ctx)   // 常用作下游幂等键，随 Activity 输入传出
shard := workflow.RandomInt(ctx, 16)    // [0, max)；max <= 0 时返回 0
```

首次执行生成并写入 Marker，重放读回同一个值。

## SideEffect

```go
var side map[string]any
err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
    return map[string]any{"region": "sg"}
}).Get(ctx, &side)
if err != nil {
    return err
}
```

- 闭包在 `SideEffect` 调用时执行一次，返回值写入 Marker；重放跳过闭包。`Get` 把 Marker 的值解码到 `out`（指向闭包返回类型的指针）。
- 闭包由解释器执行，可用符号与 Workflow 函数体相同：字面量、Workflow 变量、`workflow.*`。它不能调用 Go 库或读取外部状态，这类工作放进 Activity。
- 闭包参数写成内联闭包或保存闭包的局部变量。

## MutableSideEffect

```go
level := 1
f := func(ctx workflow.Context) any { return level }
eq := func(a, b any) bool { return a == b }

var out int
if err := workflow.MutableSideEffect(ctx, "risk-level", f, eq).Get(ctx, &out); err != nil {
    return err
}
level = 2 // 值变化后的下一次调用记录新 Marker
if err := workflow.MutableSideEffect(ctx, "risk-level", f, eq).Get(ctx, &out); err != nil {
    return err
}
```

- 同一 ID 首次调用执行 `f` 并记录 Marker；之后每次调用执行 `f`，`equals(新值, 旧值)` 为 `true` 时复用旧 Marker，为 `false` 时记录新 Marker。
- 重放按记录逐次复现，不调用 `f` 与 `equals`。
- ID、返回值结构、`equals` 语义属于持久化契约；改 ID 等于建立另一条 Marker 序列。

## Marker 的定位

| API | 定位方式 |
| --- | --- |
| `RandomUUID`、`RandomInt`、`SideEffect` | 协程路径 + 该协程内同类调用的序号 |
| `MutableSideEffect` | 显式 ID + 该 ID 的调用次数 |
| `GetVersion` | change ID |

在已有 Run 走过的位置之前新增、删除这些调用会让重放找不到对应 Marker，Workflow Task 以非确定性错误失败；用 `GetVersion` 保护。

## 数据约束

Marker 经过 JSON 持久化：超过 2^53 的整数与精度敏感的小数用字符串表示；闭包返回 struct 时字段按 `json` tag 写入，`Get` 解码回同类型。
