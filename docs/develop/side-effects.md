---
title: Side Effect 与确定性值
description: 确定性时间、随机数、UUID、SideEffect 与 MutableSideEffect
status: Stable
---

# Side Effect 与确定性值

<FeatureStatus status="Stable" />

Workflow 重放时必须得到与首次执行相同的值。Tenuto 把时间取自 Event 时间戳，把随机值和 Side Effect 的结果写入 Marker，重放时直接读回。

## 确定性时间

```go
now := workflow.Now(ctx)
```

`workflow.Now` 返回最近一条已应用 Event 的时间戳。同一个 Workflow Task 内多次调用得到相同的值，时间随新 Event 推进。

## 随机 UUID

```go
id := workflow.RandomUUID(ctx)
```

首次执行生成 UUID 字符串并写入 `RandomUUID` Marker，重放读取同一个值。常见用途是为下游请求生成幂等键，再作为 Activity 输入传出。

## 随机整数

```go
shard := workflow.RandomInt(ctx, 16)
```

返回 `[0, max)` 范围内的整数，首次执行使用加密随机源并写入 `RandomInt` Marker。`max` 小于等于 0 时返回 0。

## SideEffect

`SideEffect` 在首次执行时运行闭包并把返回值写入 Marker，重放时跳过闭包直接读取：

```go
var side map[string]any
err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
    return map[string]any{"region": "sg"}
}).Get(ctx, &side)
if err != nil {
    return err
}
```

- 闭包在 `SideEffect` 调用时执行，`Get` 只把 Marker 里的值解码到 `out`。
- `Get` 的目标是指向闭包返回类型的指针，值经过 JSON 往返解码。
- 闭包与 Workflow 函数体一样由解释器执行，可用的符号范围相同：字面量、Workflow 变量和 `workflow.*` 调用。读取外部配置、调用 Go 库的工作放进 Activity。

组合随机原语生成一个固定的值：

```go
uuidValue := workflow.RandomUUID(ctx)
randomValue := workflow.RandomInt(ctx, 100)
err = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
    return map[string]any{"uuid": uuidValue, "n": randomValue}
}).Get(ctx, &side)
```

## MutableSideEffect

`MutableSideEffect` 用显式 ID 跟踪一个可能变化的值，只在 `equals` 判定值发生变化时记录新 Marker：

```go
level := 1
f := func(ctx workflow.Context) any { return level }
eq := func(a, b any) bool { return a == b }

var out int
if err := workflow.MutableSideEffect(ctx, "risk-level", f, eq).Get(ctx, &out); err != nil {
    return err
}
// level 不变时再次调用复用旧 Marker；level 改变后的调用记录新 Marker
level = 2
if err := workflow.MutableSideEffect(ctx, "risk-level", f, eq).Get(ctx, &out); err != nil {
    return err
}
```

`equals` 对标量值可以直接用 `==` 比较。

- 同一 ID 的首次调用执行 `f` 并记录 Marker。
- 之后每次调用执行 `f` 得到新值，调用 `equals(新值, 旧值)`；返回 `true` 时复用旧 Marker，返回 `false` 时记录新 Marker。
- 重放时按首次执行的记录逐次复现，不调用 `f` 和 `equals`。
- 新旧值都是 `nil` 视为相等，只有一侧为 `nil` 视为不等。

ID、返回值结构和 `equals` 语义属于持久化契约。

## Marker 的定位方式

| API | Marker 定位 |
| --- | --- |
| `RandomUUID`、`RandomInt`、`SideEffect` | 协程路径 + 该协程内同类调用的序号 |
| `MutableSideEffect` | 显式 ID + 该 ID 的调用次数 |
| `GetVersion` | change ID |

重放时某个调用点找不到对应 Marker，Worker 报告非确定性错误。在已有 Run 走过的位置之前新增 `SideEffect`、`RandomUUID` 或 `RandomInt` 调用属于需要 [版本保护](/develop/versioning) 的修改。

## 选型

| 需求 | 使用方式 |
| --- | --- |
| 当前时间 | `workflow.Now` |
| 随机 UUID 或整数 | `RandomUUID`、`RandomInt` |
| 每个调用点固定一个由 Workflow 值计算出的结果 | `SideEffect` |
| 同一 ID 的值随执行推进更新 | `MutableSideEffect` |
| 外部读取、系统时间、Go 库计算 | Activity |
| 代码版本分支 | `GetVersion` |

## 数据约束

Marker 经过 JSON 持久化：

- 超过 2^53 的整数和精度敏感的小数用字符串表示。
- 整数经过存储后以浮点形式读回，`Get` 按目标类型解码还原。
- 闭包返回值的结构保持向后兼容。
- 闭包可以返回结构体，字段按 `json` tag 写入 Marker，`Get` 解码回同类型；`MutableSideEffect` 的 `equals` 收到的两个参数都是该结构体类型。

## 故障场景

- Marker 写入后 Worker 退出：重放读取已提交的值。
- 闭包执行出错：当前 Workflow Task 失败并记录诊断信息。
- `MutableSideEffect` 的 ID 改名：新代码建立另一条 Marker 序列。
- `equals` 语义变化：只影响之后的首次执行决策，已记录的 History 按原记录重放。

相关内容：[确定性约束](/develop/determinism)、[Workflow 版本管理](/develop/versioning)。
