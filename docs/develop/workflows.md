---
title: 编写 Workflow
description: 用 Go 函数与 @Workflow 注解定义 Tenuto Workflow
status: Stable
---

# 编写 Workflow

<FeatureStatus status="Stable" />

Workflow 是一个带 `@Workflow` 注解的 Go 函数。生成器 `tenuto-workflow-gen` 把它翻译成 WLang Program，Workflow Worker 用解释器按 Event History 执行这份 Program。Workflow 函数的 Go 代码本身不在运行期被调用，`sdk/workflow` 里的函数都是定义期标记。

## 最小 Workflow

下面是仓库 `docs/examples/startup-workflow/workflow.go` 的完整内容：

```go
package startupflow

import (
    "context"
    "time"

    workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type GreetInput struct {
    Name string `json:"name"`
}

type GreetOutput struct {
    Message string `json:"message"`
}

type SayHelloInput struct {
    Name string `json:"name"`
}

type SayHelloOutput struct {
    Message string `json:"message"`
}

// @Workflow(type="startup-greeting",name="Startup Greeting",policy=auto_upgrade,version=v1)
func Greet(ctx workflow.Context, in GreetInput) (GreetOutput, error) {
    out, err := CallSayHello(ctx, SayHelloInput{Name: in.Name}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
    if err != nil {
        return GreetOutput{}, err
    }
    return GreetOutput{Message: out.Message}, nil
}

// @Activity(type="startup.say-hello",queue="startup",version=v1)
func SayHello(_ context.Context, in SayHelloInput) (SayHelloOutput, error) {
    return SayHelloOutput{Message: "hello " + in.Name}, nil
}
```

`CallSayHello` 是生成器写出的调用桥，见 [注解与代码生成](/develop/code-generation)。

## 函数签名

| 位置 | 规则 |
| --- | --- |
| 第一个参数 | `workflow.Context` |
| 输入 | 一个结构体 DTO（或它的指针），或 `map[string]any`，或省略；形参名任意，Worker 按形参名绑定。Run 的 Input 是 JSON object，标量、切片与 `any` 入参在生成时报错（`文件:行:列: Workflow W 的输入类型 string 不是 JSON object`） |
| 返回 | `(Out, error)`、`error`，或只返回数据 |
| 输出类型 | 结构体或 `map[string]any`。Run 的 Output 统一存成 JSON object，标量返回值不进入 Output |
| 函数形态 | 包级普通函数；方法上的 `@Workflow` 被生成器拒绝 |

一个包目录只能有一条 `@Workflow` 注解。多个 Workflow 放在多个包里。

## @Workflow 注解

注解写在函数文档注释的 `//` 行里，逗号分隔 `key=value`，值可以带双引号。

| key | 必填 | 含义 |
| --- | --- | --- |
| `type` | 是 | Workflow Type，启动 Run 时用它指明运行哪个定义 |
| `name` | 否 | 展示名，缺省等于 `type` |
| `policy` | 否 | Run 版本策略：`auto_upgrade`（缺省）、`pinned`；其他取值在生成期报错 |
| `version` | 否 | 作者声明的发布标签，写入 `workflow.json` 并参与内容 hash |

未知 key、缺少 `type`、未知 `policy`、`/* */` 块注释里的注解都会报 `file:line` 错误。`policy` 的运行语义见 [Workflow 版本管理](/develop/versioning)。

## 调用 Activity

Workflow 通过生成的桥 `CallX` 调度 Activity：

```go
reserved, err := CallReserveInventory(ctx, ReserveInventoryInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if err != nil {
    return OrderOutput{}, err
}
```

桥调用只支持三种语句形态：`out, err := CallX(...)`、`out, err = CallX(...)`、`return CallX(...)`（以及 `if _, err := CallX(...); err != nil`）。直接调用 `@Activity` 实现函数会被生成器拒绝。并发调度使用 `workflow.ExecuteActivity` 返回的 Future，见 [并发、Channel 与 Selector](/develop/concurrency)。

## 等待条件

`workflow.Await` 阻塞当前协程，直到条件闭包返回 `true`：

```go
createDone := false
// 其他协程或 Update Handler 把 createDone 置为 true
workflow.Await(ctx, func() bool { return createDone })
```

条件闭包只读取 Workflow 内存变量。协程每次被恢复时条件重新求值，条件需要由后续 Signal、Timer、Activity 结果或 Update 推进。

## 读取 History 规模

```go
info := workflow.GetInfo(ctx)
if info.GetContinueAsNewSuggested() {
    return workflow.ContinueAsNew(ctx, "processor", state)
}
```

`workflow.Info` 提供 `GetCurrentHistoryLength()`、`GetCurrentHistorySize()`、`GetContinueAsNewSuggested()`、`GetContinueAsNewReasons()`，数值是当前 Workflow Task 开始时的已提交 History 快照。

## Run 何时结束

函数体返回决定 Run 的结束：

- 函数体返回 nil 时 Run 进入 `completed`，返回值是 Run 的 Output。
- 函数体返回非 nil `error` 时 Run 失败，错误编码成结构化 Failure，见 [ApplicationError](/develop/application-error)。返回取消错误（`workflow.IsCanceledError` 为 true）时 Run 进入 `cancelled`。
- 函数体返回时仍在等待的 `workflow.Go` 协程、未完成的 Activity、Timer 和正在执行的 Update Handler 被放弃。需要它们的结果时，在返回前等待，见 [并发](/develop/concurrency) 的「Run 的结束与未完成的协程」。
- 函数体自身还在等待（Activity、Timer、Signal、Await）时，Run 保持运行。
- 收到取消请求时根 Context 被取消，函数体继续运行，可以在 `workflow.NewDisconnectedContext` 上做清理，再按返回值结束，见 [取消、超时与终止](/develop/cancellation-timeouts#取消)。

## 启动 Run

Worker 启动时自动登记随二进制发布的定义（见 [运行 Worker](/develop/workers)）。调用端给出 Workflow Type（`@Workflow` 的 `type`）和 Workflow ID 启动：

```go
c, err := client.Dial(client.Options{HostPort: "localhost:7400"}) // Namespace 留空表示 default
if err != nil {
    return err
}
defer c.Close()

run, err := c.ExecuteWorkflow(ctx,
    client.StartWorkflowOptions{ID: "greeting-1001"}, // Workflow ID：这次执行的业务标识，可以留空
    "startup-greeting",                               // Workflow Type：运行哪个定义
    map[string]any{"name": "tenuto"},
)
if err != nil {
    return err
}
var out GreetOutput
err = run.Get(ctx, &out) // 等 Run 结束并解码结果
```

两个标识各管一件事：

| 标识 | 含义 | 规则 |
| --- | --- | --- |
| Workflow Type | 运行哪个定义，取 `@Workflow` 的 `type` | 必填；新 Run 使用该类型当前 active 的 Program |
| Workflow ID | 这次执行的业务标识，Signal、Query、Update、取消都按它寻址 | 选填；留空时生成一个 UUIDv7 文本（如 `0198c1f2-6f0a-7c3e-9d41-2b7e5a1c9f30`）：Go SDK 在发送前生成，HTTP 请求留空时由服务端生成 |

同一个 Namespace 和 Workflow ID 同时只有一个进行中的 Run：

- 该 Workflow ID 已有进行中的 Run 时，`ExecuteWorkflow` 返回 `client.ErrWorkflowExecutionAlreadyStarted`（HTTP 409 `current run conflict`）。
- 该 Workflow ID 的上一个 Run 已结束时，`ExecuteWorkflow` 启动新 Run，新 Run 成为这个 Workflow ID 的 current Run。
- 同一个 Workflow Type 可以同时运行任意多个 Run，每个 Run 用自己的 Workflow ID；留空 Workflow ID 时每次启动都得到新的 ID。

输入是 JSON object，字段名对应入口 DTO 的 `json` tag。[Signal-With-Start](/sdk/signal-with-start) 和 [Update-With-Start](/sdk/update-with-start) 采用同一套启动规则。

## Workflow API 一览

| 能力 | API | 文档 |
| --- | --- | --- |
| Activity | `CallX`、`workflow.ExecuteActivity`、`workflow.ActivityOptions` | [编写 Activity](/develop/activities) |
| 并发 | `workflow.Go`、`NewWaitGroup`、`NewChannel`、`NewBufferedChannel`、`NewSelector` | [并发](/develop/concurrency) |
| 等待 | `workflow.Await`、`workflow.Sleep` | 本页、[Timer](/develop/timers) |
| 时间与随机 | `workflow.Now`、`RandomUUID`、`RandomInt`、`SideEffect`、`MutableSideEffect` | [Side Effect 与确定性值](/develop/side-effects) |
| 消息 | `WaitSignal`、`SetQueryHandler`、`SetUpdateHandler` | [消息交互总览](/develop/signals-queries-updates) |
| 组合 | `ExecuteChildWorkflow`、`ContinueAsNew` | [Child Workflow](/develop/child-workflows)、[Continue-As-New](/develop/continue-as-new) |
| 版本 | `GetVersion` | [版本管理](/develop/versioning) |
| 错误 | `NewApplicationError`、`NewApplicationErrorWithCause`、`NewNonRetryableApplicationError`、`IsApplicationError`、`ApplicationErrorDetail`、`ApplicationErrorDetailCount` | [ApplicationError](/develop/application-error) |
| History 信息 | `GetInfo` | 本页 |

Workflow 函数体可用的 Go 语法见 [Workflow 可用的 Go 语法](/develop/go-subset)。
