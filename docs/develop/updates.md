---
title: Typed Update
description: Update Handler、生命周期、强类型结果、幂等和异常场景
status: Stable
---

# Typed Update

<FeatureStatus status="Stable" />

Update 是带结果的持久化业务命令。Server 记录 Accepted，Workflow Handler 返回后记录 Completed，调用方通过 Handle 读取强类型结果或 Application Error。

## Handler 签名

```go
func(workflow.Context, ...Args) error
func(workflow.Context, ...Args) (Result, error)
```

示例：

```go
type ChangeLimitResult struct {
    Previous int64 `json:"previous"`
    Current  int64 `json:"current"`
}

err := workflow.SetUpdateHandler(ctx, "change-limit",
    func(ctx workflow.Context, next int64) (ChangeLimitResult, error) {
        if next <= 0 {
            return ChangeLimitResult{}, workflow.NewApplicationError(
                "limit must be positive", "InvalidLimit", next,
            )
        }
        previous := limit
        limit = next
        return ChangeLimitResult{Previous: previous, Current: limit}, nil
    },
)
```

## 生命周期

```text
请求 → Validator → WorkflowUpdateAccepted → Handler 执行
                                      ├─ success → WorkflowUpdateCompleted
                                      └─ failure → WorkflowUpdateCompleted
```

`Accepted` 证明 Update 已进入 Run 的持久化历史。`Completed` 证明 Handler 已返回结果或业务错误。

## Client

```go
handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
    WorkflowID:   workflowID,
    UpdateID:     requestID,
    UpdateName:   "change-limit",
    Args:         []any{int64(5000)},
    WaitForStage: client.WorkflowUpdateStageAccepted,
})
if err != nil {
    return err
}

var result ChangeLimitResult
if err := handle.Get(ctx, &result); err != nil {
    return err
}
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`，Namespace 取自它。多个 goroutine 调用同一个 Handle 的 `Get` 时会共享 Completed 轮询结果。

等待 `Completed` 的请求在 Server 上最多等 20s：Update 完成时应答带结果；Run 在 Update 完成之前结束时，History 里这个 Update 已是失败的终态，应答带这个失败；20s 到期仍未完成时应答 `Accepted`，`handle.Get` 以同一个 Update ID 再发一次（按 Update ID 幂等，不会重复执行 Handler）。

### 跨副本唤醒

多副本部署时，受理 Update 请求的副本与处理完成它的 Workflow Task 的副本通常不同。等待方（Update 等 `Completed`，以及 [`WorkflowRun.Get`](/sdk/client#get-跟随执行链) 用的 `wait-closed`）在受理请求的副本上按 Run 登记，结果总从数据库读取：

1. 先读 Run 的 execution state 的 `next_event_id`，再读 History（Update 的记录或 Run 的结束事件）。满足条件就返回。
2. 不满足时带着读到的 `next_event_id` 等待 Run 变化。History 每追加一条事件 `next_event_id` 就增大，并与事件在同一事务提交，所以第 1 步之后的任何提交都会被看到，不会漏掉唤醒。
3. 唤醒有两个来源：本副本提交 Update 完成或 Run 结束后立即唤醒；有等待者期间，副本每 100ms 用一条 `SELECT ... FROM tenuto_execution_states WHERE run_id IN (...)` 读取本副本全部被等待的 Run（每 500 个 Run 一条），`next_event_id` 变大的 Run 唤醒它的等待者，等待者回到第 1 步。

代价：没有等待者的副本不读库、不排定定时器。有等待者时每个副本每秒 10 条批量读取，与等待者数量无关（被等待的 Run 超过 500 个时按 500 个一条增加）；Run 每变化一次，它的每个等待者多读一次 execution state 与 History（Update 等待者）或 execution state（`wait-closed`）。

等待者只存在于受理请求的副本内存里。副本重启或下线时进行中的请求随连接断开，调用方重发：`handle.Get` 与 `WorkflowRun.Get` 收到连接错误时返回错误，对同一个句柄再调用一次即可，Update 按 Update ID 幂等。

## Await 与 Activity

Handler 可以使用 `workflow.Await`、Activity Future、Timer、Channel、Selector 和 `workflow.Go`。Await 条件需要由后续 Signal、Timer、Activity 或其他 Update 推进。

```go
func(ctx workflow.Context) (CreateResult, error) {
    workflow.Await(ctx, func() bool { return createDone })
    return initResult, nil
}
```

Handler 的源码约束由生成器检查：Handler 写成内联函数，第一个参数是 `workflow.Context`，业务参数个数固定（不支持 `...T`），返回 `error` 或 `(Result, error)`。`Result` 使用 struct、标量、切片或带具体元素类型的 map；`SetUpdateHandler` 是处理 Update 的唯一方式。

## Validator

Validator 是 Server 侧按 Update Name 注册的无状态输入校验器，在写入 `WorkflowUpdateAccepted` 之前运行。它通过 Server 进程内的 `(*engine.Server).RegisterUpdateValidator(name, validator)` 注册，`sdk/workflow` 里没有对应的 Workflow 侧 API。没有注册 Validator 的 Update Name 全部进入 Accepted。Validator 拒绝时调用端得到 `client.ErrUpdateRejected`，History 不产生任何事件。

依赖 Workflow 状态的校验写在 Handler 里，通过返回 `workflow.NewApplicationError` 表达拒绝。

## Handler 缺失

Update 进入 Accepted 后，Worker 在 Workflow 主流程第一次阻塞（或返回）时查找同名 Handler。没有找到时，Worker 用 `Failure{Type: "UnknownUpdate", Message: "no update handler registered for \"<name>\""}` 完成这个 Update，History 写入 `WorkflowUpdateCompleted`（failure）。调用端的 `handle.Get` 或 `WaitForStage=Completed` 得到 `*apperror.Error`，`Type()` 为 `UnknownUpdate`。

Handler 在主流程第一次阻塞之前注册，才能接到 Run 启动时就已到达的 Update。调用端和 Workflow 共享稳定的 Update Name。

## 行为矩阵

| 场景 | History | 调用结果 |
| --- | --- | --- |
| Validator 拒绝 | 无 Accepted | `ErrUpdateRejected` |
| Handler 成功 | Accepted + Completed success | 解码强类型 Result |
| Handler 返回 Application Error | Accepted + Completed failure | `*apperror.Error`（`Outcome.Failure`） |
| Handler 缺失 | Accepted + Completed failure | `*apperror.Error`，Type 为 `UnknownUpdate` |
| Handler Await 尚未满足 | Accepted | Handle 保持可等待 |
| 同一 Update ID 重试 | 复用已有记录 | 返回相同阶段或结果 |
| 同一 Update ID 并发发送 | first-wins Accepted | 所有调用观察同一逻辑 Update |
| Client Context 先截止 | 服务端状态继续推进 | 使用相同 Update ID 再次调用 |
| Accepted 后、Handler 完成前 Run 关闭（完成、失败、超时、取消、终止、continue-as-new） | Accepted + Run 终态事件，不写 Completed | `*apperror.Error`，Type 为 `AcceptedUpdateCompletedWorkflow`，不可重试；等待 Completed 的调用在 Run 关闭时立即返回，查询看到同一结果 |
| Run 已终态后发送 | 无新 Accepted | Update ID 已记录过时返回当时的阶段或结果；新的 Update ID 返回终态错误 |
| Run 已终态后用 Update-With-Start 发送 | 新 Run 的 Started + Accepted | 启动新 Run 并处理 Update；Update ID 已记录过时返回当时的结果 |
| Payload 解码失败 | Handler 无法执行 | 返回可诊断失败 |

## 版本契约

Update Name、参数顺序、JSON 字段、结果类型和 Application Error Type 都属于持久化契约。发布新版本时保留仍有 Run 在用的 Handler，这些 Run 完成后再移除对应名称。

原子启动和更新流程见 [Update-With-Start](/sdk/update-with-start)，错误结构见 [Failure 参考](/reference/failures)。
