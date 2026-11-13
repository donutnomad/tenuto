---
title: Activity 幂等最佳实践
description: at-least-once 执行下的幂等键：操作名加这一次操作的身份，三种身份来源与各自的稳定范围
status: Stable
---

# Activity 幂等最佳实践

<FeatureStatus status="Stable" />

Tenuto 保证 Activity 至少执行一次（at-least-once），不保证只执行一次。Attempt 失败或超时后按重试策略再执行；Worker 停止时交还的 Attempt 会派发给其他 Worker；结果上报丢失时，下游已经生效的调用会被再执行一次；人工 Reset 会让 Activity 从头再来。**每个 Activity 都必须幂等**：同一次操作执行多次，外部系统的效果与执行一次相同。这是编写 Activity 的基本要求。

## 幂等键

幂等键标识的是"这一次操作"：

```text
幂等键 = 操作名 + 这一次操作的身份
```

操作名区分同一个对象上的不同操作（扣款、退款、发货）。身份区分同一种操作的不同次（第 1 期与第 2 期扣款、第一次与第二次部分退款）。只按对象取键（如 `charge:<OrderID>`）粒度过粗：同一订单的第二次扣款会被当成第一次的重复而被丢掉。

把同一个键传给下游的幂等接口，或写进带唯一索引的幂等记录表：

```sql
INSERT INTO payment_commands (idempotency_key, payment_id, status)
VALUES (?, ?, 'processing')
ON DUPLICATE KEY UPDATE idempotency_key = VALUES(idempotency_key);
```

"这一次操作的身份"有三种来源，按优先顺序：

### 1. 输入里已有的业务 ID（首选）

订单号加期数、退款单号、发货批次号、调用方传入的请求 ID。它们由业务定义，与 Workflow 怎么执行、执行了几次无关。

```go
type ChargeInstallmentInput struct {
    OrderID       string `json:"order_id"`
    InstallmentNo int    `json:"installment_no"`
    AmountCents   int64  `json:"amount_cents"`
}

func ChargeInstallment(ctx context.Context, in ChargeInstallmentInput) (ChargeInstallmentOutput, error) {
    key := "charge-installment:" + in.OrderID + ":" + strconv.Itoa(in.InstallmentNo)
    paymentID, err := gateway.Charge(ctx, key, in.AmountCents)
    if err != nil {
        return ChargeInstallmentOutput{}, err
    }
    return ChargeInstallmentOutput{PaymentID: paymentID}, nil
}
```

资金类等不能重做的操作只用这一种。业务上没有现成 ID 时，让发起方在启动 Workflow 的输入里带一个请求 ID。

### 2. 在 Workflow 里生成，随输入传给 Activity

业务没有天然 ID 时，在 Workflow 里用 `workflow.RandomUUID` 为每次操作生成一个，放进 Activity 的输入。值记在 History 里，重放和 Activity 重试拿到同一个值；循环里每次调用各生成一个。

```go
func PartialRefundsWorkflow(ctx workflow.Context, in PartialRefundsInput) error {
    for _, amount := range in.Amounts {
        refund := RefundInput{RefundID: workflow.RandomUUID(ctx), PaymentID: in.PaymentID, AmountCents: amount}
        var out RefundOutput
        if err := workflow.ExecuteActivity(ctx, IssueRefund, refund, workflow.ActivityOptions{
            StartToCloseTimeout: 30 * time.Second,
        }).Get(ctx, &out); err != nil {
            return err
        }
    }
    return nil
}

func IssueRefund(ctx context.Context, in RefundInput) (RefundOutput, error) {
    reference, err := gateway.Refund(ctx, "issue-refund:"+in.RefundID, in.PaymentID, in.AmountCents)
    if err != nil {
        return RefundOutput{}, err
    }
    return RefundOutput{RefundReference: reference}, nil
}
```

生成的 ID 也适合存进业务记录，之后用它查询或对账。需要其他形式的值时用 `workflow.SideEffect`，见 [副作用与随机值](/develop/side-effects)。

### 3. Workflow ID + Activity ID

`activity.GetInfo(ctx)` 的 `ActivityID` 标识 Run 里被调度的这一个 Activity，同一个 Activity 的各次重试相同；`WorkflowID` 标识所属的 Workflow。两者拼起来，不改输入就能得到幂等键：

```go
func SendReceipt(ctx context.Context, in SendReceiptInput) (SendReceiptOutput, error) {
    info := activity.GetInfo(ctx)
    key := "send-receipt:" + info.WorkflowID + ":" + info.ActivityID
    return SendReceiptOutput{}, mailer.Send(ctx, key, in.Email, in.Body)
}
```

`ActivityID` 没有在 `ActivityOptions` 里指定时按 Activity 在 Workflow 代码里的调用位置编号，每个 Run 从头编号；指定时是代码给出的值，同样只在一个 Run 内有意义。所以这个键只在同一个 Run 里唯一，适用于不会 continue-as-new、Workflow ID 不复用、执行结果不打算靠 Reset 重做的 Workflow。

### 各来源的稳定范围

| 场景 | 1 业务 ID | 2 Workflow 生成 | 3 Workflow ID + Activity ID |
| --- | --- | --- | --- |
| Attempt 失败或超时后重试、Worker 交还后重新派发 | 相同 | 相同 | 相同 |
| 人工 Reset 单个 Activity | 相同 | 相同 | 相同 |
| Workflow Reset，回退点在这次调用之前 | 相同 | 回退点在 `RandomUUID` 之前时为新值 | 相同：新 Run 沿用 Workflow ID，重新执行的调用拿到同一个 Activity ID，下游把它当成重复请求 |
| Workflow 级重试 | 相同 | 新值 | 相同：同上 |
| Continue-As-New 之后的下一代 Run | 相同 | 新值 | **冲突**：下一代 Run 从头编号，同一位置上的另一次操作得到上一代的键，被下游当成重复而丢掉 |
| 关闭后用同一个 Workflow ID 启动新的 Workflow | 相同 | 新值 | **冲突**：同上 |

三种来源都能挡住 Attempt 级的重复执行。区别在 Workflow 级的重新执行：Reset 或 Workflow 重试后，第 1、3 种得到原来的键，下游返回第一次的结果，不会再做一次；第 2 种在回退点越过生成位置时得到新键，下游会再执行一次。希望 Reset 后真正重做的操作用第 2 种；不能重做的用第 1 种。

### 不能用来构造幂等键的值

- `Attempt`：每次重试都变，用了等于没有幂等键。只用于日志。
- 时间戳：`time.Now()`、`info.StartedTime` 每次 Attempt 都不同。
- Activity 里生成的随机值：`uuid.New()`、`rand` 在每次 Attempt 里都是新值。随机 ID 只能在 Workflow 里生成后传入（第 2 种）。
- `RunID` 单独使用：Reset、Workflow 重试和 continue-as-new 都会换一个 Run，键随之改变。

## 超时与未知结果

Start-To-Close 超时只说明 Tenuto 没有收到这次 Attempt 的结果，下游可能已经执行成功。幂等键让下一次 Attempt 得到第一次的结果，而不是再执行一次；没有幂等接口的下游，先按幂等键查询外部状态再决定是否执行。

## Heartbeat

长任务定期记录可恢复的游标。Heartbeat details 保存已提交的进度，下一次 Attempt 用 `activity.GetLastHeartbeatDetails` 从该位置继续，已经处理过的部分不重复执行。

## 补偿

跨多个系统的业务事务使用 Saga：每个成功步骤记录补偿所需 ID，后续步骤失败时按业务逆序执行补偿 Activity。补偿本身也是 Activity，同样必须幂等，用被补偿操作的键派生自己的键（如 `release-inventory:<reservation_id>`）。

## 人工操作

Activity Reset 前检查外部副作用和幂等记录。异步完成的回调保存 provider event ID，重复的 Webhook 返回同一结果。

示例代码见 `docs/examples/idempotency/example_test.go`。
