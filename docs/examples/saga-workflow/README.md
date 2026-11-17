# Saga 显式逆序补偿示例

演示没有 Saga 框架时如何在 Workflow 函数体里写正向编排与逆序补偿。所有 Activity 都是同一包里的普通 Go 函数，使用结构体 I/O 与 `@Activity(type,queue,version)`；生成器产出 `CallX` 桥和包级 `RegisterActivities`。

## 文件

| 文件 | 内容 |
| --- | --- |
| `workflow.go` | 手写源码：`@Workflow` 编排（`PlaceOrder`） |
| `activities.go` | 手写源码：5 个 `@Activity` 实现与幂等键常量 |
| `types.go` | 手写源码：入参、输出与各 Activity 的 DTO |
| `zz_sagaflow_contract_gen.go` | 生成：`CallReserveInventory` 等调用桥、`RegisterActivities` |
| `zz_sagaflow_artifacts_gen.go` | 生成：`WorkflowDefinition()`（embed `workflow.json`） |
| `workflow.json` / `activities.json` | 生成：可注册的配置（含程序）与 Activity 清单 |
| `sagaflow_test.go` | 真实 WLang 解释器的行为矩阵测试 |
| `worker/main.go` | 真实双 Worker 启动顺序（Activity 先，Workflow 后） |

Workflow、Activity 实现与 DTO 按职责分文件存放：生成器把 `CallX` 桥重写成 `workflow.ExecuteActivity(ctx, <marker>, ...)`，marker 是包级函数符号；同包其它文件里声明的 `@Activity` 实现同样被解析成符号（生成器的回归用例见 `sdkgen/testdata/splitpkg` 与 `sdkgen/crossfile_test.go`）。唯一限制是 marker 名不能在该 Workflow 函数体内被局部变量或函数值遮蔽——那种写法按 Go 语义就是变量引用，生成会报明确诊断。

## 行为矩阵

`go test ./docs/examples/saga-workflow/` 覆盖下表每个场景。

| Scenario 输入 | 终态 | Activity 调度顺序 | 终态错误 |
| --- | --- | --- | --- |
| `""` | completed | reserve → charge → shipment | 无（output 带 reservation/payment/shipment） |
| `payment_declined` | failed | reserve → charge → release | `payment declined, reserved inventory released` |
| `shipment_rejected` | failed | reserve → charge → shipment → refund → release | `shipment rejected, compensation done=refund_payment,release_inventory` |
| `shipment_rejected_refund_failed` | failed | reserve → charge → shipment → refund → release | `... compensation_failed=refund_payment ` |
| `payment_unknown` | failed | reserve → charge | `charge payment outcome unknown, manual reconciliation required; ...` |
| `payment_pending` | failed | reserve → charge | `charge payment returned unknown status, manual reconciliation required; ...` |
| `shipment_unknown` | failed | reserve → charge → shipment | `create shipment outcome unknown, manual reconciliation required; ...` |

退款失败时仍然继续释放库存，再把失败项写进同一条错误；`refund_payment` 与 `release_inventory` 分别出现在 `failed=` 片段里。

## 两条语义边界

- **业务明确拒绝**：`ChargePayment.Status == "declined"` 或 `CreateShipment.Status == "rejected"` 且 `err == nil`。下游确认没有产生副作用，代码自动补偿。
- **结果不确定**：Activity 返回 `error`（超时、进程退出、响应丢失），或返回了既非成功也非拒绝的状态。此时下游可能已经生效，代码不执行任何补偿，直接返回 `SagaManualReconciliation`。

`payment_declined` 只释放库存，因为拒绝发生在扣款之前。`shipment_rejected` 才需要先退款再释放库存。

## 幂等键

固定操作名 + 稳定业务 ID：

```text
reserve_inventory:<OrderID>
charge_payment:<OrderID>
refund_payment:<PaymentID>
release_inventory:<ReservationID>
```

每个 Saga 步骤在一个订单上只执行一次，订单 ID 加操作名就是这一次操作的身份。`activity.GetInfo(ctx)` 的 `RunID` 在 Reset 或 Workflow 重试创建新 Run 时会变，只适合作日志标签。三种幂等键来源与各自的稳定范围见 [Activity 幂等](/best-practices/activity-idempotency)。

## 重试语义

生成的 `workflow.json` 没有 Workflow 级 `retry_policy`：Workflow 失败后直接进入终态，每个失败场景的 `WorkflowFailed` 事件 `RetryState` 与 `NewExecutionRunID` 都为空，不会自动新建 Run 重跑，人工核对不会被后台重试覆盖。

Activity 默认按 Server 的缺省策略不限次数重试。示例里结果可能不确定的步骤（扣款、发货、退款）写了有限次重试（`MaxAttempts: 3`，间隔 10ms），重试耗尽后 Workflow 拿到错误并转入人工核对或补偿失败；预留与释放库存使用缺省策略，失败时一直重试到成功。重试能安全进行，靠的是上面的业务幂等键：同一个键的重复请求，下游返回第一次的结果。

## 限制

- `SagaManualReconciliation` 只记录「哪些步骤已完成/未完成」，不保证外部副作用与 history 完全一致。外部已完成但响应丢失的窗口仍然存在，需要按幂等键查下游状态。
- 退款补偿有限次重试后仍失败时只产生明确的失败 Run 与错误信息，不落补偿账本；后续处理由人工或运维操作触发。
- 示例的下游是进程内函数，幂等键只回显在 Output 里，没有真实去重表。

## 命令

```sh
# 从仓库根目录生成（唯一入口在 server/cmd/tenuto-server/main.go）
go generate ./...

# 行为矩阵 + 注册契约
go test ./docs/examples/saga-workflow/ -v

# 真实双 Worker（需要已启动的 Tenuto Server）
go run ./docs/examples/saga-workflow/worker
```
