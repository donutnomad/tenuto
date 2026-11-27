---
title: Workflow 版本管理
description: Run 版本策略 pinned / auto_upgrade、GetVersion、Activity 契约与 Worker Build ID、安全发布
status: Stable
---

# Workflow 版本管理

<FeatureStatus status="Stable" />

Tenuto 有三层互相独立的版本机制：

| 层 | 机制 | 决定什么 |
| --- | --- | --- |
| Workflow 定义内容 | 内容 hash + latest 指针 + Run 的 `policy` | 一个 Run 用哪份 Program 执行 |
| 源码兼容分支 | `workflow.GetVersion` | 同一份 Program 面对新旧 History 走哪条分支 |
| Activity Worker | Task Queue、三维契约、Build ID | 哪些 Worker 进程可以执行 Activity |

## 定义版本与 latest

生成器为每份 `workflow.json` 计算内容 hash。同一个 `(Namespace, Workflow Type, 内容 hash)` 只保存一份，重复登记幂等。每个 Workflow Type 有一个 latest 指针：

- Worker 启动自动登记：新内容首次出现时 latest 指向它；同 hash 重复启动保持 latest。
- `tenuto-workflow-gen generate -url ... -publish`：注册并激活 latest，用于显式发布和回滚。
- `tenuto-workflow-gen generate -url ...`（不带 `-publish`）：只登记，latest 保持不变。

新启动的 Run 使用当时的 latest。`@Workflow(version=...)` 是作者声明的发布标签，写入配置并参与内容 hash。

## Run 版本策略

`@Workflow(policy=...)` 决定运行中的 Run 如何对待之后登记的新内容：

| policy | 行为 |
| --- | --- |
| `auto_upgrade`（缺省） | Run 在后续 Workflow Task 边界采用定义的 latest 内容 |
| `pinned` | Run 固定启动时的内容，只有显式迁移才切换 |

```go
// @Workflow(type="order-flow",policy=pinned,version=v3)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error)
```

运行中修改单个 Run 的策略：

```go
err := c.UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{
    WorkflowID: workflowID,
    RunID:      runID, // 空值为 current Run
    Policy:     workflow.PolicyPinned,
})
```

`c` 是 [`sdk/client`](/sdk/client) 的 `Client`，调用的是 HTTP 端点 `/runtime/workflow/set-policy`（下文记作 `SetRunPolicy`）。策略接受 `auto_upgrade`（`workflow.PolicyAutoUpgrade`）、`pinned`（`workflow.PolicyPinned`），只改变该 Run 的有效策略，不改变它当前使用的内容。它只作用于 `running` 的 Run；Run 已结束时返回 `cannot set policy: workflow <run_id> is <status>`（HTTP 400），已存的策略保持原值。

对暂停（`ConfigError`）的 Run 调用它就是恢复：Server 关闭暂停时停在半路的 Workflow Task（History 里是一条 `WorkflowTaskFailed`，`cause` 为 `run_resumed`），再调度一个新的 Workflow Task。内容选择发生在 Workflow Task 开始时，所以 `auto_upgrade` 的恢复在这个新任务上换到 latest，`pinned` 的恢复留在原内容。

内容选择在开始 Workflow Task 的事务里按该 Run 此刻的策略与暂停状态确定，多个 Server 副本之间也是如此：`SetRunPolicy` 返回之后，任何副本开始的 Workflow Task 都按新策略选择内容。

Run 固定了目标 Worker（`target_worker_identity`）时，`auto_upgrade` 换到 latest 之前先判定目标 Worker 能否承接 latest 的全部 Activity：不能承接时 Run 停在当前内容并以 `ConfigError` 暂停；活跃 Worker 列表读取失败时无法判定，这个 Workflow Task 留在当前内容，下一个 Workflow Task 再判定。

`auto_upgrade` 换到 latest 之前还检查 History 里已有的载荷能否被 latest 的类型读回原值；读不回时 Run 留在当前内容并以 `ConfigError` 暂停，见下文「换版前的载荷检查」。

选择依据：

- `auto_upgrade` 的 Run 会用新 Program 重放旧 History，新代码需要通过 `GetVersion` 兼容旧命令序列：每个已完成的 Workflow Task 产生的命令与 History 里该 Workflow Task 的命令事件一致。
- `pinned` 的 Run 始终用启动时的 Program，新代码可以自由改动；旧 Program 的 Activity 仍需要有 Worker 提供。

## GetVersion

```go
version := workflow.GetVersion(ctx, "charge-policy", 3, 6)
if version >= 6 {
    charged, err = activities.CallChargeV2(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
} else {
    charged, err = activities.CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
}
```

签名：`func GetVersion(ctx Context, changeID string, minSupported, maxSupported int) int`。

| 场景 | 返回值 |
| --- | --- |
| 新执行首次到达该 change ID | `maxSupported`，写入 Marker |
| History 里已有该 change ID 的 Marker | Marker 记录的版本 |
| 重放旧 History、首次遇到该 change ID 且没有 Marker | `workflow.DefaultVersion`（`-1`） |
| 同一 Run 内再次调用同一 change ID | 与首次调用相同的值 |

返回值必须落在 `[minSupported, maxSupported]` 内，否则 Worker 报告非确定性错误、Run 进入 Blocked：

- 版本小于 `minSupported`：代码已移除对该版本的支持。
- 版本大于 `maxSupported`：代码比 History 旧。

### 给已上线的代码加第一个版本点

旧 Run 的 History 里没有 Marker，重放时得到 `workflow.DefaultVersion`。`minSupported` 写成 `workflow.DefaultVersion` 才能覆盖它们：

```go
if workflow.GetVersion(ctx, "step-3-to-6", workflow.DefaultVersion, 1) == workflow.DefaultVersion {
    // 旧流程：History 里没有 Marker 的 Run
} else {
    // 新流程
}
```

### 提高版本上限

已有版本点继续演进时提高 `maxSupported`，旧 Marker 的值仍在范围内：

```go
// v1 代码
version := workflow.GetVersion(ctx, "charge-policy", 3, 4)
// v2 代码：新执行记录 6，v1 写入的 Marker 读回 4
version := workflow.GetVersion(ctx, "charge-policy", 3, 6)
```

### 收敛旧分支

确认没有 Run 依赖旧版本后提高 `minSupported` 并删除旧分支。此后 History 里版本低于新下限的 Run 会报非确定性错误，因此先用搜索属性确认存量。

## 查询仍依赖旧版本的 Run

新执行写入版本 Marker 时，同步更新保留搜索属性 `TemporalChangeVersion`，取值为本 Run 已确定版本的 `changeID-version` 列表。编码后长度达到 2048 字节时跳过写入。该键由引擎独占，外部 API 写入会被拒绝。

```go
result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalChangeVersion": "charge-policy-4"},
})
```

只有新执行经过变更点时才写入该属性。重放时得到 `workflow.DefaultVersion` 的 Run 不带这个值，按 Workflow ID 和启动时间确认。

## Activity Worker 与 Build ID

版本是 Run 的属性（`policy` 与 Program 内容），单个 Activity 不固定 build：

- `worker.Options.BuildID` 声明 Worker 的代码版本，用于登记契约、观测与 Sticky Cache，不出现在 Activity 任务上。`workflow.ActivityOptions` 没有 `BuildID`。
- Activity 任务投到它的 Task Queue。每次投递（首次调度、重试、恢复重派、Worker 停止交还）都只按队列路由；Run 固定了目标 Worker（`target_worker_identity`）时只交给这个 Identity。队列上任何 build 的 Worker 都能领取。
- Worker 开始一次 Attempt 时，Server 把 Run 当前 Program 对这个 Activity 声明的契约与领取者这个 build 的上报对照。不兼容时这次领取被拒绝：Attempt 不开始、不消耗重试次数，任务在 Workflow Task 失败退避的首个间隔（缺省 1s）之后重新投递，由其他 Worker 领取。
- `ActivityStarted` 事件记录实际执行的 `workerIdentity` 与 `workerBuildId`。

`GetVersion` 选择业务分支，契约决定哪些 build 能执行 Activity，两者互不影响。配置见 [运行 Worker](/develop/workers)。

旧 build 下线前先排空：Program 声明的契约只有旧 build 满足时（例如 `pinned` 在旧 Program 上的 Run），它的 Activity 一直等契约兼容的 Worker 领取；旧 build 已经下线时任务留在队列里，新 build 每次领到都被拒绝。等这些 Run 跑完再下线旧 build，或者给 Activity 设置 `ScheduleToStartTimeout` 限定等待。契约不变（只改实现）或只做兼容改动（入参新增字段）的发布不受影响：旧 build 下线后，重试与已经排队的投递由新 build 领取。

## 发布流程

1. 新代码加入 `GetVersion`，同时保留旧分支；或者把 Workflow 声明为 `pinned`。
2. 运行统一生成，用旧 History 样本执行 Replay 测试。
3. 部署新 Worker。先升级全部 Server，再升级独立部署的 Workflow Worker。
4. 观察 Blocked Run、Workflow Task failure 和 Sticky fallback。
5. 等待存量 Run 越过变更点或完成，再提高 `minSupported` 收敛旧分支。

## 需要版本保护的变化

| 变化 | 风险 |
| --- | --- |
| 新增、删除或重排 Activity | Command 序列改变 |
| 修改 Timer 位置 | Timer Command 改变 |
| 修改协程启动顺序 | 缺省 ActivityID 等原语 ID 和调度顺序改变 |
| 修改 Selector case 顺序 | ready case 选择改变 |
| 新增、删除 SideEffect / RandomUUID / RandomInt 调用 | Marker 对应关系改变 |
| 修改 Update / Query Handler 名称 | 调用端契约改变 |

Activity 内部实现可以常规发布；它的输入输出结构和错误 Type 保持业务兼容。Activity 的契约是 `type`、`input` 指纹、`output` 指纹三项。Server 调度 Activity 时不对照契约，任务照常写入 `ActivityScheduled` 并投到队列；Worker 领取时 Server 按领取者自己的上报对照 Program 声明的契约：指纹相同即兼容；指纹不同时按数据流向比较 wire 结构：`input` 由 Workflow 写、Worker 读，`output` 由 Worker 写、Workflow 读，写方的每个字段读方都要有、类型相同或无损放宽（如 `int32` → `int64`、`T` → `*T`），读方多出的字段取零值。所以 Worker 的入参新增字段兼容，旧 Program 的调用由新 build 直接执行；删字段、改名、改类型不兼容；Worker 的出参新增字段时旧 Workflow 不认识它，同样不兼容。不兼容的领取被拒绝，任务约 1s 后重新投递。在线 Worker 全都不兼容时任务在队列里等待，Run 不暂停，契约兼容的 build 上线后自动领取；设置了 `ScheduleToStartTimeout` 时按它超时。新旧 build 同时在线且契约不同时，任务可能先被不兼容的 build 领到，每次拒绝让这次投递晚约 1s。

结构不变、结果的含义变了（例如运费从不含税改成含税）时，指纹不变，Server 分不出新旧含义。新含义用一个新的 `type`，旧 `type` 的实现留在 Worker 里，直到所有 Program 都不再调用它、旧 Program 上的 Run 都已结束。在途 Run 的 History 里已经记过旧 `type` 的位置，换成新 `type` 时用 `GetVersion` 保护。

## 改包名、类型名与函数名

把 Workflow 和它的类型移到另一个包（例如 `order` 改名为 `order222`）、改 Go 类型名或函数名，存量 Run 照常运行，前提是下表右侧四项保持不变：

| 可以改 | 保持不变 |
| --- | --- |
| 包名、包路径（移动目录） | `@Workflow(type=...)` |
| Go 类型名、Go 字段名（`json` tag 不变） | `@Activity(type=...)` |
| Workflow 函数名、Activity 函数名 | JSON 字段名（`json` tag） |
| | JSON 字段的类型 |

原因：

- Program 的类型表以 `<import path>.<类型名>` 命名，Activity 符号是 `<包名>.<函数名>`，所以改名后生成器产出新的内容版本，按 Run 的 `policy` 处理：`pinned` 的 Run 留在旧 Program，`auto_upgrade` 的 Run 在下一个 Workflow Task 换到新 Program。
- History 只保存 JSON 载荷（Activity 输入与结果、子 Workflow 输入与结果、Signal），每份 Program 用自己的类型表解码。新 Program 按同样的 JSON 字段读出同样的值。
- 重放比较命令种类、ID 与 ActivityType，不比较 Go 名字。Activity 契约指纹由 JSON 字段名与类型算出，改名前后相同，新旧 build 互相承接 Activity。
- Execution Cache Key 含内容版本：换到新 Program 的 Run 在这个 Worker 上按完整 History 恢复一次，以新的 Key 入缓存。

改 `@Workflow(type)` 等于新建另一个 Workflow，存量 Run 不会换到它；改 `@Activity(type)` 让重放的 ActivityType 失配（见下文「需要版本保护的变化」）。

改 JSON 字段名（如 `json:"order_id"` 改成 `json:"orderId"`）会改变 Activity 契约指纹，并且旧载荷不再对应新字段：

- `auto_upgrade` 的 Run：History 里已有 `order_id` 形状的载荷时，换版前的载荷检查拒绝升级，Run 留在旧 Program 并以 `ConfigError` 暂停（见下一节）。重放本身不比较载荷，没有这项检查时新类型会把旧载荷解成零值并继续执行。
- `pinned` 的 Run 留在旧 Program，它的 Activity 需要旧契约的 build；旧 build 已下线时这个 Activity 在队列里等待（新 build 的领取被拒绝），旧 build 重新上线后自动继续。

字段改名按下文「安全地改 JSON 字段名」分步发布；Activity 的破坏性改动也可以用新 `type` 加 `GetVersion` 分支，或者把 Workflow 声明为 `pinned` 并保留旧 build 到存量 Run 结束。

以上行为由 `sdk/worker/package_rename_e2e_test.go` 验证：真实生成器生成 `order` 与 `order222` 两个包，`pinned` 与 `auto_upgrade` 的 Run 跨过升级（含 Execution Cache）完成并按各自的 Program 重放；改了 JSON 字段的版本作为反例：`auto_upgrade` 的 Run 被暂停、不产出错误结果，发布保留 `order_id` 的版本后恢复并正确完成。

## 换版前的载荷检查

`auto_upgrade` 的 Run 在 Workflow Task 开始时换到 latest 之前，Server 对照当前 Program 与 latest 在每个载荷位置解码用的类型，只看 JSON 结构（字段名、类型、可选性），与 Go 包名、类型名无关：

| 载荷位置 | History 里的事件 | 类型取自 |
| --- | --- | --- |
| Workflow 输入（含 continue-as-new 传入的输入） | `WorkflowStarted` | 入口参数类型 |
| Signal（按名字） | `WorkflowSignaled` | `WaitSignal(ctx, "name").Get(ctx, &v)` 的 `v` |
| Activity 结果（按 `type`） | `ActivityCompleted` | `Get(ctx, &v)` 的 `v`，或 `CallX` 的返回值 |
| 未关闭 Activity 的输入（按 `type`） | 没有关闭事件的 `ActivityScheduled` | 调用处的输入实参；之后由 Worker 按 Run 新 Program 的契约执行 |
| 子 Workflow 结果（按 Workflow Type） | `ChildWorkflowCompleted` | `Get(ctx, &v)` 的 `v` |
| Update 参数（按名字与序号） | `WorkflowUpdateAccepted` | `SetUpdateHandler` 处理函数的参数 |
| SideEffect / MutableSideEffect（按 ID）的值 | `MarkerRecorded` | `Get(ctx, &v)` 的 `v` |
| 应用错误的 detail | 带失败的 `ActivityFailed`、`ActivityTimedOut`、`ChildWorkflowFailed` | `ApplicationErrorDetail(err, i, &v)` 的 `v` |

只有 History 里已经出现的位置参与判定。兼容规则：

- 当前类型的每个字段在新类型里以同一 JSON 名存在，类型兼容，逐层递归（嵌套 struct、切片元素、map 的值）。新类型新增的字段允许，旧载荷里取零值。
- 字段删除、改名、改类型都不兼容。数值只接受无损放宽：整数变宽（`int32` → `int64`）、32 位以内的整数 → `float64`、`float32` → `float64`；`int64` 与 `float64` 互转不兼容。
- `T` 改成 `*T` 兼容；`*T` 改成 `T` 不兼容（`null` 与零值无法区分）。
- 一侧是 `any` 或 `map[string]any` 时结构未知，不判定。

不兼容时 Run 不换版本：内容保持当前版本，Run 以 `ConfigError` 暂停，没有 Workflow Task 在新 Program 上执行。消息写出位置、新旧类型与字段路径：

```text
auto upgrade to 3f2a… refused: payloads recorded in history cannot be read by the candidate program: signal "approve": order.Approval -> order222.Approval: order_id: field is missing in the new type (removed or renamed); set policy pinned to finish on the current program, or publish a version that keeps these fields and set policy auto_upgrade to resume
```

处理二选一：

- `SetRunPolicy(..., pinned)`：Run 在当前内容上继续，当前内容用到的 Activity 仍需要契约兼容的 Worker。
- 发布能读回这些载荷的版本（保留旧字段），再 `SetRunPolicy(..., auto_upgrade)`：恢复后的 Workflow Task 重新检查并换到新的 latest。

内容按版本不可变：Server 进程内按版本保存解码后的内容，调度、派发与 Activity 开始时的契约对照都读同一份，不再逐次解码；两份内容之间的对照结果也按版本对缓存在进程内；有不兼容位置时才读取 Run 的 History。当前内容或 History 读取失败时本次 Workflow Task 留在当前内容、不暂停，下一个 Workflow Task 再判定。

### 检查如何找到解码目标

检查从 Program 的语句里找每处解码（`Get`、`ApplicationErrorDetail`、Update 处理函数的参数）的位置与类型：

- future：直接调用（`workflow.WaitSignal(ctx, "approve").Get(...)`）、保存它的变量、future 切片的元素、同包函数的 future 参数（按调用处的实参），以及 Selector 回调的参数——`AddFuture` / `AddSignalFuture` / `AddChildFuture` 回调里的 `f` 就是加入的那个 future。
- 名字（Signal 名、Activity `type`、子 Workflow Type、MutableSideEffect ID、Update 名）：字面量与 Go 常量、只赋值为同一常量的变量、常量的 `+` 拼接、实参全是常量的 `fmt.Sprintf`、每个调用处都传同一常量的同包函数参数。
- 类型：`var v T`、`v := T{...}`、同包函数的返回值（含 `v, err := f()`）、从另一个变量或字段（`v := in.Order`）复制、入口参数。变量按作用域查找，不同闭包里的同名变量互不影响。

`AddReceive` 回调里 `Receive` 的值来自 Workflow 内部的 Channel，不进 History，不检查。Query 的参数同样不进 History。

位置或类型无法静态确定的解码处（例如 `workflow.WaitSignal(ctx, "approve-"+in.OrderID)` 的名字取自输入，或解码目标的类型来自宿主函数的返回值）按种类保守判定：同一种载荷（Signal、Activity 结果……）在当前 Program 与 latest 里的未确定解码处的类型 JSON 结构不同（类型未知时，两份 Program 类型表里任一类型的结构不同），且 History 里有这种载荷，检查无法证明安全，同样不换版并暂停。消息写出解码处在 WLang 程序里的路径（同包函数在 `let.__wlang_func_<函数名>` 下）与原因：

```text
auto upgrade to 3f2a… refused: the payload check cannot prove that payloads recorded in history stay readable: candidate program: a signal with a non-constant name decoded into order.Approval at program[12].expr.Get (the signal name is not a constant); signal types differ between the two versions; set policy pinned to finish on the current program, ...
```

两份 Program 的类型结构全部相同（例如只改了包名）时不暂停。处理方式与上面相同：`SetRunPolicy(..., pinned)` 在当前内容上跑完；或把名字改成常量、给变量写明类型后发布，再 `SetRunPolicy(..., auto_upgrade)`。这条规则比较的是类型，不比较代码：未确定的解码处改用另一个结构未变的已有类型时检查发现不了。

### 安全地改 JSON 字段名

以 `order_id` 改成 `orderId` 为例：

1. 新版本同时保留两个字段，代码两边都读：

   ```go
   type Approval struct {
       OrderID    string `json:"order_id"` // 旧字段，第 3 步删除
       OrderIDNew string `json:"orderId"`
   }

   id := approval.OrderIDNew
   if id == "" {
       id = approval.OrderID
   }
   ```

   新类型只是增加了字段，存量 Run 照常换版。
2. 发送方（Signal、Update、启动 Run 的调用方、Activity 实现的返回值）改为写 `orderId`。
3. 等所有在 History 里记录过这类载荷的 Run 结束，再删除 `order_id` 与回退分支。此时仍在运行、记录过这类载荷的 `auto_upgrade` Run 会被换版检查暂停，用 `SetRunPolicy(..., pinned)` 让它们在旧内容上跑完。

Activity 的输入或输出改名时，第 1 步的新旧 build 契约指纹不同，按[Activity Worker 与 Build ID](#activity-worker-与-build-id)的顺序发布：新 build 先上线，旧 build 在 `pinned` Run 跑完后下线。

## Sticky Cache 与版本

Execution Cache Key 包含 Workflow Type、Program Version、Build ID 和 Executor ABI。版本失配会驱逐旧 session，并走完整恢复路径。

## 回滚

重新登记旧内容并激活（`-publish`）把 latest 切回旧 hash。回滚后的代码需要理解已写入的新 Marker：History 里的版本大于旧代码的 `maxSupported` 时报非确定性错误。发布前保存双向 Replay 样本。

相关内容：[确定性约束](/develop/determinism)、[安全发布](/best-practices/safe-deployments)、[预生产测试](/operations/pre-production-testing)。完整示例在仓库 `docs/examples/versioned-workflow` 和 `docs/examples/multiprocess`。`docs/examples/upgrade-demo` 用三个独立进程把 Activity 与 Workflow 的七个升级场景逐个跑一遍并断言结果，运行方式见该目录的 README。
