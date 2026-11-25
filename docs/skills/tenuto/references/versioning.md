# 版本与升级

三层互相独立的机制：

| 层 | 机制 | 决定什么 |
| --- | --- | --- |
| Workflow 定义内容 | 内容 hash + latest 指针 + Run 的 `policy` | 一个 Run 用哪份 Program 执行 |
| 源码兼容分支 | `workflow.GetVersion` | 同一份 Program 面对新旧 History 走哪条分支 |
| Activity Worker | 队列 + 四维契约（按 BuildID 登记） | 哪些 Worker 进程可以执行 Activity：领取时契约与 Program 对得上的 build |

升级演练见 `docs/examples/upgrade-demo/`（以该目录自带的说明为准）；`GetVersion` 的最小示例在 `docs/examples/versioned-workflow/`；多进程发布在 `docs/examples/multiprocess/`。

## 重放比较什么

重放时 Worker 把代码产生的命令逐条与 History 对照，比较三项：命令种类、ID（Activity 为 ActivityID）、ActivityType（`internal/executor/replay_match.go`）。Activity 的输入值、超时、重试参数不参与比较。失配时 Workflow Task 以 `non_deterministic_error` 失败，消息是下面之一：

```text
nondeterministic workflow: history event is <...>, replay command is <...>
nondeterministic workflow: missing replay command for <...>
nondeterministic workflow: extra replay command <...>
```

改变命令序列的改动（需要保护）：

| 改动 | 原因 |
| --- | --- |
| 新增、删除、重排 Activity / Child / Timer | 命令种类或缺省 ID 的序号改变 |
| 换 Activity `type` | ActivityType 改变 |
| 新增、删除 `SideEffect` / `RandomUUID` / `RandomInt` | Marker 对应关系改变 |
| 改协程启动顺序、把步骤移进或移出协程 | 协程路径改变，缺省 ID 改变 |
| 改 Selector case 顺序 | 同时就绪时选中的 case 改变 |
| 分支条件依赖的数据来源改变 | 走到另一条命令序列 |

不改变命令序列的改动（直接发布）：Activity 内部实现、传给 Activity 的输入值、超时与重试参数、纯内存计算、Query Handler 的返回内容、日志性质的字段。

## 改包名、类型名与函数名

可以改：包名与包路径、Go 类型名、Go 字段名（`json` tag 不变）、Workflow 与 Activity 的函数名。保持不变：`@Workflow(id)`、`@Activity(type)`、JSON 字段名、JSON 字段类型。

- 类型表的名字是 `<import path>.<类型名>`，Activity 符号是 `<包名>.<函数名>`：改名产出新的内容版本。`pinned` Run 留在旧 Program；`auto_upgrade` Run 下一个 Workflow Task 换到新 Program，用新类型表解码 History 里的 JSON 载荷，结果不变。
- Activity 契约指纹只由 JSON 字段名与类型算出，改名前后相同，新旧 build 互相承接。
- Execution Cache Key 含内容版本：换版的 Run 在该 Worker 上冷恢复一次（miss → 完整 History 恢复 → 新 Key 入缓存）。
- 改 JSON 字段名（`order_id` → `orderId`）：History 里已有旧形状载荷的 `auto_upgrade` Run 被换版前的载荷检查拒绝，留在旧 Program 以 `ConfigError` 暂停（下一节）；`pinned` Run 的 Activity 只有旧契约的 build 能执行，旧 build 下线时调度它记 `ConfigError`（`contract mismatch ... input expected ... actual ...`），旧 build 回来后 `SetRunPolicy(..., pinned)` 恢复。字段改名按下一节的三步发布。
- 出处：`sdk/worker/package_rename_e2e_test.go`（真实生成器生成 `order` / `order222`，`pinned` 与 `auto_upgrade` 跨升级完成并重放，含 Execution Cache；JSON 字段改名的反例里 `auto_upgrade` Run 暂停、不产出错误结果，发布保留 `order_id` 的版本后恢复并正确完成）。

## 换版前的载荷检查

`auto_upgrade` 的 Run 换到 latest 之前，Server 对照当前 Program 与 latest 在每个载荷位置解码用的类型（只看 JSON 结构，`program.ComparePayloadTypes`），只判定 History 里已经出现的位置（`execution.RecordedPayloadPositions`）：

| 位置 | History 事件 | 类型取自 |
| --- | --- | --- |
| Workflow 输入（含 continue-as-new 传入的） | `WorkflowStarted` | 入口参数 |
| Signal（名字） | `WorkflowSignaled` | `WaitSignal(ctx, "name").Get(ctx, &v)` 的 `v` |
| Activity 结果（`type`） | `ActivityCompleted` | `Get` 的 `v` / `CallX` 的返回值 |
| 未关闭 Activity 的输入（`type`） | 没有关闭事件的 `ActivityScheduled` | 调用处的输入实参 |
| 子 Workflow 结果（Workflow ID） | `ChildWorkflowCompleted` | `Get` 的 `v` |
| Update 参数（名字、序号） | `WorkflowUpdateAccepted` | Handler 的参数 |
| SideEffect / MutableSideEffect（ID）的值 | `MarkerRecorded` | `Get` 的 `v` |
| 应用错误的 detail | 带失败的 `ActivityFailed` / `ActivityTimedOut` / `ChildWorkflowFailed` | `ApplicationErrorDetail` 的 `v` |

- 兼容：旧类型的每个字段在新类型里同名存在且类型兼容（递归到嵌套 struct、切片元素、map 值）；新增字段允许。
- 不兼容：删字段、改 JSON 名、改类型；数值只接受无损放宽（`int32` → `int64`、32 位以内整数 → `float64`、`float32` → `float64`），`int64` ↔ `float64` 不兼容；`*T` → `T` 不兼容，`T` → `*T` 兼容。一侧是 `any` / `map[string]any` 不判定。
- 不兼容时不换版，内容保持当前版本，`ConfigError`：`auto upgrade to <version> refused: payloads recorded in history cannot be read by the candidate program: <位置>: <旧类型> -> <新类型>: <字段路径>: <原因>; ...`。处理：`SetRunPolicy(..., pinned)` 在当前内容上跑完；或发布能读回的版本后 `SetRunPolicy(..., auto_upgrade)`。
- 当前内容或 History 读不到时本次不换版、不暂停；对照结果按版本对缓存。
- 解码目标的识别：future 追溯到直接调用、变量、future 切片元素、同包函数的 future 参数、Selector `AddFuture` / `AddSignalFuture` / `AddChildFuture` 回调的参数；名字折叠常量（字面量、Go 常量、只赋同一常量的变量、常量 `+` 拼接、常量实参的 `fmt.Sprintf`、各调用处同值的函数参数）；类型取 `var v T`、`v := T{...}`、同包函数返回值（含多返回值）、变量复制、字段访问、入口参数。变量按作用域查找（每个闭包一层）。`AddReceive` 的 Channel 值与 Query 参数不进 History，不检查。
- 位置或类型无法确定的解码处（名字取自输入、类型来自宿主函数返回值等）按种类保守判定：两份 Program 里该种类未确定解码处的类型 JSON 结构不同（类型未知时类型表里任一结构不同），且 History 里有这种载荷，就不换版并暂停，`ConfigError`：`auto upgrade to <version> refused: the payload check cannot prove that payloads recorded in history stay readable: <current|candidate> program: <载荷> decoded into <类型> at <WLang 路径> (<原因>); <种类> types differ between the two versions; ...`。类型结构全部相同（只改包名）时不暂停。处理同上，或把名字改成常量、写明变量类型。规则比较类型、不比较代码：未确定解码处换成另一个结构未变的已有类型时检查不到。

字段改名的三步发布（`order_id` → `orderId`）：

1. 新版本两个字段都保留（`OrderID` 带 tag `json:"order_id"`，`OrderIDNew` 带 tag `json:"orderId"`），代码先读新字段、为空时读旧字段。只加字段，存量 Run 照常换版。
2. 发送方（Signal / Update / 启动方 / Activity 返回值）改写 `orderId`。
3. 记录过这类载荷的 Run 结束后删除 `order_id`；仍在运行的这类 `auto_upgrade` Run 会被暂停，`SetRunPolicy(..., pinned)` 让它们在旧内容上跑完。Activity 类型改名同时按「发布顺序」处理契约。

## 定义内容、latest 与 policy

- 生成器为每份 `workflow.json` 计算内容 hash；`(Namespace, Workflow ID, hash)` 只保存一份，重复登记幂等。
- 每个 Workflow ID 有一个 latest 指针，新启动的 Run 使用当时的 latest。
- Worker `Start` 自动登记 `worker.Options.Workflows`：新内容首次出现时 latest 指向它；已知 hash 再次登记保持 latest 不变（旧 Worker 重启不会把 latest 拉回去）。
- `tenuto-workflow-gen generate -url <server> <pkg>` 只登记（`POST /runtime/workflow/artifacts`）；加 `-publish` 登记并激活 latest（`POST /runtime/workflow/register`），用于显式发布与回滚。

`@Workflow(policy=...)` 决定运行中的 Run 如何对待之后登记的新内容：

| policy | 行为 | 对新代码的要求 |
| --- | --- | --- |
| `auto_upgrade`（缺省） | Run 在下一个 Workflow Task 边界换到 latest，用新 Program 重放已有 History | 新 Program 对所有存量 History 产生相同命令序列（靠 `GetVersion`） |
| `pinned` | Run 固定在启动时的内容 | 新代码可以自由改动；旧 Program 用到的 Activity（type、契约）仍要有 Worker 提供 |

注解只接受 `auto_upgrade` 与 `pinned` 两个值。

```go
// @Workflow(id="order-flow",policy=pinned,version=v3)
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error)
```

`@Workflow(version=...)` 是作者的发布标签，写入配置并参与内容 hash，不参与重放比较。

运行中修改单个 Run 的策略：

```go
err := c.UpdateWorkflowExecutionOptions(ctx, client.UpdateWorkflowExecutionOptions{
    WorkflowID: workflowID, RunID: runID, // RunID 空 = current Run
    Policy:     workflow.PolicyPinned,    // 或 workflow.PolicyAutoUpgrade
})
```

`c` 是 `sdk/client` 的 Client（下文把这个操作记作 `SetRunPolicy`）。HTTP：`POST /runtime/workflow/set-policy`，body `{"namespace":"orders","run_id":"...","policy":"pinned"}`。它改变该 Run 的有效策略、清除 `ConfigError`、关闭暂停时停在半路的 Workflow Task（`WorkflowTaskFailed`，`cause` 为 `run_resumed`）并立即调度一个新的 Workflow Task，`auto_upgrade` 在这个新任务上换到 latest；其他取值返回 `unknown workflow policy ...`；Run 已结束时返回 `cannot set policy: workflow <run_id> is <status>`（400），策略保持原值。Console 上对应两个按钮：`Resume with latest (AutoUpgrade)`、`Pin current content`。

内容选择在开始 Workflow Task 的事务里按 Run 此刻的策略与暂停状态确定（`execution.ChooseWorkflowTaskProgram`，`server/biz/domain/execution/upgrade_state.go`），多副本部署下 `SetRunPolicy` 返回之后任何副本都按新策略选版。固定了 `target_worker_identity` 的 `auto_upgrade` Run 换到 latest 前判定目标 Worker 能否承接 latest：不能承接 → `ConfigError` 暂停；活跃 Worker 读不到 → 本次留在当前内容，下一个 Workflow Task 再判定，不暂停。

## GetVersion

```go
// func GetVersion(ctx Context, changeID string, minSupported, maxSupported int) int
// const DefaultVersion = -1
```

| 场景 | 返回值 |
| --- | --- |
| 新执行首次到达该 change ID | `maxSupported`，写入 Marker |
| History 里已有该 change ID 的 Marker | Marker 记录的版本 |
| 重放的 History 已越过这一点且没有 Marker | `workflow.DefaultVersion`（`-1`） |
| 同一 Run 内再次调用同一 change ID | 与首次相同 |

返回值落在 `[minSupported, maxSupported]` 之外时，Workflow Task 以非确定性错误失败。

### 给已上线的代码加第一个版本点

存量 Run 的 History 没有 Marker，重放得到 `DefaultVersion`，所以 `minSupported` 写 `workflow.DefaultVersion`：

```go
v := workflow.GetVersion(ctx, "add-fraud-check", workflow.DefaultVersion, 1)
if v == workflow.DefaultVersion {
    // 旧流程：与改动前的命令序列完全一致
    charged, err = CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
} else {
    // 新流程
    if _, err := CallFraudCheck(ctx, FraudInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil {
        return OrderOutput{}, err
    }
    charged, err = CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
}
```

桥调用只能是完整右值，所以每个分支里各写一条 `out, err = CallX(...)`，变量在分支外声明。

### 继续演进

```go
v := workflow.GetVersion(ctx, "add-fraud-check", workflow.DefaultVersion, 2) // 提高 maxSupported
if v == workflow.DefaultVersion {
    // 原始流程
} else if v == 1 {
    // 第一次改动
} else {
    // 第二次改动
}
```

每个独立的改动点用独立的 change ID；同一个点的后续演进复用 change ID 并提高 `maxSupported`。

### 收敛旧分支

确认没有 Run 依赖旧版本后提高 `minSupported` 并删除旧分支。此后版本低于新下限的 Run 重放时报非确定性错误，所以先确认存量。

新执行写入版本 Marker 时同步更新引擎保留的搜索属性 `TemporalChangeVersion`，值是 `changeID-version` 列表：

```go
result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalChangeVersion": "add-fraud-check-1"},
})
```

重放时得到 `DefaultVersion` 的 Run 不带这个属性，按 Workflow ID 与启动时间确认它们已经结束。

### 回滚

把 latest 切回旧内容：对旧源码执行 `generate -url ... -publish`。旧代码需要理解已写入的新 Marker：History 里的版本大于旧代码的 `maxSupported` 时同样报非确定性错误。

## Activity 契约

每个 `@Activity` 生成四维契约：`type`、`version`、`input` 指纹、`output` 指纹（指纹形如 `wire:<hash>`，由 JSON 字段名与类型算出，见 `codegen.md`）。

- Workflow 侧：契约随 Program 写进 `workflow.json`。
- Worker 侧：`RegisterActivities` 把契约放进注册表，Worker `Start` 时随路由上报。
- Server 对照两次，比较逻辑都在 `server/biz/domain/dispatch/activity_contract.go`；Worker 注册与定义登记阶段不做这项对照。
  - 调度时（`server/biz/service/engine/server_definition.go` 的 `resolveActivityCommand`）：确认至少一个在线 Worker 的上报与声明一致。全部在线 Worker 都不一致时 Run 记 `ConfigError` 并暂停派发 Workflow Task。
  - 领取时（`server/biz/service/engine/server_activity.go` 的 `RefuseIncompatibleActivityClaim`）：任务投到队列，不带 build，队列上任一 build 都能领到；开始 Attempt 之前按领取者这个 BuildID 的上报对照 Run 当前 Program 的声明。不一致时 `BeginActivityTask` 返回 `worker_contract_mismatch`：Attempt 不开始、不消耗重试次数，任务在 Workflow Task 失败退避的首个间隔（缺省 1s）之后重新投递。

`ConfigError` 的消息：

```text
activity "order.charge" contract mismatch on worker "pay-1" (namespace "orders" queue "payments" build "pay-v2"): input expected "wire:ab12…" actual "wire:cd34…"
```

`expected` 是 Program 的声明，`actual` 是 Worker 的上报；字段是 `version`、`input`、`output` 之一。

### Activity 改动的分类

| 改动 | 写法 | 契约变化 |
| --- | --- | --- |
| 内部实现，结构与语义不变 | 只改实现 | 无 |
| 输入或输出加字段 | 加字段，`version` 保持不变；实现把零值当「未提供」 | 指纹改变 |
| 语义改变，结构不变 | 升 `@Activity(version=...)` | `version` 改变 |
| 破坏性改动（字段改名、改类型、删除）且有 Run 跨越这次改动 | 新 `type`（如 `order.charge-v2`）+ Workflow 里 `GetVersion` 分支；新旧实现都注册 | 新 type 是一份新契约，旧 type 不变 |

契约是相等对照：加字段也让新旧 Program 与新旧 Worker 互不匹配，所以新旧并存期间两个 build 都要在线。

## BuildID

版本是 Run 的属性（`policy` 与 Program），单个 Activity 不固定 build。

- `worker.Options.BuildID` 标识 Worker 的代码版本；Server 按 `namespace + activity type + queue + BuildID` 保存每个 build 上报的契约。
- 新旧 build 并存时 BuildID 必须不同：相同 BuildID 的登记互相覆盖，Server 无法区分两份契约。
- Activity 任务不带 build。首次投递、重试、恢复重派、Worker 停止交还都只按队列投递（Run 固定了 `target_worker_identity` 时只交给这个 Identity），队列上任一 build 都能领取，契约在领取时校验（见上节）。
- `workflow.ActivityOptions` 没有 `BuildID` 字段：Go 源码里写它编译不过；手写产物里写它时 Workflow Task 以未知字段失败。
- `ActivityStarted` 的 `workerIdentity` / `workerBuildId` 记录实际执行者，只用于观测。
- `ActivityWorkerVersionDefault` / `ActivityWorkerVersionWeight` 只在 Activity 没有指定队列、多个队列都承接它的类型时参与选择队列，不决定由哪个 build 执行。
- 运维规则：旧 build 下线前先排空只有它满足契约的 Activity（`pinned` 在旧 Program 上的 Run）。旧 build 已经下线时这些任务留在队列里，新 build 每次领取都被拒绝，直到旧 build 回来或 `ScheduleToStartTimeout` 到期。契约不变的发布（只改实现）没有这个限制：旧 build 下线后重试与排队中的投递由新 build 领取。

## 发布顺序

基准：能执行新契约的 Activity Worker 先上线，新 Program 后上线，旧 build 最后下线。

Worker `Start` 内部的顺序是「上报路由与契约 → 登记定义（新内容成为 latest）→ 开始 poll」，所以一个进程同时带 Activity 与 `Workflows` 时，单进程内的顺序已经正确。多个进程、多个 build 时：

1. 部署新 build 的 Activity Worker（新 BuildID）。Program 需要晚于它上线时，这一步让 `Workflows` 为空。
2. 确认新 build 就绪（`WaitReady`、`ListWorkers`）。
3. 发布新 Program：启动携带新 `Workflows` 的 Worker，或 `generate -url ... -publish`。
4. 观察：`config_error`、`upgrade_error`、Blocked 的 Run、Workflow Task 失败。
5. 旧 Program 上的 `pinned` Run 全部结束后下线旧 build。

各类 Run 的表现：

| Run | 新 Program 发布后 |
| --- | --- |
| 新启动的 Run | 用新 Program，Activity 由契约一致的新 build 执行 |
| `auto_upgrade` 的存量 Run | 下一个 Workflow Task 边界换到新 Program 并重放；通过后由新 build 执行 |
| `pinned` 的存量 Run | 留在旧 Program，Activity 由契约一致的旧 build 执行；新 build 领到时被拒绝，任务约 1s 后重新投递 |

## 升级失败的两种状态

### UpgradeError（不阻塞，自动重试）

`auto_upgrade` 的 Run 换到新 Program 后重放失配：

```text
auto upgrade to <version> failed at workflow task <N>: non_deterministic_error: <detail>; retrying with backoff
```

- Run 的内容保持切换前的版本，状态仍是 `running`，按退避重试升级；重试成功后 `upgrade_error` 清空。
- 处理：发布兼容的内容（补上 `GetVersion` 分支），下一次重试自动接手；或 `SetRunPolicy(..., "pinned")` 让它留在旧内容（旧内容需要的 Activity 仍要有 Worker）。

### ConfigError（暂停，等待人工）

Program 声明的契约与全部在线 Worker 不一致，`auto_upgrade` 换版前发现 History 里的载荷读不回 latest 的类型（`auto upgrade to <version> refused: payloads recorded in history ...`，见「换版前的载荷检查」），或准备 Workflow Task 时遇到每次都会重现的失败（`config_error` 以 `workflow task cannot be prepared: ` 开头，例如 Run 采用的 Program 版本读不到，见 `operations.md`）：

- Run 状态仍是 `running`，Server 停止为它派发 Workflow Task，`config_error` 保存原因。
- 典型成因：`pinned` Run 的旧 build 已经下线；新旧 build 用了同一个 BuildID；Worker 与 Program 来自不同次生成。
- 处理二选一：让契约一致的 build 重新上线，然后 `SetRunPolicy(..., "pinned")` 重新调度；或 `SetRunPolicy(..., "auto_upgrade")` 换到 latest（新 Program 要能重放已有 History）。两者都清除 `ConfigError`。

读取这两个字段：`c.DescribeWorkflowExecution(...)` 返回的 `ConfigError` / `UpgradeError`，HTTP `get-run` 响应里的 `run.config_error` / `run.upgrade_error`。

## 发布前验证

- 用旧 Program 产生的 History 重放新 Program：`docs/examples/versioned-workflow/` 里有测试样板。
- 回滚方向同样验证：新 History 重放旧 Program。
- 对照 `SKILL.md` 的「升级决策」表确认每类 Run 的预期。
