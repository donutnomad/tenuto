---
title: 问答
description: 按提问顺序记录的引擎行为问答：Activity 契约与升级、Workflow 版本标签、GetVersion、Reset、幂等 Key 与取消、生成产物、Activity 队列与路由、remote 契约、BuildID
status: Stable
---

# Tenuto QA

按提问顺序记录对引擎行为的问答。每条答案以代码为准，文中给出核对过的位置。第一次读先看问题 0，再按主题找。

## 按主题索引

| 主题 | 问题 |
| --- | --- |
| 整体结构 | [0](#问题-0-tenuto-由哪几部分组成-一个-run-是怎么跑完的) |
| 生成与部署 | [12 生成产物](#问题-12-生成器产出哪些-json-为什么先用-go-定义-再生成-json-最后用-json-注册到-server)、[13 独立 Server](#问题-13-要单独运行一个-tenuto-server-吗)、[21 谁执行 Workflow Task](#问题-21-workflow-task-由谁执行-server-内嵌的-worker-还是业务进程) |
| 版本与升级 | [1](#问题-1-activity-注解里的-type-和-version-有什么作用-同一个-type-把-version-从-v1-改到-v2-有什么影响)–[9](#问题-9-workflow-version-只是给人看的吗-可以不改-version、只改内部编排并用-getversion-做兼容吗) |
| 队列与路由 | [14](#问题-14-activity-的-queue-由谁决定-activity-不写-queue-会报错吗)–[19](#问题-19-remoteactivity-的作用是什么) |
| BuildID | [20](#问题-20-buildid-是什么-在哪里起作用、填在哪里、不填会怎样) |
| 可靠性与排障 | [10 幂等 Key](#问题-10-activity-因故障被重复调用-内部用什么构造幂等-key-同一个订单上有很多操作-key-不能只按订单-id)、[11 取消](#问题-11-取消一个-workflow-时-workflow-代码能收到通知并做补偿吗-正在执行的-activity-会怎样)、[22 Run 卡住怎么查](#问题-22-run-卡住了-blocked、暂停、一直不结束-按什么顺序排查)、[23 Activity 会不会重复执行](#问题-23-worker-崩溃或-server-重启时-activity-会不会重复执行) |

## 问题 0：Tenuto 由哪几部分组成？一个 Run 是怎么跑完的？

**Server 保存状态并调度，Workflow Worker 按定义重放 Workflow 代码、产生命令，Activity Worker 执行业务代码。Workflow 用 Go 写，生成器把它编译成定义（`workflow.json`）嵌进二进制，Worker 启动时登记给 Server。**

| 部分 | 职责 |
| --- | --- |
| tenuto-server | 保存 Event History 与执行状态，调度 Activity、Timer、Signal、Update，负责重试和超时（问题 13） |
| Workflow 定义 | `workflow.json`：编译后的 WLang 程序与它调用的 Activity 契约，按内容 hash 区分版本（问题 12） |
| Workflow Worker | 领取 Workflow Task，按 History 确定性地重放程序，产生下一批命令；Server 默认内嵌一个（问题 21） |
| Activity Worker | 领取 Activity Task，执行业务 Go 函数，上报结果；启动时上报自己拉取的队列、登记的 Activity 与契约（问题 14、20） |
| Client | 启动 Run，发送 Signal、Query、Update，读取结果 |
| Run | 一次执行：Workflow ID 是业务单号，绑定一份定义（`pinned` 固定、`auto_upgrade` 跟随最新，问题 6–9） |
| Namespace | 隔离 Run、定义、队列与 Worker 路由 |

一个 Run 的过程：

1. Client 用 Workflow Type（`@Workflow` 的 `type`）和 Workflow ID 启动 Run，Server 写入 History，产生第一个 Workflow Task。
2. Workflow Worker 领取它，按 History 重放程序；程序走到 `CallX` 时产生「调度 Activity」命令。
3. Server 为命令确定队列（问题 14），确认有在线 Worker 的契约满足定义的声明，把 Activity Task 投到队列，写 `ActivityScheduled`。
4. Activity Worker 领取任务，Server 再按领取者的契约核对一次，执行业务函数，结果写进 History。
5. 结果到达后产生下一个 Workflow Task，Workflow Worker 从头重放到新位置，继续产生命令，直到函数返回，Run 结束。

Workflow 代码会被反复重放，所以必须确定（问题 7）；Activity 不重放，执行语义是至少一次，所以必须幂等（问题 10、23）。

## 问题 1：`@Activity` 注解里的 `type` 和 `version` 有什么作用？同一个 `type` 把 `version` 从 v1 改到 v2 有什么影响？

注解形如：

```go
// @Activity(type="startup.say-hello",queue="startup",version=v1)
```

**`type` 是 Activity 的身份，决定任务派给谁、由哪个函数执行。`version` 是契约版本标签，服务端只拿它做相等对照：版本不一致的 worker 领取任务时被拒绝，不开始执行。`version` 不参与查找实现函数，服务端也不推断新旧版本是否兼容。**

### `type`

- **Worker 侧**：注册表按 `type` 存实现函数，同一个注册表里重复注册同一个 `type` 会报错（`sdk/activity/registry.go`）。
- **服务端**：调度时路由目录按 `namespace + type` 找在线 worker，确定任务投到哪个 queue，并确认至少一个在线 worker 的契约与声明一致（`server/biz/domain/dispatch/activity_routes.go`）。这个 `type` 没有任何在线 worker 时返回 `ErrActivityRouteUnavailable`。任务只按 queue 投递，不带 build；worker 开始执行前，服务端按这个 worker 自己的上报再对照一次契约（`server/biz/domain/dispatch/activity_contract.go` 的 `CheckClaimingWorkerContract`）。
- **Workflow 侧**：编译产物里每个 Activity 调用都带着 `type`，调度命令靠它寻址。

### `version`

注解生成的契约有四个维度：`type`、`version`、`input`、`output`，后两个是入参和出参结构的 wire 指纹（`protocol/activity_contract.go`）。

同一条注解一次生成两份产物：

- **Worker 上报侧**：`RegisterActivities(registry)` 把实现和契约一起登记进注册表，worker 启动时上报注册表里的全部契约。
- **Workflow 声明侧**：`workflow.json` 的 `activities` 字段随 Program 一起编译，表示这个 Workflow 期望的契约。

服务端逐维度做相等对照两次：调度 Activity 时对照在线 worker 的上报（`server/biz/service/engine/server_definition.go` 的 `resolveActivityCommand`），worker 开始执行前对照领取者自己的上报（`server/biz/service/engine/server_activity.go` 的 `RefuseIncompatibleActivityClaim`），比较逻辑在 `server/biz/domain/dispatch/activity_contract.go`：

| 情况 | 结果 |
|---|---|
| Workflow 没声明契约 | 不对照，照常派发 |
| worker 没有上报 Workflow 声明的维度 | 判为不一致，任务不派发 |
| 同一 `type` 有多个在线 build | 任务投到 queue，任一 build 都可能领到；契约不一致的领取被拒绝，任务约 1s 后重新投递 |
| 所有候选 worker 的上报都与声明不同 | 任务不派发，Run 记下 `ConfigError` 并暂停，等人工修复 |

### 把 `version` 从 v1 改到 v2

注解改成 v2 后重新生成并发布，新 Program 声明 v2，新 worker 上报 v2，两边一致。受影响的只有一类 Run：仍在用旧 Program（声明 v1）、且之后还要调度这个 Activity 的 Run。

v1 worker 下线后各类 Run 的结果：

| Run | 结果 |
|---|---|
| 新启动的 Run | 用最新 Program，声明 v2，正常派发 |
| 运行中、策略 `auto_upgrade` | 下一个 Workflow Task 边界切到最新 Program，声明变成 v2，正常派发 |
| 运行中、策略 `pinned`，后面还要调度这个 Activity | 声明仍是 v1，只有 v2 worker 在线，Run 暂停 |
| 运行中，这个 Activity 已经调度过 | 不受影响，对照只发生在调度那一刻 |

暂停时的报错是 `contract mismatch ... version expected "v1" actual "v2"`，写在 Run 的 `ConfigError` 上。"找不到 worker"是另一种情况：这个 `type` 完全没有在线 worker。

### 暂停后的恢复

暂停保留 history，可以恢复：

- 把 v1 build 的 worker 重新上线，然后对该 Run 执行 set-policy。set-policy 清除 `ConfigError`（`server/biz/domain/execution/upgrade_state.go` 的 `Run.OverridePolicy`），Run 继续推进。
- 把该 Run 的策略改成 `auto_upgrade`，它在下一个 Workflow Task 边界切到声明 v2 的 Program。前提是新 Program 能重放这条 Run 的已有 history。

### 升级做法

1. **兼容的改动保持 `version` 不变。** 入参出参结构变了有 `input` / `output` 指纹自动拦截，`version` 留给"结构没变但语义变了"的情况手动升。
2. **升 `version` 时新旧 build 并存一段时间**，等 `pinned` 的旧 Run 跑完再下线 v1 worker。一个 worker 进程里同一个 `type` 只有一个实现，v1 和 v2 并存靠两个 build 的进程同时在线。任务投到同一个 queue，两个 build 都可能领到；声明 v1 的任务被 v2 worker 领到时被拒绝，约 1s 后重新投递，最终由 v1 worker 执行，声明 v2 的反之。
3. **有长期运行的 `pinned` Run、又想只留一个 worker 进程**：新语义用新的 `type`（例如 `startup.say-hello-v2`），两个实现注册在同一个进程里。

### 核对范围

`auto_upgrade` 一行核对的是选版规则（`server/biz/domain/execution/upgrade_state.go` 的 `(*Run).ChooseProgramVersion`）。声明版本变化后的 history 重放是否通过，没有用测试用例验证。

## 问题 2：Activity 的输入和输出结构都变了，是不是只能新建一个 Activity、换一个新的 `type`？

**新 `type` 不是唯一做法。沿用原 `type` 也能升级，条件是新旧两个 build 的 worker 并存到旧 Run 跑完。运行中的 Run 跨越这次改动时，新 `type` 是唯一安全的做法。**

### 结构变化在引擎里的表现

- `input` / `output` 指纹按 wire 结构计算：JSON 字段名、字段类型、可选性、嵌套结构（`program/wire.go`）。增加一个可选字段也会让指纹变化。
- 服务端只做相等对照，没有"向后兼容"这一档。结构变化的效果与问题 1 里手动升 `version` 相同，区别是它自动发生。
- 重放只对照 ActivityID 和 ActivityType（`internal/executor/replay_match.go` 的 `commandMatchesEvent`），输入输出的内容和结构不参与重放对照。

### 沿用原 `type` 时各类 Run 的结果

| Run | 结果 |
|---|---|
| 新启动的 Run | 新 Program 与新 worker 的指纹一致，正常运行 |
| `pinned`，还没调度到这个 Activity | 声明的是旧指纹；旧 build 的 worker 在线就由它执行，只剩新 worker 时 Run 暂停并记 `ConfigError` |
| `auto_upgrade`，还没调度到这个 Activity | 下一个 Workflow Task 边界切到新 Program，按新结构调度，正常运行 |
| `auto_upgrade`，这个 Activity 已经完成 | 重放对照通过；history 里存的是旧结构的输出，新 Program 按新结构读取它 |

最后一行是风险所在：字段改名、改类型、删除后，新 Program 读旧输出得到的是缺失值或解码错误。重放对照不拦截这种情况。

### 选择

1. **只有短时 Run，或能等旧 Run 跑完**：沿用原 `type`，新旧 build 并存一段时间后下线旧 worker。
2. **有长时间运行的 Run 会跨越这次改动**：新建 `type`（例如 `order.charge-v2`），Workflow 里用 `GetVersion` 分支，旧 Run 继续调旧 `type`，新 Run 调新 `type`。两个实现注册在同一个 worker 进程里，旧 Run 的 history 始终由旧结构解释。
3. **只是加字段**：在引擎里与破坏性变更同等对待（指纹变化），做法同 1 或 2。

### 核对范围

"新 Program 按新结构读取旧输出"的具体表现（缺失值还是报错）没有用测试用例验证；已核对的是重放对照不包含输入输出，以及 history 中的输出原样进入重放状态（`internal/executor/interpreter/history_apply.go`）。

### 追问：新 Program 读旧输出，是不是违背了确定性？Temporal 检测到不一致会报错

**它违背的是数据兼容性，确定性检查的对象是命令序列。Temporal 在这一点上的行为与 Tenuto 相同。**

确定性的定义：同一段 history 重放出同一串命令。检查方式是把重放产生的命令逐条与 history 里的命令事件对照。

| | 对照内容 |
|---|---|
| Temporal | 命令种类、Activity ID、Activity Type 等标识 |
| Tenuto | 命令种类、ActivityID、ActivityType（`internal/executor/replay_match.go`） |

两边都不对照 Activity 的输入内容和输出结构。Temporal 里改了 Activity 返回的 struct，重放时旧 payload 由 data converter 解进新 struct，缺的字段是零值，多的字段被忽略，不产生 non-determinism 错误。

读旧输出之后有两种走向：

- **后续命令序列变了**（例如按新字段走了另一个分支，调度了不同的 Activity）：对照在分叉的那条命令处失败，报 `nondeterministic workflow: history event is ... replay command is ...`，Workflow Task 以 `non_deterministic_error` 失败。`auto_upgrade` 的候选重放失败时，Run 记下 `UpgradeError`，current 内容保持失败前的版本，按退避重试；发布兼容内容或改成 `pinned` 后恢复（`server/biz/service/engine/run_upgrade.go`）。
- **后续命令序列没变**（例如只是把这个值传给下一个 Activity 当输入）：对照通过，下游拿到零值或错误的值。Temporal 检测不到。Tenuto 的 `auto_upgrade` Run 在换版之前对照新旧 Program 解码 History 里已有载荷用的类型（字段删除、改名、改类型），读不回时不换版、以 `ConfigError` 暂停（[换版前的载荷检查](/develop/versioning#换版前的载荷检查)）；新增字段属于兼容改动，旧载荷照常取零值。

对 Tenuto 的 `auto_upgrade` Run，两种走向都只在新类型能读回旧载荷时出现：结构不兼容的改动在换版前就被拦下，不会走到重放。

所以确定性检查是兜底，拦得住"走了不同的路"，拦不住"走同一条路但数据错了"。跨版本的数据兼容由发布方保证，做法是问题 2 的选择 2：新 `type` 加 `GetVersion` 分支。

## 问题 3：同一个 `type` 的 Activity，input 只新增了一个字段（对旧调用方无影响），能正常兼容运行吗？有必要改 `version` 吗？

**数据层面兼容，引擎层面按不兼容处理。`version` 保持不变。**

### 数据层面

旧 Workflow 发来的 input 没有新字段，新 worker 用 JSON 解码后该字段是零值（`sdk/activity/convert.go`）。实现把零值当作"未提供"处理，旧调用就能得到正确结果。

### 引擎层面

- `input` 指纹对 record 的全部字段做哈希：JSON 名、类型、`omitempty`（`protocol/wire.go` 的 `WireFingerprint`）。新增一个字段，指纹就变。
- 对照是逐维度相等（`protocol/activity_contract.go` 的 `CompareActivityContracts`），引擎没有"新增可选字段算兼容"的规则。
- 结果与问题 2 的表格一致：新 Run 和 `auto_upgrade` 的 Run 正常运行；`pinned` 在旧 Program 上、且之后还要调度这个 Activity 的 Run，在只剩新 worker 时暂停并报 `contract mismatch ... input expected "wire:..." actual "wire:..."`。

### `version`

`version` 保持不变。`input` 指纹已经变化，起到了区分新旧契约的作用，再升 `version` 得到的是同一个结果。`version` 留给指纹覆盖不到的情况：结构相同、语义变了。

### 做法

1. 新旧 build 的 worker 并存，等旧 Program 上的 `pinned` Run 跑完再下线旧 worker。
2. 已经只剩新 worker、有 Run 暂停了：把这些 Run 改成 `auto_upgrade`。新增字段不改变 Workflow 的命令序列，重放对照通过，Run 切到新 Program 后按新指纹调度。

### 现状说明

"数据兼容但被引擎挡住"是当前相等对照的直接后果：引擎用它换来"声明与实现结构完全一致"的保证，代价是新增字段也要走一次新旧并存的发布流程。

### 追问：Temporal 对这种情况是怎么处理的？

**Temporal 不做这种对照，新增字段直接兼容运行。兼容性由发布方靠约定保证，服务端不参与。**

- 服务端不看结构：input 和 output 在服务端是不透明的 payload 字节，没有契约、指纹或版本对照。
- 按 task queue 派发：服务端不知道哪个 worker 实现了哪些 Activity Type。worker 拿到任务后在本地按 type 名查实现，查不到时这次 Activity Task 失败，按重试策略重试。
- 解码由 data converter 负责，默认是 JSON。旧 Workflow 发来的 input 缺新字段，解码后是零值；多出来的字段被忽略。
- 官方建议 Activity 的参数和返回值各用一个 struct，目的是以后加字段不破坏签名。

| | Temporal | Tenuto |
|---|---|---|
| 新增可选字段 | 直接运行 | 指纹变化，旧 Program 上的 `pinned` Run 需要旧 worker 并存 |
| 字段改名、改类型 | 服务端不拦，下游拿到零值或解码失败，Activity 运行时才暴露 | 调度前拦住，Run 暂停并给出明确的 mismatch 报错 |
| 结构没变、语义变了 | 靠发布方自己控制 | 手动升 `version` 拦住 |
| 新旧代码并存 | Worker Versioning（按 build 或 deployment version 把 Run 固定到某一版 worker） | build 并存，领取时按契约拒绝不一致的 worker |

Temporal 选的是宽松：兼容的改动零成本，不兼容的改动到运行时才发现。Tenuto 选的是严格：不兼容的改动在调度前被挡住，代价是兼容的改动也要走一次并存发布。

Temporal 的部分来自对它的了解，没有对照它的文档或源码核对；Worker Versioning 的具体形态以官方文档为准。

## 问题 4：只剩新 worker、旧 worker 已下线、Activity 契约不一致时，console 网页上显示报错还是暂停？能在网页上把 Run 改成 `auto_upgrade` 吗？

**显示为暂停，Run 的状态仍是 Running。可以在网页上直接改成 `auto_upgrade`。**

### 页面表现

Run 详情页顶部出现一条红色横幅（`console/src/lib/run-detail.ts` 的 `runConfigErrorBanner`）：

- 标题：`Configuration paused: a deterministic configuration or contract conflict stopped this run.`
- 说明：`Workflow task dispatch is paused until the conflict is fixed. ...`
- 详情：服务端记录的原文，即 `activity "..." contract mismatch on worker "..." ... input expected "wire:..." actual "wire:..."`

Run 不进入 Failed。服务端停止为它派发 Workflow Task（`server/biz/service/engine/server_delivery.go`），history 保留。

### 恢复入口

横幅里有两个按钮，Actions 下拉菜单里有同样的两项（`console/src/app/[namespace]/runs/[runID]/page.tsx`）：

| 按钮 | 效果 |
|---|---|
| `Resume with latest (AutoUpgrade)` | 策略改为 `auto_upgrade`，清除暂停，下一个 Workflow Task 切到最新 Program |
| `Pin current content` | 策略改为 `pinned`，清除暂停，继续用当前 Program |

两个按钮都调用 `/set-policy`。服务端清除 `ConfigError` 并立即重新调度一个 Workflow Task（`server/biz/service/engine/run_upgrade.go` 的 `SetRunPolicy`）。2026-11-10 起，`auto_upgrade` 的恢复依赖这次恢复开启一个全新的 Workflow Task：`SetRunPolicy` 先以 `WorkflowTaskFailed`（`cause=run_resumed`）关闭暂停时仍然打开的那个 Workflow Task，新任务开始时才重新选择 Program。

### 恢复后的结果

- 点 `Resume with latest`：最新 Program 声明的指纹与新 worker 一致，Run 继续运行。最新 Program 仍是旧声明时，调度到这个 Activity 会再次暂停。
- 点 `Pin current content`：Run 仍按旧 Program 的声明调度，只剩新 worker 时再次暂停。这个按钮用于旧 worker 已经重新上线的情况。
- 切到新 Program 后重放与 history 不一致：出现黄色横幅显示 `UpgradeError`，Run 按退避重试，内容保持切换前的版本。

### 默认策略

Workflow 定义没有指定策略时，Run 启动时取 `auto_upgrade`（`run_upgrade.go` 的 `applyRunPolicyDefaults`）。默认配置下的 Run 在下一个 Workflow Task 边界自动跟到最新 Program，问题 1 到 3 里"`pinned` 的旧 Run 暂停"只发生在定义或 Run 显式设为 `pinned` 的情况。

### 核对范围

以上来自 console 与服务端代码，没有在浏览器里实际复现这个场景。

## 问题 5：新 Program 可以晚于新 worker 上线吗？中间这段时间任务继续调度到旧 worker 上

**可以。满足三个条件：旧 worker 在这段时间保持在线；新旧 worker 的 BuildID 不同；新 worker 启动时不携带新的 Workflow 定义。中间这段时间新 worker 也会领到旧契约的任务，领取被拒绝后任务重新投递给旧 worker。**

### 中间这段时间的调度

线上的 Run 用旧 Program，声明旧契约。任务投到 queue，新旧两个 build 都在 poll。契约不变的发布里新旧 worker 都能执行它。契约变了时，新 worker 领到旧契约的任务会被拒绝（`server/biz/domain/dispatch/activity_contract.go` 的 `CheckClaimingWorkerContract`）：attempt 不开始、不消耗重试次数，任务约 1s 后重新投递，最终由旧 worker 执行。每次被拒绝让这个任务晚约 1s。

新 Program 上线后：

- 新启动的 Run 和 `auto_upgrade` 的 Run 声明新契约，由新 worker 执行。
- 仍在旧 Program 上的 `pinned` Run 继续由旧 worker 执行，跑完后旧 worker 下线。

### 三个条件

1. **旧 worker 保持在线。** 旧 worker 下线后，还没调度的旧声明只剩不一致的候选，Run 暂停（问题 4）；已经调度的任务留在队列里，被新 worker 反复拒绝，直到旧 worker 回来或 `ScheduleToStartTimeout` 到期。
2. **新旧 worker 的 BuildID 不同。** 路由目录按 `namespace + type + queue + BuildID` 标识一条路由（`activity_routes.go` 的 `sameActivityRouteVersion`）。BuildID 相同时，后上报的那条覆盖先上报的，目录里只剩一份契约；领取时的对照也按 BuildID 取上报，引擎分不出新旧。
3. **新 worker 启动时不携带新的 Workflow 定义。** worker 在开始 poll 之前登记随二进制发布的 Workflow 定义（`internal/worker/worker.go` 的 `registerWorkflows`），新内容首次出现即激活为 latest（`server/biz/usecase/definitions/usecase.go` 的 `EnrollWorkflowInNamespace`）。新 worker 的 `Workflows` 选项里带着新定义时，它一启动新 Program 就上线了。要分两步发布，新 worker 用纯 Activity 角色部署、`Workflows` 留空，新 Program 之后由 Workflow worker 或登记接口发布。

### 登记时的校验

登记 Program 时只检查路由是否存在，缺路由只记告警，契约对照留到调度时（`activity_routes.go` 的 `ValidateWorkflowConfigInNamespace`）。所以反过来的顺序也成立：新 Program 先上线、新 worker 后上线，期间调度到这个 Activity 的新 Run 会因为只有旧 worker 而暂停，新 worker 上线后在 console 点恢复。先上 worker 再上 Program 是不产生暂停的顺序。

### 追问：worker 带着新的 Workflow 定义一起上线，有没有问题？

**没有问题，这是默认的发布方式。新 Program 和新 worker 的契约同时生效。**

worker 的启动顺序（`internal/worker/worker.go` 的 `Start`）：

1. 上报 Activity 路由和契约。
2. 登记随二进制发布的 Workflow 定义，新 Program 激活为 latest。
3. 开始 poll。

路由先于 Program 生效，所以新 Program 激活的那一刻，声明新契约的任务已经有一致的 worker 在线。第 2 步到第 3 步之间派出的任务在队列里等这个 worker 开始 poll。

上线后各类 Run 的去向：

| Run | 去向 |
|---|---|
| 新启动的 Run | 新 Program，由新 worker 执行 |
| `auto_upgrade` 的运行中 Run（默认策略） | 下一个 Workflow Task 边界切到新 Program，由新 worker 执行 |
| `pinned` 在旧 Program 上的 Run | 继续由旧 worker 执行 |

两点注意：

- 旧 worker 留到旧 Program 上的 `pinned` Run 跑完再下线；线上没有 `pinned` 的 Run 时，旧 worker 可以随新 worker 就绪后下线。
- 旧版本的 worker 进程重启不会把 latest 改回旧 Program：已登记过的内容重复出现时 latest 保持不变（`EnrollWorkflowInNamespace`）。

Workflow worker 与 Activity worker 是两个独立部署时，带新定义的 Workflow worker 先上线就成了问题 5 末尾"Program 先于 worker"的顺序，先发 Activity worker。

---

# 第二阶段：Workflow

## 问题 6：`@Workflow` 注解的 `version=xxx` 有什么作用？编排没变，只改 `version` 会怎样？

`version` 是作者写的发布标签。它写进生成的 Workflow 配置（`compiler/sdkgen/program.go` 的 `result.Workflow.Version`），随配置一起计算内容版本（`definition.WorkflowConfigVersion`：配置规范化 JSON 的 sha256）。重放时比较的是命令序列，标签不参与比较。

### 只改 `version` 的后果

配置内容多了一个不同的字段值，内容版本随之改变，服务端把它当作一份新内容：

- **登记**：带着新标签的 worker 启动时登记这份配置，`(Namespace, Workflow ID, hash)` 是新的，latest 指针指向它。
- **新启动的 Run**：使用新内容。
- **运行中的 `auto_upgrade` Run**：在下一个 Workflow Task 边界换到新内容，用它重放已有 History。编排相同，命令序列相同，重放通过，Run 继续执行。
- **运行中的 `pinned` Run**：留在启动时的内容。worker 按任务携带的版本向服务端读取对应配置（`GetWorkflowConfig(ctx, runID, programVersion)`），所以新 worker 也能执行旧内容；旧内容用到的 Activity 仍要有 worker 提供。
- **sticky 缓存**：缓存按 `WorkflowType@内容版本` 建键，换版本后的第一个 Workflow Task 做一次完整重放。

### 用法

`version` 适合用来给一次发布打上可读的名字，在 console 和配置列表里区分内容。它本身不提供兼容性保证：编排有变化时，兼容性靠 `workflow.GetVersion` 分支或 `pinned` 策略。编排没变时改它是安全的，代价是多一份登记内容和一次完整重放。

### 追问：什么是"下一个 Workflow Task 边界"？

Workflow Task 是服务端派给 worker 的一次"推进编排"的任务：worker 把 Workflow 代码从头重放到当前位置，再往前执行到下一个等待点，把这一段产生的命令交回服务端。Workflow 代码每次只跑一小段，跑到必须等外部结果的地方就停下；等的东西到了，服务端再派一个新的 Workflow Task。

```go
func OrderFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
    a := workflow.ExecuteActivity(ctx, Charge, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}).Get(ctx)   // 等待点 1
    workflow.Sleep(ctx, time.Hour)                            // 等待点 2
    b := workflow.ExecuteActivity(ctx, Ship, a, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}).Get(ctx)      // 等待点 3
    return b, nil
}
```

| WFT | 触发事件 | worker 这次做了什么 | 交回的命令 |
| --- | --- | --- | --- |
| 1 | Run 启动 | 从头执行到等待点 1 | 调度 Activity `Charge` |
| 2 | `Charge` 完成 | 重放到等待点 1，拿到结果，继续到等待点 2 | 启动 1 小时 Timer |
| 3 | Timer 到期 | 重放到等待点 2，继续到等待点 3 | 调度 Activity `Ship` |
| 4 | `Ship` 完成 | 重放到等待点 3，函数返回 | 完成 Workflow |

触发新 Workflow Task 的事件：Run 启动、Activity 完成或失败、Timer 到期、收到 Signal、收到 Update、子 Workflow 结束。

"边界"是两个 Workflow Task 之间的间隙。在 Timer 等待期间发布新版本时：WFT 2 已用旧内容提交；Timer 到期后服务端派 WFT 3，查到 latest 是新内容，让 WFT 3 带上新版本；worker 用新 Program 从头重放，发出的"调度 `Charge`"与 History 对上后直接取用记录的结果，Activity 不会重新执行；追上 History 后继续执行新的部分。一个 Workflow Task 执行期间版本固定。

重放通过的条件是新代码发出的命令与 History 已记录的逐条一致。只改 `version` 标签时命令序列不变。新代码把 `Charge` 换成别的 Activity 时，重放到等待点 1 的命令与 History 不符，服务端报 `non_deterministic_error`，Run 留在旧版本并带退避重试；这类改动用 `workflow.GetVersion` 分支兼容。

## 问题 7：`GetVersion` 怎么用、怎么实现兼容？数字从哪来？原来没有 `GetVersion`，现在调用并写了 10，下次拿到的就是 10 吗？

数字由作者自己定。只有"新执行第一次走到这一行"时才会把值写进 History；已经走过这一行的旧 Run 不会被写入，重放时一直得到 `workflow.DefaultVersion`（`-1`）。

```go
v := workflow.GetVersion(ctx, changeID, minSupported, maxSupported)
```

- `changeID`：改动点的名字。
- `minSupported`、`maxSupported`：这份代码能处理的版本范围；惯例是第一次改动用 1，之后每次加 1。
- `workflow.DefaultVersion`（`-1`）：这段历史由加入 `GetVersion` 之前的代码写出。

| 情况 | 返回值 | 是否写入 History |
| --- | --- | --- |
| 新执行第一次走到这一行 | `maxSupported` | 写入 Marker |
| History 里已有该 `changeID` 的 Marker | Marker 记的值 | 否 |
| 重放时 History 已越过这一点、没有 Marker | `-1` | 否 |

对 `GetVersion(ctx, "x", workflow.DefaultVersion, 10)`：

- 发布之后新启动的 Run：返回 10 并记入 History，之后每次重放都返回 10。
- 发布之前已经跑过这个位置的旧 Run：返回 `-1`，不写入，之后仍是 `-1`。
- 发布之前启动、还没跑到这个位置的旧 Run：第一次走到这行时返回 10 并写入 Marker。

### 兼容性的来源

用返回值分支：旧 Run 得到 `-1` 走旧代码路径，发出的命令与它的 History 一致，重放通过；新 Run 得到新版本号走新路径。同一份代码同时服务两批 Run。

```go
v := workflow.GetVersion(ctx, "add-fraud-check", workflow.DefaultVersion, 1)
if v == workflow.DefaultVersion {
    charged, err = CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
} else {
    if _, err := CallFraudCheck(ctx, FraudInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil {
        return OrderOutput{}, err
    }
    charged, err = CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
}
```

### 演进与收尾

- 同一个点再改：复用 `changeID`，提高 `maxSupported`，增加对应分支。
- 另一个改动点：使用新的 `changeID`。
- 删除旧分支：确认没有 Run 依赖旧版本后提高 `minSupported`。
- 返回值落在 `[minSupported, maxSupported]` 之外时，Workflow Task 以非确定性错误失败（例如回滚到 `maxSupported` 较小的旧代码）。

实现位置：`internal/executor/interpreter/host_sideeffect.go` 的 `GetVersion` 与 `validateVersion`。

### 追问：`minSupported` 是默认值吗？上次存入 10，这次 `maxSupported` 写 11，取出来是 10 吗？

`minSupported` 是下限，只用于校验：取出的版本小于它时，Workflow Task 以非确定性错误失败。它不被写入，也不作为返回值。History 里没有 Marker 时返回的固定是 `workflow.DefaultVersion`（`-1`），与 `minSupported` 的取值无关。

代码从 `GetVersion(ctx, "x", -1, 10)` 改为 `GetVersion(ctx, "x", -1, 11)` 后：

| Run 的情况 | 返回 |
| --- | --- |
| History 里记了 `x=10` | 10 |
| 加 `GetVersion` 之前就跑过这里，没有 Marker | `-1` |
| 这次发布之后第一次走到这里 | 11，并记入 History |

这份代码需要 `-1`、10、11 三个分支。

删除旧分支时提高 `minSupported`：确认没有 Marker 的老 Run 都已结束后改为 `GetVersion(ctx, "x", 10, 11)` 并删掉 `-1` 分支。此后仍有老 Run 被重放时，它得到 `-1`，小于下限，报非确定性错误。`minSupported` 把"这份代码已经不认识这么老的历史"变成明确的失败。第一次给已上线的代码加版本点时，`minSupported` 写 `workflow.DefaultVersion`。

### 追问：第一个字符串参数是 ID 吗？Temporal 也有吗？

是改动点的 ID。Temporal 的 `workflow.GetVersion(ctx, changeID, minSupported, maxSupported)` 有同样的参数，Tenuto 的签名与它一致。

Marker 记录一对值：`changeID` 和版本号；重放时按 `changeID` 查找。一个 Workflow 里的多处改动各用各的 ID，独立记录和取值：

```go
v1 := workflow.GetVersion(ctx, "add-fraud-check", workflow.DefaultVersion, 1)
v2 := workflow.GetVersion(ctx, "split-shipping", workflow.DefaultVersion, 1)
```

规则：

- 一处改动一个 ID，名字描述改动内容。
- 同一处继续演进时复用 ID，只提高 `maxSupported`。
- ID 写定后保持不变；改名后旧 Run 按新名字找不到 Marker，得到 `-1`。
- 同一个 Run 内对同一个 ID 多次调用返回相同的值，以第一次为准。

写入 Marker 时 `changeID-版本号` 同步记入搜索属性 `TemporalChangeVersion`，可以据此查出仍停在某个版本的 Run，用来判断旧分支能否删除。

### 追问：三个版本 a（无 `GetVersion`）、b（`GetVersion(ctx, "g", -1, 1)`）、c（`GetVersion(ctx, "g", -1, 2)`），同一个 Run 重放时拿到 `-1` 还是 1？

记下的值跟着 Run 的 History 走。Run 第一次走到这一行时是真实执行，就写入 Marker，之后在该 Run 内一直返回这个值；是重放时越过，就不写入，一直返回 `-1`。

| Run 的经历 | 版本 b 下 | 升到版本 c 后 |
| --- | --- | --- |
| ① 在版本 a 时已经跑过这个位置，之后才升到 b | `-1`，不写入 | `-1`，不写入 |
| ② 在版本 a 时启动、升到 b 之后才第一次跑到这个位置；或直接在 b 下启动 | 1，写入 `g=1` | 1 |
| ③ 升到 c 之后才第一次跑到这个位置；或直接在 c 下启动 | — | 2，写入 `g=2` |

第 ① 类不补写 Marker：History 在这个位置之后已有别的事件，补写会改变已有历史。

版本 c 的代码保留 `-1`、1、2 三个分支。Run 当前由哪份代码执行与 `GetVersion` 的返回值是两件事：第 ② 类 Run 由版本 c 的代码执行时仍返回 1，走的是 c 代码里对应版本 b 流程的分支。

### 追问：既然走过的位置值固定，Workflow 怎么升级？重放时 `GetVersion` 这一行也会执行，为什么不会与 History 对不上？

升级就是发布新代码。固定的只有已经执行过的那一步。以"扣款前加风控"为例：

```go
v := workflow.GetVersion(ctx, "g", workflow.DefaultVersion, 1)
if v == 1 {
    风控
}
扣款
发货
```

- 发布后创建的订单：得到 1，执行风控、扣款、发货。
- 发布前创建、还没走到扣款的订单：第一次走到这行，同样得到 1。
- 发布前已经扣款、在等发货的订单：得到 `-1`，跳过风控，继续发货。

`GetVersion` 的用途是让新代码发布后老 Run 不被破坏；新流程由新 Run 和还没走到那一步的 Run 执行。让已经过了那一步的 Run 重新按新流程走一遍是 Reset 的职责。

重放时这一行照常执行，函数内部分三种情况（`internal/executor/interpreter/host_sideeffect.go` 的 `GetVersion`）：

1. History 里有该 `changeID` 的 Marker：返回 Marker 的值。
2. 没有 Marker，当前是重放：返回 `-1`，不记录，不产生命令。
3. 没有 Marker，当前是第一次真实执行：返回 `maxSupported`，写入 Marker。

worker 持有 Run 的全部 History：代码还在走已有记录的那一段是重放，走到末尾之后是真实执行。`ExecuteActivity` 每次执行都产生一条命令并与 History 逐条比对；`GetVersion` 在重放时只查找自己的 Marker，不往命令序列里加东西。老 Run 重放出的命令序列与加 `GetVersion` 之前相同。

### 追问：重放时返回的 `-1` 是参数里传的还是内置的？写 `GetVersion(ctx, "g", 10, 11)` 会怎样？

内置常量 `DefaultVersion = -1`，与参数无关。`GetVersion(ctx, "g", 10, 11)` 下：

| Run | 拿到的值 | 结果 |
| --- | --- | --- |
| 老 Run，重放时越过这一行，没有 Marker | `-1` | 低于下限 10，Workflow Task 以非确定性错误失败并退避重试 |
| History 里记了 `g=10` | 10 | 正常 |
| 第一次真实执行到这一行 | 11，写入 Marker | 正常 |

错误文本：`workflow code removed support of version -1 for changeID "g". the oldest supported version is 10`。

示例里第三个参数写 `workflow.DefaultVersion`，含义是下限为 `-1`、没有 Marker 的老 Run 也能处理；它让内置的 `-1` 通过校验。第一次给已上线的 Workflow 加 `GetVersion` 时下限写 `workflow.DefaultVersion` 并保留旧流程分支；确认没有 Marker 的老 Run 都结束后再提高下限。

## 问题 8：Run 已经结束，再跑一遍时用的是新版本的 Workflow 吗？

`GetVersion` 的第三个参数是这份代码能处理的最低版本；写成 `workflow.DefaultVersion` 才能重放加入 `GetVersion` 之前的历史。

Tenuto 没有名为 rerun 的操作。再跑一遍有三种做法：

| 做法 | History | `auto_upgrade`（默认） | `pinned` |
| --- | --- | --- | --- |
| 用同一个 Workflow ID 重新 start | 全新 | latest | latest |
| Reset | 保留回退点之前的部分 | latest，用新代码重放保留的历史 | 原来的版本 |
| Workflow 级自动重试 | 全新 | latest | 原来的版本 |

- 重新 start：全新的 Run，启动时取当时的 latest，不继承旧 Run 的策略；`GetVersion` 都是第一次执行，返回 `maxSupported`。
- Reset：新 Run 继承内容版本与策略（`execution.NewResetRun`）。`auto_upgrade` 在下一个 Workflow Task 换到 latest 并重放保留的历史，新代码的 `GetVersion` 分支要能兼容这段历史；回退点之后是真实执行。`pinned` 始终用旧内容。
- 自动重试：新 attempt 继承版本与策略（`execution.NewRetryRun`），History 为空。`auto_upgrade` 在第一个 Workflow Task 换到 latest；`pinned` 继续用旧内容。

版本选择规则：`server/biz/domain/execution/upgrade_state.go` 的 `(*Run).ChooseProgramVersion`，每个 Workflow Task 派发前，运行中的 `auto_upgrade` Run 取 latest，其余取 Run 记录的版本。

## 问题 9：`@Workflow(version=...)` 只是给人看的吗？可以不改 `version`、只改内部编排并用 `GetVersion` 做兼容吗？

可以。`version` 是给人看的发布标签，不参与重放比较，也不决定 Run 用哪份代码。服务端识别新内容靠整份配置的内容 hash：编排变了，生成的 Program 变了，hash 随之改变，latest 指针指向新内容。兼容性由 `GetVersion` 分支和 `policy` 负责。

`version` 也算进了内容 hash。只改 `version`、编排不动，同样会产生一份新内容（见问题 6）。它与系统并非完全无关，它引起的变化是无害的。

每次发布顺手改一下 `version`（如 `v3`、`2026-11-fraud`），console 的配置列表里能看出哪份内容对应哪次发布；保持不变也不影响任何功能。

## 问题 10：Activity 因故障被重复调用，内部用什么构造幂等 Key？同一个订单上有很多操作，Key 不能只按订单 ID

Tenuto 保证 Activity 至少执行一次，不保证只执行一次，每个 Activity 都必须幂等。幂等 Key 标识的是"这一次操作"：Key = 操作名 + 这一次操作的身份。`charge_payment:<OrderID>` 这种只按操作对象取 Key 的写法粒度过粗，同一订单的第二次同类操作会被当成重复。

"这一次"的身份有三种来源：

### 1. 输入里已有的业务 ID（首选）

```go
key := "refund:" + in.RefundRequestID
key := "charge-installment:" + in.OrderID + ":" + strconv.Itoa(in.InstallmentNo)
```

退款单号、期数、发货批次、调用方传入的请求 ID。同一订单的多次操作各有各的 Key，同一笔操作被重复调用时 Key 相同。

### 2. 在 Workflow 里生成，随输入传给 Activity

```go
refund := RefundInput{RefundID: workflow.RandomUUID(ctx), PaymentID: in.PaymentID, AmountCents: amount}
err := workflow.ExecuteActivity(ctx, IssueRefund, refund, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}).Get(ctx, &out)
// Activity 内：key := "issue-refund:" + in.RefundID
```

`RandomUUID` 的值记在 History 里，重放和 Activity 自动重试得到同一个值；循环里每次调用各生成一个。

### 3. Workflow ID + Activity ID

```go
info := activity.GetInfo(ctx)
key := "send-receipt:" + info.WorkflowID + ":" + info.ActivityID
```

`ActivityID` 标识 Run 里被调度的这一个 Activity，同一个 Activity 的各次重试相同。没有在 `ActivityOptions` 里指定时它按调用位置编号，每个 Run 从头编号。

### 稳定性

| 场景 | 1 业务 ID | 2 Workflow 生成 | 3 `WorkflowID`+`ActivityID` |
| --- | --- | --- | --- |
| Activity 自动重试、Worker 交还后重新派发 | 稳定 | 稳定 | 稳定 |
| Workflow 重放 | Activity 不会被再次调用 | | |
| Reset，回退点在这次调用之前 | 稳定 | 回退点在 `RandomUUID` 之前时变 | 稳定 |
| Workflow 级重试 | 稳定 | 变 | 稳定 |
| Continue-As-New 后的下一代、复用 Workflow ID 的新 Workflow | 稳定 | 变 | 冲突：同一位置上的另一次操作得到相同的 Key |

故障重试三种都能挡住。Reset 或 Workflow 重试后，第 1、3 种得到原来的 Key，外部系统返回第一次的结果；第 2 种在回退点越过生成位置时产生新 Key，外部系统会真的再执行一次。第 3 种不能用于会 continue-as-new 或复用 Workflow ID 的 Workflow。资金这类不能重做的操作用第 1 种；没有业务 ID 时，让发起方在启动 Workflow 的输入里带一个请求 ID。`Attempt`、时间戳和 Activity 里生成的随机值每次执行都变，不能进 Key。完整说明见 [Activity 幂等](/best-practices/activity-idempotency)。

## 问题 11：取消一个 Workflow 时，Workflow 代码能收到通知并做补偿吗？正在执行的 Activity 会怎样？

**能。取消是交给 Workflow 代码的请求，语义与 Temporal Go SDK 相同：根 Context 被取消，代码继续运行，清理之后由返回值决定终态。Activity 只有在 Workflow 取消它所在的 Context 时才被请求取消，并经 Heartbeat 得知。**

### 送达

`CancelWorkflow` 写入 `WorkflowCancellationRequested` 并调度 Workflow Task。Worker 在这条事件之后、协程推进之前取消根 Context（`internal/executor/interpreter/executor.go` 的 `runDecisionBatch`），重放时在同一位置送达。取消一个作用域时，先按发起顺序取消它的 Activity、Timer 与子 Workflow，再按创建顺序取消派生的作用域（`host_context.go` 的 `cancelScope.cancel`）。

| 等待中的操作 | 命令与事件 | Workflow 看到 |
| --- | --- | --- |
| Activity | `RequestCancelActivity` → `ActivityCancelRequested`；未开始的同事务 `ActivityCancelled` | `WaitForCancellation: false` 立即返回取消错误；`true` 等 `ActivityCancelled`、完成或失败 |
| `Sleep` | `CancelTimer` → `TimerCancelled` | 立即返回取消错误 |
| 子 Workflow | `RequestCancelChildWorkflow` → `ChildWorkflowCancelRequested`，子 Run 收到取消请求 | `false` 立即返回取消错误；`true` 等子 Workflow 结束 |
| `Await` | 无 | 条件不满足时返回取消错误 |
| `WaitSignal`、Channel、`Select` | 无 | 不受影响；把 `ctx.Done()` 放进 Selector 一起等待 |

### 补偿与终态

```go
if workflow.IsCanceledError(err) {
    cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
    // 在 cleanupCtx 上调度补偿 Activity
    return err
}
```

清理发在 `NewDisconnectedContext` 上，它不随根 Context 取消。返回取消错误时 Run 为 `cancelled`，返回 `nil` 为 `completed`，返回其他错误为 `failed`；没有取消请求时返回取消错误同样是 `cancelled`（Temporal Server 的 `handleCommandCancelWorkflow` 不检查取消请求）。可运行的示例见 `docs/examples/cancellation`。

### Activity

Workflow 的取消请求本身不取消 Activity，Activity 照常开始，清理 Activity 因此能运行。Workflow 取消 Activity 后（`server/biz/domain/execution/cancellation.go`）：

| Activity 状态 | 结果 |
| --- | --- |
| 排队中或重试退避中 | 立即 `ActivityCancelled`，不再派发 |
| 已开始，Heartbeat 得知取消后返回 `ctx.Err()` 或取消错误 | Worker 上报取消确认，`ActivityCancelled` 带 details |
| 已开始，仍返回成功 | `ActivityCompleted`，完成优先 |
| 已开始，返回其他错误或超时 | 不再重试，`retryState` 为 `CancelRequested` |
| 已开始，不 Heartbeat | 收不到取消，运行到自己返回或超时 |

Terminate 仍然立即关闭 Run，不运行 Workflow 代码。完整说明见 [取消、超时与终止](/develop/cancellation-timeouts)。

## 问题 12：生成器产出哪些 JSON？为什么先用 Go 定义，再生成 JSON，最后用 JSON 注册到 Server？

**每个 Workflow 包只产出一个 JSON：`workflow.json`。它是 Server 接受的 Workflow 定义，包含编译后的 WLang 程序 `program`、调用的 Activity 契约 `activities`，以及 `type`、`name`、`policy`、`version`。生成的 `zz_<pkg>_artifacts_gen.go` 用 `//go:embed` 把它打进二进制，`WorkflowDefinition()` 在 Worker 启动时把它登记到 Server。**

| 产物 | 内容 | 运行时 |
| --- | --- | --- |
| `workflow.json` | WorkflowConfig：`program`、`activities`（符号 → `type`、`task_queue`、`version`、入参出参 wire 指纹）、`policy`、`version` | 嵌入二进制，Worker 启动时登记 |
| `zz_<pkg>_artifacts_gen.go` | embed `workflow.json`，导出 `WorkflowDefinition()` | 编译进二进制 |
| `zz_<pkg>_contract_gen.go` | `CallX` 调用桥、签名断言、`RegisterActivities` | 编译进二进制 |

先生成 JSON 的原因：

- **Server 执行 Workflow 时手里没有业务 Go 代码。** Workflow Task 可以由 Server 内嵌的 Worker 执行，Console 用浏览器里的 WASM 重放 Trace，它们都需要一份可解释的程序描述。
- **版本有据可查。** 定义按 `workflow.json` 的内容算 hash（`program.WorkflowConfigVersion`），Run 绑定的是一份具体的内容，`pinned` 和 `auto_upgrade` 都以它为依据。
- **部署单元是一个二进制。** JSON 嵌在二进制里，运行时不读外部文件，产物与二进制不会错配。
- **产物与源码一致由 CI 保证。** CI 执行 `go generate ./...` 后用 `git diff --exit-code` 检查产物已提交。

审阅时看 Go 源码；`workflow.json` 的 `activities` 一段可以用来确认契约变化。

## 问题 13：要单独运行一个 tenuto-server 吗？

**要。tenuto-server 是独立进程，保存 Event History 和执行状态，负责调度 Activity、Timer、重试和超时。业务进程是它的客户端，经 HTTP 连接；Server 里没有业务代码，Workflow 定义由各个 Worker 启动时登记上去。**

```bash
# 本地开发：内存后端，不需要数据库，重启后数据清空
WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:7400 go run ./server/cmd/tenuto-server

# 生产：MySQL 后端，首次部署与每次升级先 migrate
tenuto-server migrate && tenuto-server
```

只有测试会用 `server/app/assembly` 把 Server 组装进同一个进程，例如 `docs/examples/upgrade-demo/scenario/scenario_test.go`。

## 问题 14：Activity 的 queue 由谁决定？Activity 不写 queue 会报错吗？

**queue 是 Activity 自己的属性，和调用它的 Workflow、和放在同一个包里的 Workflow 都无关。不写 queue 不报错：这时由执行它的 Worker 决定，Server 调度时按在线 Worker 的登记选队列。**

Server 调度一个 Activity 时按顺序取队列，取到的队列写进 History 的 `ActivityScheduled.taskQueue`（`server/biz/service/engine/server_definition.go` 的 `resolveActivityCommand`）：

1. 调用处的 `ActivityOptions.TaskQueue`，只作用于这一次调用。
2. `@Activity(queue=...)`：写进 `workflow.json`，生成的 `RegisterActivities` 用 `activity.RegisterActivityOnQueue` 连同队列一起登记。
3. 都没写：定义不声明队列，Server 在路由目录里选。目录是在线 Worker 上报的「拉取的队列 × 登记的 Activity 类型」；先排除契约与定义不一致的路由，再优先默认路由（`ActivityWorkerVersionDefault`），再按权重（`server/biz/domain/dispatch/activity_routes.go` 的 `ResolveActivityRoute`）。

Workflow Task 不分队列，由 Server 派给任意 Workflow Worker。

Worker 一侧（`sdk/worker/worker.go` 的 `activityTaskQueues`）：

| `ActivityTaskQueues` | Worker 拉取的队列 |
| --- | --- |
| 不写 | registry 登记的队列（`reg.Queues()`）；有不带队列的 Activity 时再加上 `default` |
| 写了 | 原样使用；必须包含 registry 登记的每个队列，否则 `worker.New` 返回错误；可以额外列别的队列 |

不写 queue 时，只在调度时有两种情况让 Run 显示 Blocked（`activity_route_unavailable`），都不是写法错误：没有任何在线 Worker 登记这个 Activity；几个服务在互不相关的队列上提供它（见问题 17）。Worker 上线或声明队列后，Run 按退避继续，History 不变。

### 追问：Temporal 的 Workflow queue 会被所有 Activity 继承吗？

会。Temporal 的 Workflow 跑在一个必填的 task queue 上，Activity 的 `ActivityOptions.TaskQueue` 不写时投到 Workflow 所在的队列。Temporal Server 没有「哪个 Worker 登记了哪个 Activity」的目录：调用其他服务的 Activity 时，调用处必须写对方的队列名，写错时任务投到没有实现的队列，Worker 报 activity type not registered 后按重试策略重试。Temporal 为跨服务调用另外提供了 Nexus：endpoint 在 Registry 里登记，指向一个 namespace 和 task queue，调用方只需要 endpoint 名。

Tenuto 的对应做法：

| | Temporal | Tenuto |
| --- | --- | --- |
| Activity 不写 queue | 投到 Workflow 的队列 | 由执行它的 Worker 决定，Server 按登记选路 |
| 调用处覆盖 | `ActivityOptions.TaskQueue` | 同左 |
| 跨服务调用 | 调用处写对方队列名，或用 Nexus | `@RemoteActivity` 契约写在共享契约包里（问题 19），调用方用生成的 `CallX` |
| 队列上没有能执行的 Worker | 投递后反复重试 | 调度前检查，不投递，Run Blocked 并说明原因 |

## 问题 15：Worker 的 `ActivityTaskQueues` 写了 `queue1`、`queue2`，没写 queue 的 Activity 投到哪个？两个 Worker 分别写 `queue1, queue2` 和 `queue2, queue1` 呢？

**`ActivityTaskQueues` 是 Worker 拉取的队列列表，Worker 把 registry 里的每个 Activity 上报到每个队列上。没写 queue 的 Activity 进入名字排在最前面的队列，即 `queue1`；数组里写的顺序不影响结果。两个 Worker 顺序相反时结果相同，都投到 `queue1`，两个 Worker 都拉它，谁先领到谁执行。**

- 两条候选路由（`queue1`、`queue2`）由同一个进程拉取，属于同一组，不触发歧义检查（问题 17）。
- 选路在 `chooseActivityRoute`：有默认路由时在默认路由里选，没有默认、有权重时按调用的 hash 按权重分流，都没有时取第一个；排序按 BuildID、再按队列名（`sortActivityRoutes`）。
- 没写 queue 表示任何能执行它的 Worker 都可以，Server 只会选有 Worker 拉取并登记了这个 Activity 的队列，选哪个都能执行，影响的只是负载落在哪个队列。
- 一个 Worker 拉多个队列，用途是承接写明队列的任务：`@Activity(queue="queue2")`、调用处改派、迁移队列的过渡期。需要固定队列时在 `@Activity` 上声明。

## 问题 16：Activity 注解写了 `queue1`，调用处指定了 `queue2`，会发生什么？

**这一次调用投到 `queue2`：调用处的 `ActivityOptions.TaskQueue` 优先于注解，只作用于这一次调用（`internal/executor/interpreter/host_api.go` 先取 Program 的队列，再被调用选项覆盖）。其他调用仍投到 `queue1`。**

能否执行取决于 `queue2` 上有没有提供这个 Activity 的 Worker（`resolveExplicitActivityRoute`）：

| Worker | 结果 |
| --- | --- |
| 不写 `ActivityTaskQueues`，只拉 `queue1` | `queue2` 上没有路由；定义声明了契约时调度不派发，Run Blocked（`declares a contract and no online worker on task queue queue2 reported one`），不改投 `queue1`，有 Worker 拉 `queue2` 后按退避继续 |
| `ActivityTaskQueues: []string{"queue1", "queue2"}` | Activity 也在 `queue2` 上可执行，投到 `queue2` 并执行 |
| 另一个 Worker 拉 `queue2` 并登记了它 | 投到 `queue2`，由那个 Worker 执行；这是把部分调用交给专门 Worker 池的用法 |

注解上的 queue 是默认值，不限制 Worker 在别的队列上提供它。调用处的队列可能来自变量，生成期检查不到；Console 的 Run 详情里这次调度的 `Queue Source` 显示 `overridden at call site`。

## 问题 17：两个服务提供同名同契约的 Activity（例如误拷贝），会怎样？同一个服务部署 10 个实例算这种情况吗？

**10 个实例不算：它们拉同一个队列，任务投到这个队列，谁领到谁执行。两个服务同名时，结果取决于它们拉的队列和调用方有没有声明队列。**

Server 不判断「是不是同一份代码」：type、version、入参指纹、出参指纹都相同的 Worker 被视为可以互相替代。

| 情况 | 结果 |
| --- | --- |
| 调用方声明了队列（`@Activity(queue)` 或调用处） | 只在那个队列里选，另一个服务不影响 |
| 没声明队列，两个服务拉同一个队列 | 不报错，谁领到谁执行；与多实例部署无法区分 |
| 没声明队列，两个服务在互不相关的队列上，都是默认路由，或都既没有默认也没有权重 | 调度不派发，Run Blocked（`activity_route_unavailable`，`declares no task queue and workers serve it on unrelated task queues`），声明队列或停掉一边后按退避继续 |
| 没声明队列，只有一边是默认路由 | 投给默认路由那一边 |
| 没声明队列，没有默认路由、配置了权重 | 按权重分流，视为显式配置 |
| 两边契约不同（version 或指纹不同） | 调度与领取时按 Run 声明的契约区分，契约不符的 Worker 领取时被拒绝 |

「互不相关」按进程判断（`ambiguousRoutePools`）：同一个队列上的路由是一组；同一个 Worker 进程（Identity）拉取的几个队列是一组。BuildID 不参与，它可以为空，也可能被不同服务共用。Worker 登记本身不做这项检查：两个服务提供同名 Activity、调用方都声明了队列是正常用法。

避免误拷贝：Activity 的 `type` 加服务前缀（`payments.charge`）；Console Workers 页的 Activity Routing 表列出每个类型在哪些队列、由几个 Worker 提供。

## 问题 18：把一个 Activity 的队列从 `queue01` 改到 `queue02`，`queue01` 上的任务是不是没人处理了？

**是。改完后没有 Worker 拉 `queue01` 时，投在 `queue01` 的任务没人领取；不会丢失或重复执行，有 Worker 重新拉 `queue01` 后继续。受影响的范围取决于队列写在哪里。**

队列写在 `@Activity(queue)` 时，它是 `workflow.json` 的一部分，改队列就是发布一份新定义（内容 hash 变了）：

| Run | 之后调度这个 Activity 投到 | 新 build 只拉 `queue02` 时 |
| --- | --- | --- |
| 新启动的 | `queue02` | 正常 |
| `auto_upgrade` 在途 | 下一个 Workflow Task 边界切到新定义，投 `queue02` | 正常 |
| `pinned` 在途 | 旧定义，投 `queue01` | Blocked（`activity_route_unavailable`），按退避重试 |
| 改动前已投进 `queue01`、还没领取的任务 | 留在 `queue01` | 停在 Scheduled，直到有 Worker 拉 `queue01` 或 `ScheduleToStartTimeout` |

队列只在 Worker 的 `ActivityTaskQueues` 上配置时，定义里没有它，改队列不产生新定义，之后的调度（包括 `pinned` 的 Run）跟随 Worker 的登记；只有已投进 `queue01` 的任务留在原处。

过渡期让新 build 同时拉两个队列，等 `queue01` 清空后再去掉：

```go
worker.Options{
	Activities:         registry,                       // 注解登记的是 queue02
	ActivityTaskQueues: []string{"queue02", "queue01"}, // 显式列表可以多于 registry 登记的队列
}
```

## 问题 19：`@RemoteActivity(...)` 的作用是什么？

**声明一个实现不在本进程的 Activity，通常由另一个服务实现。它写在命名函数类型上，只描述契约。**

```go
// examples/shop/contracts/contracts.go
// @RemoteActivity(type="payments.charge",queue="payments",version=v1)
type ChargeRemote func(context.Context, ChargeInput) (ChargeOutput, error)
```

| | `@Activity` | `@RemoteActivity` |
| --- | --- | --- |
| 标注在 | 有函数体的实现函数 | 函数类型 |
| `CallX` 调用桥 | 生成 | 生成，Workflow 里写 `contracts.CallCharge(ctx, in)` |
| 标记符号 | 实现函数本身（`orders.Notify`） | 生成的 `ChargeRemoteActivity`，函数体只有 panic，Server 只按符号绑定到 `payments.charge` |
| `RegisterActivities` | 登记 | 不登记；只有 remote 契约的包不生成 `RegisterActivities`（`compiler/sdkgen/contracts.go` 的 `renderRegisterActivities`） |
| `workflow.json` 的契约 | type、queue、version、入参出参指纹 | 同左，指纹从函数类型的 `In`、`Out` 算出 |

实现方用同一组 queue、type、version 登记：

```go
activity.RegisterActivityOnQueue(registry, "payments", "payments.charge", "v1", g.Charge)
```

两边的 DTO 可以各自定义，JSON 字段名、类型与可选性一致时指纹相同；Server 在调度和领取时对照两边的契约。调用方的代码里不出现队列名和类型名字符串。

## 问题 20：BuildID 是什么？在哪里起作用、填在哪里、不填会怎样？

**BuildID（`worker.Options.BuildID`）标识进程跑的是哪个版本的代码，作用是让同一个 Activity 的不同契约版本同时保存在 Server 上、互不覆盖。它不决定任务投给谁。**

| | 标识 | 例子 |
| --- | --- | --- |
| Identity | 一个进程实例 | 不填时为 `<pid>@<hostname>`（`sdk/client/client.go`） |
| BuildID | 进程跑的代码版本 | Git commit、镜像 tag |

起作用的地方：

1. **路由目录按「队列 + 类型 + BuildID」保存契约**（`sameActivityRouteVersion`）。BuildID 不同时分开保存；相同时合并成一条，后登记的契约覆盖先登记的。调度时 Server 确认至少一条路由的契约满足 Run 的声明。
2. **领取时按领取者的 BuildID 找它上报的契约**（`activity_contract.go` 的 `CheckClaimingWorkerContract`），与 Run 的声明不一致时拒绝，任务约 1s 后重新投递。
3. **记录与展示**：`ActivityStarted` 的 `workerBuildId`，Console 的 Worker 列表。

它不决定任务投到哪个队列，不把 Run 绑定到某个版本（`ActivityState` 不保存 build），不参与 Workflow Task 的分派，也不参与问题 17 的歧义分组。

### 填在哪里

`worker.New` 的 `Options.BuildID`。不填时取编译时写入二进制的 Git revision：前 12 位，工作区有未提交改动时加 `-dirty`（`sdk/worker/worker.go` 的 `resolveBuildID`）。二进制里没有 revision 时 `worker.New` panic：

```text
worker: Options.BuildID is empty and the binary carries no VCS revision (built with go run, go test, -buildvcs=false, or outside a git checkout); set Options.BuildID or build the binary with go build inside the repository
```

`go run`、`go test`、`-buildvcs=false`、构建时没有 `.git` 目录（例如 Docker 构建没复制它）都不写入 revision，这些场景显式设置，例如从部署注入的环境变量读取。规则是同一份代码的所有进程用同一个值，代码变了就换值。

### 追问：两个进程都没写 BuildID、契约一致，会被当成同一份代码的多个副本吗？

Server 不判断是不是同一份代码，契约一致就视为可以互相替代，BuildID 填不填都一样。BuildID 只在新旧代码的契约不同、又同时在线时起作用：滚动发布时 v2 给 `reserve` 的入参加了字段，两批进程用不同 BuildID，路由目录保留两份契约，声明旧指纹的 `pinned` Run 仍能派给 v1 进程；用相同 BuildID 时两份契约合并成一份，`pinned` Run 可能被判为契约不符而暂停，即使 v1 进程在线。

### 追问：第二个进程改了 Activity 内部的代码顺序，会出问题吗？

不会出现确定性问题。需要确定性的是 Workflow 代码：它按 History 重放，命令顺序必须与 History 一致，改动要用 `GetVersion` 保护。Activity 不重放，History 只记它的入参和结果，内部实现可以随意重构。

发布期间新旧进程同时在线，一次调用的重试也可能分别跑在新旧代码上。结果含义没变时直接发布；结果含义变了而结构没变（例如运费从不含税改成含税），Server 看到的指纹不变，要手动升 `version`。

### 追问：BuildID 按 Git commit 填，Activity 的 `version` 是不是不用再改了？

还要改。领取时 Server 对照的是 Run 声明的 `version` 和领取者上报的 `version`，不看 BuildID；`workflow.json` 也不记录 BuildID。

| | BuildID | `version` |
| --- | --- | --- |
| 描述 | 进程的构建 | Activity 结果的含义 |
| 变化时机 | 每次提交，包括无关改动 | 只在语义变了、结构没变时手动升级；结构变了指纹自动变 |
| 调度与领取时对照 | 不对照 | 对照 |

只改 BuildID、不升 `version` 时，`pinned` 在旧定义上的 Run 调用 `quote`，新进程的契约与声明一致，可以领取，旧 Run 拿到含税运费，没有报错。升到 `v2` 后旧 Run 声明 `v1`，新进程上报 `v2`，领取被拒绝，任务只交给仍提供 `v1` 的进程。不按 BuildID 分配任务，是为了让契约兼容的新旧进程互相替代，不必让旧 Run 一直绑在旧二进制上。

## 问题 21：Workflow Task 由谁执行？Server 内嵌的 Worker 还是业务进程？

**两者都可以，默认由 Server 内嵌的 Workflow Worker 执行。Workflow Task 不分队列、不按应用划分，任何在线的 Workflow Worker 都能执行任何 Namespace、任何定义的 Workflow Task；它们执行的是定义里的 WLang 程序，不需要业务 Go 代码。**

| 方式 | 配置 | 示例 |
| --- | --- | --- |
| Server 内嵌（默认） | `WORKFLOW_WORKER_ENABLED=true`，并发 `WORKFLOW_WORKER_CONCURRENCY`（缺省 8） | `examples/shop`：三个服务都只跑 `worker.RoleActivity`，Workflow Task 全部由 tenuto-server 执行 |
| 独立 Workflow Worker | `WORKFLOW_WORKER_ENABLED=false`，部署方运行 `Role: worker.RoleWorkflow`（或 `RoleBoth`）的 Worker | `docs/examples/upgrade-demo`：app-a、app-b 用 `RoleBoth`，同时执行两类任务 |
| 两者同时 | 内嵌与独立 Worker 共同消费同一个 Workflow Task 队列 | — |

配置见 [部署 Tenuto Server](/deploy/server#内嵌-workflow-worker)。选择：

- 用内嵌：部署最简单，业务进程只跑 Activity Worker。
- 用独立 Workflow Worker：要把重放占用的 CPU 和 Sticky Execution Cache 的内存与 Server 分开、单独扩容。升级时先升级全部 Server，再升级独立 Worker。
- 关闭内嵌又没有启动独立 Worker 时，Run 停在等待 Workflow Task 的状态。

定义的登记与谁执行 Workflow Task 无关：任何 `Role` 的 Worker 都可以携带 `worker.Options.Workflows`，在启动时登记定义。shop 的 order-service 是 Activity Worker，它登记了 `place-order`。

## 问题 22：Run 卡住了（Blocked、暂停、一直不结束），按什么顺序排查？

**先看 Run 详情页顶部的提示属于哪一类：Blocked 和 AutoUpgrade 重试会按退避自动继续，修好原因即可；Configuration paused 不会自己恢复，要在页面上选择策略；什么提示都没有、Activity 停在 Scheduled，说明任务已经投递但没有 Worker 领取成功。**

| Console 上的提示 | 含义 | 会自动继续吗 | 处理 |
| --- | --- | --- | --- |
| Workflow Blocked 卡片（cause、reason、连续失败次数） | Workflow Task 连续失败，History 不变 | 会：退避从 1s 开始，最长 1 分钟（`server/biz/domain/execution/workflow_task.go`） | 按 cause 修复，见下表 |
| 红色横幅 `Configuration paused`（Run 的 `config_error`） | 确定性的配置或契约冲突，例如 `contract mismatch`，或 `auto_upgrade` 换版前发现 History 里的载荷在新类型下读不回 | 不会 | 横幅上的 `Resume with latest (AutoUpgrade)` 或 `Pin current content` 调用 set-policy 恢复（问题 4） |
| `AutoUpgrade retrying`（Run 的 `upgrade_error`） | `auto_upgrade` 的 Run 用最新定义重放旧 History 失败，仍留在上一份成功的内容上 | 会 | 发布兼容的定义（补 `GetVersion` 分支），或 set-policy `pinned` 留在旧内容（问题 7） |
| 无提示，Activity 长时间 Scheduled | 任务在队列里：没有 Worker 拉这个队列，或在线 Worker 的契约与声明不符、领取被拒（Server 日志 `activity claim refused`） | Worker 就绪后会 | Workers 页核对 Activity Routing 与 Build；契约不符时让契约一致的 build 上线 |
| Run 已 `failed` | Workflow 返回错误、执行预算超限等 | 不会 | 看 Failure 卡片，必要时 Reset（[Reset](/operations/reset)） |

Blocked 常见的 cause：

| cause | 原因 | 处理 |
| --- | --- | --- |
| `activity_route_unavailable` | 没有在线 Worker 提供这个 Activity；指定的队列上没有 Worker；没声明队列且几个服务在互不相关的队列上提供它（问题 14–17） | 启动对应 Worker，或声明队列 |
| `non_deterministic_error` | 代码产生的命令与 History 不同 | 恢复旧命令序列或补 `GetVersion` 分支后重新发布 |
| `bad_schedule_activity_attributes` | 调度 Activity 时没有关闭超时（选项来自变量，生成期检查不到） | 设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout` |
| `schedule_activity_duplicate_id` | `ActivityOptions.ActivityID` 与同一 Run 里仍未关闭的 Activity 重复 | 让并发调用用不同的 ID |

报错原文与处理的完整清单在 skill 的故障排查表（`docs/skills/tenuto/SKILL.md`）。Activity 一直重试、Run 不结束，通常是缺省 RetryPolicy 不限次数且没有设置 `ScheduleToCloseTimeout`（[RetryPolicy](/develop/activity-execution#retrypolicy)）。

## 问题 23：Worker 崩溃或 Server 重启时，Activity 会不会重复执行？

**会。Activity 的执行语义是至少一次：超时重试、执行完但结果没能上报都会让同一个业务操作再执行。每个 Activity 都要幂等（问题 10）。**

| 情况 | 结果 |
| --- | --- |
| Activity 返回错误 | 按 RetryPolicy 产生下一次 Attempt；缺省不限次数，间隔从 1s 起、倍数 2、上限 100s |
| Worker 在 Attempt 开始后崩溃，或执行完但结果没上报 | Attempt 停在 started，Start-To-Close（或 Heartbeat）超时到期后按 RetryPolicy 产生下一次 Attempt，**业务操作可能已经做过一次** |
| Worker 优雅停止 | 因停止而结束的 Attempt 交还 Server，以同一个 Attempt 序号立即重新派发，不消耗重试次数 |
| 已开始的 Attempt 在队列里多出一份副本 | 被领到时 Server 答复不执行，Worker 确认丢弃 |
| Server 重启（MySQL 后端） | 持久队列里的任务不会再入队一次；退避中的下一次 Attempt 仍按保存的 `nextAttemptAt` 派发 |
| Server 重启（内存后端） | 数据随进程清空，只用于开发 |

依据：[Activity 执行生命周期](/develop/activity-execution#投递与执行次数)。shop 示例演示了「扣款成功后响应丢失」：Tenuto 重试，payment-service 用订单号查到已有的 `pay_1001` 直接返回，只扣一次款（`examples/shop/payment/payment.go`）。

Workflow 代码不会因为重放而重复执行副作用：重放只重算命令，已经完成的 Activity 结果从 History 读回，不再调用。

