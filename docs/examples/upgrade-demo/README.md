# upgrade-demo：SDK 接入、注解写法与七个升级场景

三个独立进程通过 HTTP 协作，一个 driver 把 Activity 与 Workflow 的升级场景逐个跑一遍并逐项断言：

- **Server**：`tenuto-server`，Workflow 引擎与 HTTP API，不执行 Workflow Task。
- **app-a（履约）**：自己的 Workflow `return-flow` 与五到八个 Activity，三个发布版本 `appa/v1`、`appa/v2`、`appa/v3`。
- **app-b（订单）**：自己的 Workflow `order-flow` 与本地 Activity `orders.price`，用 `@RemoteActivity` 契约跨进程调用 app-a 的 Activity，两个发布版本 `appb/v1`、`appb/v2`。

Workflow Task 由 app-a 与 app-b 的 Worker 执行。一次实际运行的输出记录在同目录的 `RESULTS.md`。

## 运行

在项目根目录执行，不需要数据库：

```bash
./docs/examples/upgrade-demo/run-demo.sh
```

脚本生成 Workflow 产物、构建二进制、启动 `tenuto-server`，driver 再按场景启动和停止各个发布版本的 app-a / app-b 进程。全部断言通过时输出 `RESULT PASS`，退出码 0；任何一条断言失败时输出 `RESULT FAIL`，退出码 1；等待超时时附带该 Run 的 History。整个过程约 14 秒（不含构建）。

| 模式 | 命令 | Server |
|---|---|---|
| `memory`（默认） | `./docs/examples/upgrade-demo/run-demo.sh` | `tenuto-server`，`WORKFLOW_BACKEND=memory`，内存存储 |
| `mysql` | `UD_BACKEND=mysql UD_MYSQL_CNF="$HOME/.my.cnf" ./docs/examples/upgrade-demo/run-demo.sh` | `tenuto-server`，MySQL + TaskQ |
| 进程内测试 | `GOWORK=off go test ./docs/examples/upgrade-demo/... -count=1 -v` | 用 `server/app/assembly` 组装与 `memory` 模式相同的 Server，全部进程在一个测试进程里 |

两种后端按同一套规则投递 Activity Task：任务交给所在 Namespace、Task Queue 上的任一 Worker，与它的 BuildID 无关；Worker 开始执行前 Server 按它这个 build 上报的契约对照 Program 的声明，不兼容的领取被拒绝，任务约 1s 后重新投递。两种模式跑的是同一份断言。

`mysql` 模式从 mysql 客户端配置的 `[client]` 段读取 `host`、`port`、`user`、`password`，也可以用 `UD_MYSQL_HOST`、`UD_MYSQL_PORT`、`UD_MYSQL_USER`、`UD_MYSQL_PASSWORD` 覆盖。凭据经权限 `0600` 的临时客户端配置与进程环境传递，退出时删除临时配置。每次运行重建 `upgrade_demo_workflow` 与 `upgrade_demo_taskq`，`UD_DB_PREFIX` 修改前缀。

其他环境变量：`UD_API_PORT`（默认 7400）、`UD_LISTEN_HOST`（默认 localhost）、`UD_SKIP_BUILD=1`（复用 `.run/upgrade-demo/bin`）、`UD_STEP=1`（driver 在检查点暂停，按回车继续）、`UD_CONSOLE=1`（同时启动 Console）。日志在 `.run/upgrade-demo/logs/`：`driver.log` 是场景输出，`server.log` 是 Server，`app-a-v1.log` 等是各应用进程。

## 目录

```text
upgrade-demo/
├── run-demo.sh                一键运行
├── appa/
│   ├── worker.go              app-a 的 SDK 接入（Start）
│   ├── v1/ v2/ v3/            三个发布版本：activities.go、workflow.go 与生成产物
├── appb/
│   ├── worker.go              app-b 的 SDK 接入（Start）
│   ├── contract_test.go       核对 app-b 的远程契约与 app-a 的实现一致
│   └── v1/ v2/                两个发布版本：remote.go、activities.go、workflow.go 与生成产物
├── cmd/
│   ├── app-a/ app-b/          应用进程入口：解析参数、调用 Start、等待退出信号
│   └── driver/                启动和停止应用进程，运行 scenario
└── scenario/                  全部场景与断言；scenario_test.go 在进程内跑同一套场景
```

每个发布版本是一个独立的 Go 包，一个应用二进制把自己的全部发布版本编译进去，用 `-version` 选择启动哪一个。真实项目里它们是同一个包在不同时间点的提交；示例把它们并排放置，`diff -u appa/v1 appa/v2` 就是一次发布的全部改动。

每个版本包里手写的文件只有 `activities.go`、`workflow.go`（app-b 多一个 `remote.go`）。`zz_*_gen.go` 与 `workflow.json` 由 `go generate ./...` 生成。

## SDK 接入

接入代码在 `appa/worker.go` 与 `appb/worker.go` 的 `Start` 里，两个文件结构相同。`cmd/app-a/main.go` 只做参数解析和信号处理；接入代码放在包里是因为进程内测试也要调用它。

```go
// 1. Registry 同时持有 Activity 的实现与契约。生成的 RegisterActivities 一次登记本包全部 @Activity。
registry := activity.NewRegistry()
if err := rel.activities(registry); err != nil { ... }

// 2. Client 连接 Server；Worker 经它领取任务、上报 Activity 路由与契约。
c, err := client.Dial(client.Options{HostPort: opts.URL, Namespace: opts.Namespace})
if err != nil { ... }

// 3. 一个同时执行 Workflow Task 与 Activity Task 的 Worker。
w, err := worker.New(c, worker.Options{
	Role:                         worker.RoleBoth,
	Activities:                   registry,
	Identity:                     rel.build + "-1", // 进程实例
	BuildID:                      rel.build,        // 代码版本：新旧 build 靠它并存
	Workflows:                    []registration.Definition{rel.workflow()},
	ActivityWorkerVersionDefault: true,
})
if err != nil { ... }

// 4. Start：上报 Activity 路由与契约 → 登记 Workflow 定义 → 开始 poll。
w.Start(ctx)

// 5. WaitReady 返回前两步的结果；登记失败时 Worker 不 poll。
if err := w.WaitReady(readyCtx); err != nil { ... }

// 6. 进程退出前调用 Stop：停止 poll、等在途任务结束、向 Server 注销本 Worker 的路由。
w.Stop()
```

`Stop` 注销之后这个 build 立刻退出选路，driver 在停止一个应用进程后直接断言它已经离线。进程崩溃时没有机会注销，路由在心跳 TTL（默认 30s）之后到期。

`rel.activities` 与 `rel.workflow` 是生成的 `RegisterActivities` 与 `WorkflowDefinition`。app-b 的 `RegisterActivities` 只登记本地的 `orders.price`，远程契约只进入 Program 的声明，跨进程调用不需要额外的接入代码。

客户端一侧（`scenario/env.go`）用 `sdk/client`：`ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID}, workflowType, input)` 启动 Run，Workflow Type 是 `@Workflow` 的 `type`，Workflow ID 是订单号或退货单号，所以同一个类型可以同时有多个在途 Run；`SignalWorkflow`、`QueryWorkflow`、`GetWorkflowHistory` 驱动与读取 Run，`UpdateWorkflowExecutionOptions` 修改单个 Run 的版本策略。「写法基础」里用 `UpdateWorkflow` 按 Workflow ID 发送 Update `place` / `open`，同步拿到 Handler 的返回值。Run 详情（`UpgradeError`、`ConfigError`、Program 内容版本）、在线 Worker 与已登记的 Program 是 Console 使用的运维端点，driver 直接按 `protocol/runtimeapi` 的类型请求它们。

## 注解写法

| 写法 | 位置 |
|---|---|
| `@Activity(type=...,queue=...)`，函数签名 `func(context.Context, In) (Out, error)` | `appa/v1/activities.go` |
| `@RemoteActivity(...)` 声明在函数类型上，DTO 由调用方自己定义 | `appb/v1/remote.go` |
| `@Workflow(type=...,policy=...,version=...)` | `appa/v1/workflow.go`（`pinned`）、`appb/v1/workflow.go`（`auto_upgrade`） |
| 生成的类型化调用 `CallPrice(ctx, in, opts)` 与 `ActivityOptions` / `RetryPolicy` | `appb/v1/workflow.go` |
| 两个 Future 并行：`workflow.ExecuteActivity` 后分别 `Get` | `appb/v1/workflow.go` |
| Signal 等待 `workflow.WaitSignal`；Signal 与 Timer 竞争：`workflow.Go` + `workflow.Sleep` + `workflow.Await` | `appa/v1/workflow.go`、`appb/v1/workflow.go` |
| `workflow.Now`、`workflow.RandomUUID` | `appb/v1/workflow.go` |
| Query Handler 与 Update Handler | 两个 `workflow.go` 的开头 |
| `workflow.GetVersion` | `appa/v2/workflow.go`、`appa/v3/workflow.go`、`appb/v2/workflow.go` |

## 发布顺序

driver 按下面的顺序发布，每一步之前都有停在 Signal 等待点的在途 Run：

1. app-a v1 与 app-b v1 上线，跑一遍写法基础，再启动 8 个在途 Run。
2. **app-a v2 上线，与 app-a v1 并存**（场景 2、5、7）。
3. **app-b v2 上线，替换 app-b v1**（场景 1、2、3）。
4. **app-a v1 下线**（场景 4）。
5. **app-a v3 上线**（场景 6）。

先上提供 Activity 的一方、后上调用它的一方：新 Program 成为 latest 时，它声明的契约已经有 Worker 承接。

## Activity 升级

Activity 契约有三个维度：`type`、入参指纹、出参指纹。含义或结构要变时用新的 `type`，旧 `type` 保留到没有 Run 再调用它。Server 调度时不对照契约，任务投到队列；Worker 开始执行前按领取者自己的上报对照 Run 当前 Program 的声明：指纹相同即兼容，不同时按数据流向比较 wire 结构（入参新增字段兼容，删字段、改名、改类型不兼容），契约兼容的 build 执行它。新旧 build 同时在线、契约不兼容时，任务可能先被不兼容的 build 领到，被拒绝后约 1s 重新投递（`RESULTS.md` 记有这次运行的拒绝次数）。下文「派给」指实际执行它的 build（`ActivityStarted` 的 `workerBuildId`）。

### 场景 1：新增可选入参

`appa/v1/activities.go` → `appa/v2/activities.go`，`type` 不变：

```go
type ReserveInput struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
	Warehouse string `json:"warehouse,omitempty"` // v2 新增
}
```

调用方 `appb/v2/remote.go` 加上同一个字段，`appb/v2/workflow.go` 传 `Warehouse: "east"`。

结果：入参指纹从 `wire:a3853d00…` 变成 `wire:ca791891…`。v1 写出的入参 v2 都能读（v2 只多了 `warehouse`），按数据流向兼容，所以新旧 build 都能执行 v1 Program 的 reserve。v2 Program 上的 Run 派给 app-a-v2，`warehouse = east`；`pinned` 在 v1 Program 上的 Run 不传 `warehouse`，由领到它的 build 执行，`warehouse = west`。`appb/contract_test.go` 核对 app-b v1 的声明对上 app-a v2 的实现：指纹恰好差一条 `fulfillment.reserve: input`，按结构比较全部兼容。

### 场景 2：结构不变、含义变化，换新 `type`

运费从不含税改成含 10% 税。入参出参结构不变，指纹相同，Server 分不出新旧含义，所以 app-a v2 新增 `fulfillment.quote-taxed`，旧的 `fulfillment.quote` 原样保留：

```go
// appa/v2/activities.go
// 旧含义，与 v1 完全相同，保留给仍在调用旧 type 的 Run
// @Activity(type="fulfillment.quote",queue="fulfillment")
func Quote(...) { return QuoteOutput{Fee: 500 * int64(in.Qty), ...} }

// 新含义：fee 含税
// @Activity(type="fulfillment.quote-taxed",queue="fulfillment")
func QuoteTaxed(...) { return QuoteOutput{Fee: 550 * int64(in.Qty), ...} }
```

`appa/v2/workflow.go` 的退货运费改调 `CallQuoteTaxed`；`appb/v2/remote.go` 用 `@RemoteActivity(type="fulfillment.quote-taxed",...)` 声明新 type，旧的 `fulfillment.quote` 契约保留，`appb/v2/workflow.go` 改调新 type。换 type 改变了这一步的命令，所以用 `GetVersion` 保护：

```go
if workflow.GetVersion(ctx, "quote-taxed", workflow.DefaultVersion, 1) == workflow.DefaultVersion {
	label, quoteErr = CallQuote(ctx, QuoteInput{...}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
} else {
	label, quoteErr = CallQuoteTaxed(ctx, QuoteInput{...}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
}
```

已经调度过旧 quote 的 Run 重放到这里时 History 里没有这个版本点，得到 `DefaultVersion`，继续走旧 type；还没走到这一步的 Run 首次执行到这里得到 1。本示例的在途 Run 都停在这一步之前的 Signal 等待点，所以切到 v2 后都走新 type。

结果：

- v2 Program 上的 Run（新 Run 与切到 v2 的 `auto_upgrade` Run）调度 `fulfillment.quote-taxed`，只有 app-a-v2 提供它，`ship_fee = 1100`、`label_fee = 550`。
- `pinned` 在 v1 Program 上的 Run 调度 `fulfillment.quote`，`ship_fee = 1000`、`label_fee = 500`。两个 build 上报的旧 type 契约相同，谁领到谁执行。
- 场景 4 里 app-a v1 下线之后，旧 Program 上的 Run 照常调度旧 type，由 app-a-v2 执行，不暂停。

### 场景 3：破坏性变更，换 `type` 加 `GetVersion`

发货入参把一整行 `address` 拆成 `city` + `street`。app-a v2 新增 `fulfillment.ship-v2`，旧的 `fulfillment.ship` 原样保留，两个实现注册在同一个进程里。`appb/v2/workflow.go`：

```go
shipVersion := workflow.GetVersion(ctx, "ship-address-split", workflow.DefaultVersion, 1)
...
if shipVersion == workflow.DefaultVersion {
	shipped, err := CallShip(ctx, ShipInput{..., Address: in.Address}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
} else {
	shipped, err := CallShipV2(ctx, ShipV2Input{..., City: in.City, Street: in.Street}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
}
```

结果：新 Run 得到 1，`shipped_via = fulfillment.ship-v2@app-a-v2`。升级前启动的 `auto_upgrade` Run 切到 v2 Program，重放到版本点时 History 里没有记录，得到 `DefaultVersion`，继续调旧 `type`。同一个 Run 里 reserve 与 quote-taxed 已经按新契约派给 app-a-v2。

### 场景 4：旧 build 下线

app-a v1 停止（`Worker.Stop` 注销路由）之后，对两个 `pinned` 在 v1 Program 上的 Run 发 Signal：

- `return-old-type`（return-flow v1）调度旧 type `fulfillment.quote`。app-a v2 保留了它，契约与 v1 相同，Run 由 app-a-v2 执行并完成，`label_fee = 500`。
- `order-v1-offline`（order-flow v1）调度 `fulfillment.reserve`。它的入参在 v2 加了可选字段 `warehouse`，指纹不同；领取时 Server 按数据流向比较结构，v1 写出的入参 app-a-v2 都能读，判为兼容。app-a-v2 直接执行，`warehouse = west`（v1 的入参没有这个字段），`ship_fee = 1000`，Run 留在 v1 Program 完成，不需要旧 build，也不需要 set-policy。

改动不兼容时（例如 v2 把入参的 `sku` 改名为 `item`），app-a-v2 的领取被拒绝，Server 日志记 `activity claim refused`，原因写出不兼容的字段路径；任务留在队列里，Run 不暂停，旧 build 重新上线后自动继续，或者 set-policy `auto_upgrade` 让 Run 切到 v2 Program 后由 app-a-v2 领取。这条路径由 `server/biz/service/engine` 的测试覆盖，本示例没有演示。

## Workflow 升级

### 场景 5：`GetVersion` 保护的兼容变更

`appa/v2/workflow.go` 在 inspect 之后插入一步通知：

```go
notified := false
if workflow.GetVersion(ctx, "notify-after-inspect", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
	sent, err := CallNotify(ctx, NotifyInput{OrderID: in.OrderID, Condition: inspected.Condition}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	...
	notified = sent.Sent
}
```

| Run | 结果 |
|---|---|
| 新启动 | v2 Program，`notified = true`，调度 inspect → notify → quote-taxed → restock |
| 在途、`pinned` | 留在 v1 Program，`label_fee = 500`，调度旧 type quote，新旧 build 都可以执行 |
| 在途、`auto_upgrade` | 下一个 Workflow Task 边界切到 v2 Program，`GetVersion` 得到 `DefaultVersion`，`notified = false`，`label_fee = 550`，调度 quote-taxed，派给 app-a-v2 |

启动时的 Program 保留在 Run 的 original 字段，汇总表的「启动时」「结束时」两列来自它与 current。

### 场景 6：没有版本保护的不兼容变更

`appa/v3/workflow.go` 把通知挪到 inspect 之前，并删掉 `GetVersion`。新 Run 在 v3 Program 上正常完成。在途的 `auto_upgrade` Run 收到 Signal 后：

```text
UpgradeError: auto upgrade to 5913447dae33a536a496dc63b2c96dc6 failed at workflow task 15: non_deterministic_error: nondeterministic workflow: history event is ActivityScheduled(id=#a1, type=fulfillment.inspect), replay command is ScheduleActivity(id=#a1, type=fulfillment.notify) (di=1, hi=1); retrying with backoff
```

Run 的 status 保持 `running`，Program 保持 v1，History 里有一条 `WorkflowTaskFailed`（`cause=non_deterministic_error`），Server 按退避重试。

恢复：

- **改成 `pinned`**：`UpgradeError` 清除，Run 在 v1 Program 上完成。
- **发布兼容的 Program**：把 v2 的定义重新登记为 latest，Run 在下一次重试时切到 v2 Program 并完成。

### 场景 7：发布顺序

- app-a v2 上线后 `return-flow` 的 latest 立即变成 v2：Program 随 Worker 的 `Start` 登记。
- 此时 app-b 还在 v1：新启动的订单用 `order-flow` v1 Program，声明旧契约：reserve 派给 app-a-v1，旧 type quote 两个 build 都可以执行。新 Worker 先在线不影响旧 Program 上的 Run。

## 说明

- **Workflow Task 不按应用划分。** 任何一个 Workflow Worker 都可以执行任何 Program；输出里「这个 Run 的 Workflow Task 执行者」一行可以同时出现 app-a 与 app-b。
- **每个 Run 的 Workflow ID 是业务单号。** `order-flow` 与 `return-flow` 各自同时有多个在途 Run，靠 Workflow ID 区分；Signal、Query、Update 可以按 Workflow ID 寻址，也可以按 Run ID 寻址。
