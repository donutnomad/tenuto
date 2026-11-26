# Tenuto

Tenuto 是面向 Go 应用的持久化 Workflow 编排平台。Workflow 用普通 Go 编写，由生成器编译为确定性的 WLang Program；Tenuto Server 保存 Event History 与当前执行状态，负责 Activity、Timer、Signal、Update、Child Workflow 的调度、重试和超时；Worker 按历史确定性地重放 Program，业务进程或 Server 重启后，Run 从已提交的状态继续执行。仓库包含 Go SDK、Workflow 编译器、解释器 Worker、HTTP Server（内存与 MySQL 两种后端）、Next.js Console 和中文文档站。

文档站：<https://donutnomad.github.io/tenuto/>

## 功能

- **持久化执行**：Activity、Timer、Signal、Query、Update、Child Workflow 与 Continue-As-New，状态跨进程保存，崩溃后自动恢复。
- **普通 Go 编写 Workflow**：`@Workflow` / `@Activity` 注解生成类型化调用桥与可注册产物；`workflow.Go`、`WaitGroup`、Channel、Selector 由确定性协作式调度器执行。
- **确定性 SDK**：`workflow.Now`、`SideEffect`、`GetVersion`、`RandomUUID`、`RandomInt` 把非确定性值固化到 History。
- **强类型 Update 与 Application Error**：Handler 返回 Result 与 error，Update-With-Start 支持同步业务接口。
- **版本与路由**：定义按内容哈希版本化，版本是 Run 的属性（`pinned` / `auto_upgrade`）；Activity 按 Task Queue 投递，Worker 领取时按它这个 Build ID 上报的契约校验。
- **多租户与可见性**：Namespace 隔离 Run、配置、队列与 Worker 路由；Search Attributes 与 Run 列表查询。Namespace 是寻址范围，不是权限边界：Server 内置可选的 Bearer token 与 mTLS 认证（缺省关闭），按调用方分权由网关承担，见 [安全部署](docs/deploy/security.md)。
- **生产运行时**：MySQL mutable state、TaskQ matching、Sticky 增量激活、Schedule、Reset、批量操作与保留期清理。
- **Console**：Run、Event History、Worker、Schedule 管理，以及基于浏览器 WASM 重放的源码级 Trace 调试台。

## 环境要求

| 工具 | 版本 | 用途 |
| --- | --- | --- |
| Go | `go.mod` 声明的版本 | Server、SDK、Worker、编译器与 Console WASM 模块 |
| Node.js | 20+ | Console 与文档站 |
| pnpm | 10+ | Console 与文档站依赖 |
| make、curl、lsof | 系统自带版本 | 开发命令与健康检查 |

本地开发使用内存后端，不需要数据库。生产部署使用 MySQL：首次部署与每次升级先执行 `tenuto-server migrate`（版本化、只向前的 schema 迁移），再启动 Server，见 [部署 Tenuto Server](docs/deploy/server.md)。

## 快速开始

```bash
git clone https://github.com/donutnomad/tenuto && cd tenuto
pnpm --dir console install
make dev
```

`make dev` 构建 Console 的 WASM 重放模块，在后台启动内存后端的 Tenuto Server（`http://localhost:7400`）与 Demo Worker/Seeder，再在前台启动 Console（`http://localhost:7401`）。Demo 在 `demo` Namespace 下创建线性流程、并发 fan-out、重试、Child Workflow 与 Trace 样例，日志写入 `.run/`。`make stop` 停止后台进程。

另开一个终端运行下面的示例 Worker，再启动一次 Run：

```bash
make build-example
./bin/tenuto-startup-example          # 连接 http://localhost:7400，Namespace startup-demo

curl -sS http://localhost:7400/runtime/workflow/start \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"startup-demo","workflow_type":"startup-greeting","workflow_id":"greeting-1","input":{"name":"tenuto"}}'
```

在 Console 的 `startup-demo` Namespace 中查看这次 Run 的 Event History 与 Trace。完整步骤见 [五分钟快速开始](docs/quickstart/index.md)。在自己的 Go 模块里接入（`go get`、注解、生成、注册 Workflow 与 Activity）见 [接入已有项目](docs/quickstart/integrate.md)。

多个服务进程协作的完整业务示例在 [`examples/shop`](examples/shop)：下单、支付、仓储三个服务加 Tenuto Server，`./examples/shop/run-demo.sh` 一键演示重试幂等、补偿、Signal 与服务宕机后恢复，不需要数据库。

## 最小示例

Workflow 与 Activity 写在同一个包里（[`docs/examples/startup-workflow/workflow.go`](docs/examples/startup-workflow/workflow.go)）。生成器读取注解，产出 `CallSayHello` 调用桥、包级 `RegisterActivities` 和嵌入二进制的 `WorkflowDefinition()`：

```go
package startupflow

import (
	"context"

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
	out, err := CallSayHello(ctx, SayHelloInput{Name: in.Name})
	if err != nil {
		return GreetOutput{}, err
	}
	return GreetOutput{Message: out.Message}, nil
}

// @Activity(type="startup.say-hello",queue="startup")
func SayHello(_ context.Context, in SayHelloInput) (SayHelloOutput, error) {
	return SayHelloOutput{Message: "hello " + in.Name}, nil
}
```

一个 Worker 同时执行这个包的 Workflow 与 Activity：启动时先上报 Activity 路由，再登记随二进制发布的 Workflow 定义，然后开始拉取任务。节选自 [`docs/examples/startup-workflow/worker/main.go`](docs/examples/startup-workflow/worker/main.go)：

```go
c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
if err != nil {
	return err
}
defer c.Close()

registry := activity.NewRegistry()
if err := startupflow.RegisterActivities(registry); err != nil {
	return fmt.Errorf("register activities: %w", err)
}
w, err := worker.New(c, worker.Options{
	Workflows:          []registration.Definition{startupflow.WorkflowDefinition()},
	Activities:         registry,
	Identity:           "startup-worker-1",
	BuildID:            "startup-v1",
})
if err != nil {
	return err
}
w.Start(ctx)
defer w.Stop() // 优雅停止：等在途任务结束，注销 Activity 路由
if err := w.WaitReady(readyCtx); err != nil {
	return fmt.Errorf("worker ready: %w", err)
}
```

应用代码用同一个 Client 启动 Workflow 并等待结果：

```go
run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "greet-ada"}, "startup-greeting", startupflow.GreetInput{Name: "Ada"})
if err != nil {
	return err
}
var out startupflow.GreetOutput
if err := run.Get(ctx, &out); err != nil {
	return err
}
```

导入路径：`github.com/donutnomad/tenuto/sdk/client`、`github.com/donutnomad/tenuto/sdk/worker`、`github.com/donutnomad/tenuto/sdk/activity`、`github.com/donutnomad/tenuto/sdk/registration`。

## 仓库结构

```text
tenuto/
├── sdk/            # 业务代码使用的 Go SDK
│   ├── workflow/   #   Workflow API：Activity 调用、Timer、并发原语、Signal/Query/Update、确定性值
│   ├── activity/   #   Activity 注册表、Activity 上下文与 heartbeat
│   ├── apperror/   #   Application Error
│   ├── client/     #   客户端：启动、Signal、Query、Update、取消、终止、读取、Schedule、批量操作
│   ├── worker/     #   Worker：拉取并执行 Workflow Task 与 Activity Task
│   ├── registration/ # 随二进制发布的 Workflow 定义
│   └── internal/   #   SDK 实现细节：runtime HTTP 传输层
├── compiler/       # Workflow 编译器：Go 源码 → WLang Program 与可注册产物（tenuto-workflow-gen）
├── program/        # Program 与 Workflow 定义模型、解析和线上格式
├── protocol/       # 跨进程契约：任务载荷、Payload、Failure、Update、History 事件与命令
├── server/
│   ├── cmd/tenuto-server/ # Tenuto Server 入口与统一 go:generate 指令
│   ├── app/        #   组合根：按后端装配依赖
│   ├── biz/        #   领域模型（definition、execution、dispatch、schedule）、用例与执行引擎
│   ├── http/       #   Runtime HTTP handler
│   ├── infra/      #   内存后端、MySQL 持久化（GORM + gsql）、TaskQ、选主、SQL schema 与迁移
│   ├── integration/ #  端到端集成测试
│   └── demo/       #   `make dev` 使用的 Demo Worker/Seeder
├── telemetry/      # 遥测配置、slog 与运行时装配
├── cmd/tenuto-cluster/ # 多副本故障注入与压测工具（docs/operations/cluster-testing.md）
├── internal/       # 仓库内部共享实现：Worker 运行时（worker/）、WLang 解释器与增量激活（executor/）、JSON 数值
├── console/        # Next.js Console；console/wasm 是浏览器重放模块
├── examples/shop/  # 多进程业务示例：独立 Go 模块，三个服务 + Tenuto Server
└── docs/           # VitePress 中文文档站、可运行示例（docs/examples）与 Agent skill（docs/skills/tenuto）
```

## Mutable State 架构

Tenuto Server 用两层数据描述一个 Run，二者在同一个事务内一起更新：

- **Event History** 是 append-only 的事实日志。Workflow 的每个外部结果（Activity 完成、Timer 触发、Signal、Update、Child 关闭）都写成事件，Worker 按 History 重放 Program，得到相同的命令序列。
- **持久化 mutable state** 是当前调度状态的唯一权威：`ActivityState`、`TimerState`、`ChildWorkflowState` 每个 pending 项一行，主键为 `run_id + scheduled_event_id`；`WorkflowExecutionState` 保存 Run 级投影（open Workflow Task、Sticky 会话、搜索属性等）。判断 attempt、是否 started、是否终态，一律读 mutable state。

两层由 `server/biz/domain/execution` 的 `ExecutionManager` 统一提供：它组合 History、Run 与各类 state 的存储接口，`RunInTx` 在一个事务内执行 History 追加与 state 读写。内存后端（`server/infra/memory`）与 MySQL 后端（`server/infra/persistence`）实现同一接口契约。

不变式：

1. **同一事务提交**：History 追加与对应 state 的 upsert 在同一个 `RunInTx` 内提交，二者不会只写一半。Run 行的状态变更是 `Run` 上的方法（终态、升级与配置诊断、运行中策略、截止时间），由 `execution` 包里接收调用方 `ExecutionTx` 的函数与同一业务事实的事件一起提交，引擎不直接写存储；continue-as-new 与带重试的失败，接续 Run（Run 行、`WorkflowStarted`、current 切换、重试的退避 timer）与旧 Run 的终态事件、终态 Status 在同一个事务里提交，没有只写一半的窗口。
2. **读改写加锁**：修改 state 时在事务内用 `Get*StateForUpdate` 读取（MySQL 为 `SELECT ... FOR UPDATE`），校验后写回。写 Run 行的事务先取该 Run 的 execution state 行锁；写路径依据 Run 字段（状态、策略、配置错误、当前内容版本）做的判定，基于事务内 `ExecutionTx.GetRunForUpdate` 在这把锁下读到的 Run，多个 Server 副本处理同一个 Run 时不会依据另一个副本已改过的旧值。事务之外读到的 Run 只用于寻址、只读查询与 Run 创建后不变的字段（namespace、History 分支）；一次 Activity 运行时请求只读一次 Run。MySQL 上一个事务对每个 Run 的 execution state 只加锁读一次，之后的事件追加、Run 更新与激活都改这一份，提交前写回一次（一条 UPDATE），与事务追加几个事件无关；行锁从第一次读取持有到提交。一个事务锁两个已存在的 Run 时顺序固定：子 Run 向父 Run 传播关闭时先子后父，reset 按 Run ID 升序。同一个 Run 内先锁 execution state 行、再锁 Activity / timer / child state 行：Activity 的开始、完成、失败、超时、交还与 reset，timer 触发，取消命令与 Run 关闭都按这个顺序取锁；Activity 心跳只写 Activity 行，只锁这一行。InnoDB 仍选中事务作为死锁牺牲者（Error 1213）时，MySQL 后端用新事务把 `RunInTx` 的整个闭包重放一次，最多 3 次；闭包只经 `ExecutionTx` 读写，提交后的副作用放在 `RunInTx` 返回之后。
3. **幂等按 state 判定**：重复的完成、失败、触发请求依据 state 的终态丢弃，不扫描 History。
   Activity 心跳只写 state：`RecordActivityHeartbeat` 更新 `ActivityState` 的最后一次详情与时间（MySQL 上加锁读之后一条 UPDATE），不追加事件；Heartbeat 超时从 `LastHeartbeatAt` 起算，重试与重新派发的 Attempt 从这里读到详情，Attempt 超时时详情随 `ActivityTimedOut` 写入 History。
4. **恢复读 state**：周期补偿按到期索引（Run 截止时间、timer 与 Activity 超时截止时间、Scheduled Activity、open 或待调度的 Workflow Task、blocked）只读有到期或待处理事项的 Run，代价与这些事项数成正比，并把 lookahead（两个补偿周期）内到期的 Run 截止时间、timer 与 Activity 超时挂到本进程。启动恢复处理同一批候选 Run，从它们的 state 重建重试、超时和 Timer（同样只挂 lookahead 内到期的，已过期的立即触发），从 `WorkflowExecutionState` 重建已 started 的 Workflow Task 的超时；Run 缺少配置声明的截止时间时（升级到声明超时的 Program）只对候选 Run 补写，与 Temporal 相同，执行超时在 Run 启动时确定，升级不回填空闲的 Run。没有到期索引的崩溃窗口（子 Run 补建、终态传播与写回、父关闭策略、子 Run 取消请求补发）由启动恢复与低频全量补偿按游标逐页检查全部 Running Run，每页是固定几条批量读取，只有需要补做的 Run 才逐个处理。
5. **Stamp 隔离旧任务**：`ActivityState.Stamp` 是 generation token，编入签名的 task token；reset 与重试时递增，started、heartbeat、完成、失败都校验 stamp，旧 attempt 的任务被丢弃。
6. **Reset 只落在 Workflow Task 边界**：`ResolveResetNodeID` 把 reset 点映射到 Workflow Task 边界事件，`CheckResetPendingEvents` 拒绝会继承 pending Activity 或 Timer 的位置。
7. **Activity 不保存 build**：`ActivityState` 只保存队列与 Run 的目标 Worker。首次投递、重试、恢复重派与 Worker 停止交还都由 `ActivityTaskFromState` 按队列重建任务，任何订阅该队列的 build 都能领取；Worker 开始 Attempt 前，Server 按领取者这个 build 的上报对照 Run 当前 Program 的契约声明，不兼容时不开始、不消耗 Attempt，任务按退避重新投递。实际执行的 Worker 与 build 只记在 `ActivityState.WorkerBuildID` 与 `ActivityStarted`，不参与路由。
8. **Run 关闭处理依赖**：Run 以任何方式关闭（含 continue-as-new、失败重试、reset 终止）后，仍在运行的子 Workflow 按 `ChildWorkflowState` 记录的父关闭策略（Terminate / RequestCancel / Abandon）处理；终态提交之后由引擎执行，没有做完时由仍在运行的子 Run 在启动恢复与全量补偿中依据父 Run 的 execution state 补做。已接受、未完成的 Update 由 Run 的终态事件以 `AcceptedUpdateCompletedWorkflow` 失败结束（`UpdateRecordsFromEvents` 的投影），不另写事件。仍未触发的 `TimerState` 与仍未结束（scheduled、started）的 `ActivityState` 在写入终态的同一个事务里转 `run_closed`（`ExecutionTx.CloseScheduledTimers`、`CloseOpenActivities`，MySQL 上各一条 UPDATE），触发、超时与恢复都跳过它们，不写事件，与 Temporal 关闭 Run 时随 mutable state 丢弃 pending timer 与 activity 一致；迟到的完成与心跳按 Activity 已结束处理。终态同时清除 sticky 路由与 open Workflow Task 投影，sticky 回落扫描只选运行中的 Run。
9. **取消状态随命令提交**：Workflow 的取消命令与它改动的 state 同事务提交：`RequestCancelActivity` 给 `ActivityState` 置 `CancelRequested`（未开始的同事务转 `cancelled` 并写 `ActivityCancelled`），之后的失败与超时不再重试、交还的 Attempt 不再派发；`CancelTimer` 把 `TimerState` 转 `cancelled`，触发与恢复都跳过它；`RequestCancelChildWorkflow` 给 `ChildWorkflowState` 置 `CancelRequested`，子 Run 的取消请求在提交后发出，没有发出时由子 Run 在启动恢复与全量补偿中依据这个标记补发。Run 级的 `CancelRequested` 只表示 History 里有取消请求，不影响 Activity 的开始与执行。

## 文档

- [五分钟快速开始](docs/quickstart/index.md) · [Demo](docs/quickstart/demo.md) · [上线检查清单](docs/quickstart/production-checklist.md)
- [系统架构](docs/concepts/architecture.md) · [Event History](docs/concepts/event-history.md) · [Task 与 Worker](docs/concepts/tasks-workers.md)
- [编写 Workflow](docs/develop/workflows.md) · [编写 Activity](docs/develop/activities.md) · [运行 Worker](docs/develop/workers.md) · [代码生成](docs/develop/code-generation.md) · [版本管理](docs/develop/versioning.md) · [测试](docs/develop/testing.md)
- [Signal、Query 与 Update](docs/develop/signals-queries-updates.md) · [Typed Update](docs/develop/updates.md) · [Application Error](docs/develop/application-error.md)
- [部署 Tenuto Server](docs/deploy/server.md) · [生产部署](docs/deploy/production.md) · [生产就绪与验证结果](docs/operations/production-readiness.md) · [多副本故障注入与压测](docs/operations/cluster-testing.md) · [配置参考](docs/reference/configuration.md) · [HTTP API](docs/reference/http-api.md)
- [Console](docs/console/index.md) · [Trace 调试台](docs/console/trace.md)
- [Agent skill](docs/develop/agent-skill.md)

本地浏览文档站：

```bash
pnpm --dir docs install
make dev-docs        # http://localhost:7402/tenuto/
```

## 开发与测试

```bash
make generate        # 统一代码生成：go generate ./...
make check           # 生成后无 diff，再 vet 与全部测试
make test            # go test ./... 与 Console 单元测试
make build-server    # 构建 bin/tenuto-server
make build-docs      # 构建文档站
```

所有 Go 命令在仓库根目录以 `GOWORK=off` 运行。MySQL 测试默认关闭，设置 `TENUTO_TEST_MYSQL=1`（可选 `TENUTO_TEST_MYSQL_DSN`）开启。贡献流程、提交规范与文档同步要求见 [CONTRIBUTING.md](CONTRIBUTING.md)，安全问题按 [SECURITY.md](SECURITY.md) 报告。

## 许可证

[MIT](LICENSE)
