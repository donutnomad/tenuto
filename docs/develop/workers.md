---
title: 运行 Worker
description: 配置 Workflow Worker、Activity Worker、定义登记、并发度和版本路由
status: Stable
---

# 运行 Worker

<FeatureStatus status="Stable" />

Workflow Worker 执行确定性重放，Activity Worker 执行业务副作用。两者都由包 `github.com/donutnomad/tenuto/sdk/worker` 提供，经 `sdk/client` 的 `Client` 连接 Server：一个 Client 可以同时给多个 Worker 和应用代码使用。

`tenuto-server` 默认内嵌一个 Workflow Worker；`WORKFLOW_WORKER_ENABLED=false` 时由独立部署的 Workflow Worker 消费 Workflow Task。Workflow 定义随 Worker 二进制发布，在 `Start` 时登记。

## 完整示例

下面的启动顺序来自仓库 `docs/examples/saga-workflow/worker/main.go`：Activity Worker 先上线并等路由登记完成，Workflow Worker 再登记定义。

```go
import (
    "context"
    "time"

    sagaflow "github.com/donutnomad/tenuto/docs/examples/saga-workflow"
    "github.com/donutnomad/tenuto/sdk/activity"
    "github.com/donutnomad/tenuto/sdk/client"
    "github.com/donutnomad/tenuto/sdk/registration"
    "github.com/donutnomad/tenuto/sdk/worker"
)

func run(ctx context.Context, url, namespace string) error {
    c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
    if err != nil {
        return err
    }
    defer c.Close()

    // Activity Worker
    registry := activity.NewRegistry()
    if err := sagaflow.RegisterActivities(registry); err != nil {
        return err
    }
    activityWorker, err := worker.New(c, worker.Options{
        Role:                                   worker.RoleActivity,
        Activities:                             registry,
        Identity:                               "saga-activity-1",
        BuildID:                                "saga-v1",
        ActivityWorkerVersionDefault:           true,
        ActivityWorkerVersionHeartbeatInterval: 5 * time.Second,
    })
    if err != nil {
        return err
    }
    activityWorker.Start(ctx)
    defer activityWorker.Stop()

    readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    if err := activityWorker.WaitReady(readyCtx); err != nil {
        return err
    }

    // Workflow Worker：Start 时登记随二进制发布的定义
    workflowWorker, err := worker.New(c, worker.Options{
        Role:                worker.RoleWorkflow,
        WorkflowConcurrency: 1,
        Identity:            "saga-workflow-1",
        BuildID:             "saga-v1",
        Workflows:           []registration.Definition{sagaflow.WorkflowDefinition()},
    })
    if err != nil {
        return err
    }
    workflowWorker.Start(ctx)
    defer workflowWorker.Stop()
    if err := workflowWorker.WaitReady(readyCtx); err != nil {
        return err
    }

    <-ctx.Done()
    return nil
}
```

同一个包的 Workflow 与 Activity 也可以由一个 Worker 执行（`Role` 留空，`Activities` 与 `Workflows` 都设置），见 `docs/examples/startup-workflow/worker/main.go`。

## 创建与运行

```go
w, err := worker.New(c, worker.Options{...})
```

`worker.New(c, options)` 用 `c`（必须由 `client.Dial` 创建）的连接与 Namespace。与 Temporal 的 `worker.New(client, taskQueue, options)` 相比有两处不同：

- 没有 task queue 参数：Workflow Task 不分队列，由 Server 派给任意 Workflow Worker；Activity 的队列取自 registry 登记的队列，或由 `ActivityTaskQueues` 指定，一个 Worker 可以消费多个队列。
- 没有 `RegisterWorkflow` / `RegisterActivity` 方法：Workflow 是生成器打进二进制的定义（`Workflows`），Activity 由生成的 `RegisterActivities` 登记到 `activity.Registry`（`Activities`）。

| 方法 | 作用 |
| --- | --- |
| `Start(ctx)` | 在后台登记并开始拉取任务，立即返回；`ctx` 是在途任务的执行 context |
| `WaitReady(ctx)` | 等 `Start` 的登记完成，返回登记错误 |
| `Run(ctx)` | `Start` + `WaitReady`，然后阻塞到 `ctx` 结束并优雅停止；登记失败时返回错误 |
| `Stop()` | 优雅停止，见[优雅退出](#优雅退出) |

## worker.Options

| 字段 | 缺省 | 含义 |
| --- | --- | --- |
| `Role` | 见右 | `worker.RoleBoth` / `RoleWorkflow` / `RoleActivity`；留空时 `Activities` 登记了 Activity 取 `RoleBoth`，否则 `RoleWorkflow` |
| `Workflows` | 空 | 随二进制发布的 Workflow 定义，`Start` 时登记 |
| `Activities` | 空 | 本 Worker 执行的 Activity；契约随路由上报 |
| `ActivityTaskQueues` | registry 登记的队列 | 消费的 Activity Task Queue 列表；显式设置时必须包含 registry 登记的队列。队列的完整决定规则见[Activity 的队列如何决定](/operations/namespaces-task-queues#activity-的队列如何决定) |
| `WorkflowConcurrency` | `1` | 同时处理的 Workflow Task 数 |
| `ActivityConcurrency` | `1` | 每个 Activity Task Queue 的并发槽位 |
| `WorkflowTaskTimeout` | `10s` | 单个 Workflow Task 的执行超时；领取时报给 Server，Server 按它为这个 Workflow Task 计时 |
| `GracefulStopTimeout` | `10s` | `Stop` 等待在途 Activity 与 Workflow Task 结束的上限，见[优雅退出](#优雅退出) |
| `Identity` | `client.Options.Identity` | Worker 实例标识，适合 pod name 或 instance ID |
| `BuildID` | 编译时的 Git revision | Worker 代码版本：同一份代码的所有进程用同一个值，代码变了就换值。为空时取编译时写入二进制的 Git revision（前 12 位，工作区有未提交改动时加 `-dirty`）；二进制里没有 revision（`go run`、`go test`、`-buildvcs=false`、在 Git 仓库外构建）时 `worker.New` panic，此时显式设置（例如从部署注入的环境变量读取） |
| `ActivityWorkerVersionDefault` | `false` | Activity 没有指定队列、多个队列都承接它的类型时，优先选这个 Worker 登记的队列 |
| `ActivityWorkerVersionWeight` | `0` | 同上情况下各登记之间选择队列的权重；不决定由哪个 build 执行 |
| `ActivityWorkerVersionHeartbeatInterval` | TTL 的一半 | 路由心跳间隔 |
| `ActivityWorkerVersionHeartbeatTTL` | `30s` | 路由登记的存活时间；进程崩溃后路由在这段时间之后到期 |
| `ExecutionCache` | `nil` | 非 nil 时启用 Sticky Execution Cache，见 [Sticky Execution Cache](/operations/sticky-cache) |
| `Telemetry` | `client.Options.Telemetry` | OpenTelemetry providers；Worker 的日志写入其中的 `Logger`，未配置时写入 `slog.Default()` |

`ActivityTaskQueues` 为空时，Worker 拉取 registry 里登记的队列：生成的 `RegisterActivities` 带上注解里的 `queue`，手写登记用 `activity.RegisterActivityOnQueue`；有不带队列的 Activity（`activity.RegisterActivity`、`reg.Register`）时再加上 `default`。显式设置 `ActivityTaskQueues` 时必须包含 registry 登记的每个队列，否则 `worker.New` 返回错误；可以额外列出别的队列，例如 Workflow 用 `ActivityOptions.TaskQueue` 改派的队列。

Namespace 取自 Client：`Options.Namespace` 决定 Activity 的登记与领取范围，以及 Workflow 定义登记到哪个 Namespace。

## 启动与就绪

`Start` 按顺序执行：

1. 同时带有 `Workflows` 和 Activity 注册时，先同步上报 Activity Worker 版本。
2. 登记 `Workflows` 里的每份定义；任何失败都不启动 poll。
3. 启动 Activity Worker 版本心跳。
4. 启动 Workflow Task 和 Activity Task 消费协程。

`WaitReady` 返回启动前置步骤的结果：

```go
readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()

if err := w.WaitReady(readyCtx); err != nil {
    return err
}
```

定义经 Worker 所用 Client 的连接登记，任何 `Role` 的 Worker 都可以携带 `Workflows`。

Activity Worker 与 Workflow 定义的上线顺序没有硬性要求。Activity 的队列能确定时（调用处 `ActivityOptions.TaskQueue` 或 `@Activity(queue)`），调度照常写入 `ActivityScheduled` 并投进这个队列，与此刻有没有在线 Worker、在线 Worker 的契约是否兼容无关；任务在队列里等 Worker 上线，`ScheduleToStartTimeout` 从调度时起算。队列不能确定时（两处都没写、路由目录里没有这个类型或路由歧义），Workflow Task 以 `activity_route_unavailable` 失败并退避重试，Run 显示为 Blocked，Worker 上线后继续执行。

Server 把 Worker 登记缓存为路由目录：本副本上的登记与注销立即生效；其他副本上的登记与注销最多 5 秒后生效。心跳只延长登记的到期时间。路由目录总是存在，没有任何登记时是空目录：未指定 Task Queue 的 Activity 在这个 Namespace 有 Worker 登记它的类型之前以 `activity_route_unavailable` 退避等待，内存部署、测试与 MySQL 部署的规则相同。与 Server 同进程运行的 Worker 同样向 Server 登记自己的路由。

Workflow Worker 领取的是集群范围的 Workflow Task 队列，不按 Namespace 划分：Client 的 Namespace 决定 Activity 的登记与领取范围，以及 Workflow 定义登记到哪个 Namespace。Workflow Worker 的读取（配置、History、状态）凭所领 Workflow Task 的 task token，只能读这个任务所属的 Run。

## Identity、Build ID 与路由

- **Identity** 标识具体 Worker 实例。
- **Build ID** 标识 Worker 代码版本。
- **Task Queue** 表示逻辑工作队列。
- **Default / Weight** 只在 Activity 没有指定队列、多个队列都承接它的类型时参与选择队列。

Activity 调度时确定的 Task Queue 写入 `ActivityScheduled`，重试、恢复重派与停止交还继续投到这个队列。任务不带 build：队列上任何 build 的 Worker 都能领取；Run 固定了目标 Worker 时只交给这个 Identity，与它的 build 无关。Worker 开始 Attempt 时，Server 按它这个 build 上报的契约对照 Run 当前 Program 的声明，不兼容的领取被拒绝，任务约 1s 后重新投递给其他 Worker。实际执行的 Identity 与 Build ID 记录在 `ActivityStarted`。旧 build 下线前先排空只有它满足契约的 Activity（见 [版本管理](/develop/versioning#activity-worker-与-build-id)）。

## 并发度

`WorkflowConcurrency` 控制同时处理的 Workflow Task 数量。`ActivityConcurrency` 对每个 Activity Task Queue 分别生效，消费协程总数等于队列数乘以并发度。长时间 Activity 使用独立队列和足够的槽位，避免阻塞短任务。

## OpenTelemetry

Worker 使用与 Server 相同的 `TENUTO_OTEL_*` 和标准 `OTEL_EXPORTER_OTLP_*` 环境变量：

```go
observability, err := telemetry.NewFromEnv(ctx, telemetry.Config{
    ServiceName:       "orders-worker",
    ServiceVersion:    version,
    ServiceInstanceID: hostname,
})
if err != nil {
    return err
}
defer observability.Shutdown(context.Background())

c, err := client.Dial(client.Options{
    HostPort:  serverURL,
    Namespace: "orders",
    Telemetry: observability.Providers(),
})
```

`client.Options.Telemetry` 覆盖 HTTP 传播与 Update spans；从这个 Client 创建的 Worker 在 `worker.Options.Telemetry` 留空时沿用它，覆盖 Worker spans、metrics 和 logs。Worker 的全部日志（每个任务的结束记录，以及 poll、ack/nack、路由登记与心跳、结果上报失败）都写入 `Telemetry.Logger`；没有配置 Logger 时写入 `slog.Default()`。

## Server 拒绝上报时

SDK 对连接错误与 502/503/504 按幂等调用重试（见 [Client](/sdk/client)）。Activity 的 `begin` 带一个每份投递生成一次的 `request_id`：应答在途中丢失后的重发由 Server 认作同一次开始，返回原来的开始时间，Activity 照常执行。

Server 以不可重试的 4xx（400、404、409、413、422）拒绝时，重发得到同样的结果，Worker 不立即退回投递：

| 被拒绝的调用 | Worker 的处理 | 日志 |
| --- | --- | --- |
| Activity `begin` | 按连续拒绝次数退避（1s 起翻倍，最长 1 分钟）后退回租约 | WARN `server rejected the activity begin request; returning the task after a backoff` |
| Activity 结果 | 改报一次不可重试的失败，Type `ActivityResultRejected` | 任务结束记录 |
| Workflow Task 提交，应答 413 | 改报 `WorkflowTaskFailed`（`payload_size_limit_exceeded`），Run Blocked 并退避重试；Worker 在提交前已检查命令总量（3.5 MiB），超限时同样改报、不发送 | 任务结束记录 |
| Workflow Task 的其他上报 | 既不确认也不退回，投递留到队列的可见性超时；已开始的 Workflow Task 由 Server 超时重新调度 | ERROR `server rejected the workflow task report; leaving the delivery to the queue's visibility timeout` |

Workflow 程序要求的能力超出 Worker 声明时，Workflow Task 照常立即退回，由具备能力的 Worker 领取。载荷上限见 [容量上限](/operations/capacity-limits#载荷大小)。

## 优雅退出

`Stop()` 按顺序执行：

1. 停止拉取：不再发起新的 poll，并请求 Server 结束本 Worker 实例在途的长轮询（`POST /runtime/workflow/shutdown-worker`，上限 3 秒）。长轮询在连接仍然连着时以 204 结束，Server 恰好已领到的任务照常交给 Worker，在宽限期内处理。Server 确认后 Worker 最多再等 1 秒，然后取消仍未返回的 poll。Server 在连接断开之后才领到的任务立即交还：Workflow Task 记一条 `WorkflowTaskTimedOut`（`worker disconnected before receiving the workflow task`）并立即调度下一个，Activity 租约退回队列立即重投，都不等超时。
2. 关闭 `activity.GetWorkerStopChannel(ctx)` 返回的 channel。在途任务的 `ctx` 此时不取消。
3. 等在途的 Activity 和 Workflow Task 结束，最多等 `GracefulStopTimeout`（缺省 10 秒）。宽限期内结束的任务照常上报结果。
4. 宽限期到期时取消仍在运行的任务的 `ctx`，并等它们返回。
5. 向 Server 注销本 Worker 的 Activity 路由（`POST /runtime/workflow/deregister-activity-worker-version`）。注销之后这个 build 立刻退出选路；它没领走的任务留在队列里，由同一队列上的其他 Worker 领取。
6. 关闭 Execution Cache。

因 Worker 停止而结束的任务不算失败：

| 任务 | 结束方式 | Worker 的处理 | 之后 |
| --- | --- | --- | --- |
| Activity | 察觉停止 channel 后返回 `activity.ErrWorkerStopping`（或包装它的错误） | 交还这次 Attempt（`POST /runtime/activity/release`），不上报失败 | Server 以同一个 Attempt 立即重新派发，重试次数不变，`GetLastHeartbeatDetails` 读到最后一次心跳的进度 |
| Activity | 宽限期到期，`ctx` 被取消后返回任意错误 | 同上 | 同上 |
| Workflow Task | 宽限期到期仍未完成 | 不上报，确认队列租约 | Server 的 Workflow Task 超时（`WorkflowTaskTimeout`）到期后重新调度 |

Activity 在宽限期内返回成功时照常上报结果；在 Worker 停止之外返回 `ErrWorkerStopping` 按普通失败处理。交还请求没有送达时（例如 Server 不可达），这次 Attempt 停在 started，由 Start-To-Close 或 Heartbeat 超时按 Retry Policy 重试，与 Worker 崩溃相同。

长时间运行的 Activity 监听停止 channel，保存进度后交还：

```go
func Export(ctx context.Context, in ExportInput) (ExportOutput, error) {
    for offset := resumeOffset(ctx); offset < in.Total; {
        select {
        case <-activity.GetWorkerStopChannel(ctx):
            activity.RecordHeartbeat(ctx, map[string]any{"offset": offset})
            return ExportOutput{}, activity.ErrWorkerStopping
        case <-ctx.Done():
            return ExportOutput{}, ctx.Err()
        default:
        }
        offset += exportBatch(ctx, offset)
        activity.RecordHeartbeat(ctx, map[string]any{"offset": offset})
    }
    return ExportOutput{Count: in.Total}, nil
}
```

Worker 先请求 Server 结束长轮询、再断开连接，是因为 Server 不能立即察觉连接断开：这期间领到的任务写进已关闭的连接，要等队列的可见性超时或 Workflow Task 超时才重新投递。每个 Worker 实例带一个随机 session（请求头 `X-Workflow-Worker-Session`），Server 按 Identity 加 session 结束长轮询，并在 2 分钟内对这个 session 新到的 poll 立即应答 204；重启后复用同一 Identity 的新实例使用新的 session，不受影响。多副本部署时，只有接到 `shutdown-worker` 请求的副本会结束长轮询，落在其他副本上的长轮询仍由 Worker 直接取消，那一刻领到的任务按上面的超时重投。

`Stop` 可以重复、并发调用，每个调用都在停止完成后返回；没有在途任务时立即返回。忽略 `ctx` 的 Activity 会让 `Stop` 一直等到它返回。`Start` 的 `ctx` 被取消时在途任务立即中止，与宽限期到期相同。

注销是尽力而为的一次调用，等待上限 3 秒；调用失败时只记日志。进程崩溃或被 `SIGKILL` 时没有机会注销，路由在心跳 TTL（默认 30s）之后到期；队列里的任务由同一队列上仍在 poll 的 Worker 领取，它已经开始的 Attempt 由 StartToClose 或心跳超时重试。

注销只让路由到期，Worker 的登记记录保留：指定了 Target Worker 的 Workflow 仍然能找到这个离线 Worker 上次登记的队列，任务排队等这个 Identity 回来，受 `ScheduleToStartTimeout` 约束。Server 的周期全量补偿（`WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL`）删除到期超过一天、且没有 Running Run 指定的登记，Identity 含 Pod 名等每次发布都变的部分时旧登记因此不会无限累积。

Worker 是否在线只由它自己的心跳决定：网络分区或数据库故障让心跳失败超过上述期限、登记被删除后，下一次心跳得到 404，Worker 立即重新登记（失败时从 1 秒起加倍退避、不超过心跳间隔），重新进入选路。

部署平台在 `SIGTERM` 后预留的终止宽限期大于 `GracefulStopTimeout` 加约 7 秒（结束长轮询最多 4 秒、注销最多 3 秒），让 `Stop()` 走完（Kubernetes 缺省 30 秒）。
