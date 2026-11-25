# Worker

Workflow Worker 执行确定性重放（解释 Program），Activity Worker 执行原生 Go 的 Activity。两者都由包 `github.com/donutnomad/tenuto/sdk/worker` 创建，经 `sdk/client` 的 Client 连接 Server（`worker.New(c, options)`）。代码位置：`sdk/worker/`（公开 API）、`internal/worker/`（运行时）。

## 进程拓扑

| 拓扑 | 写法 | 适用 |
| --- | --- | --- |
| Server 内嵌 Workflow Worker + 业务进程只跑 Activity | 业务进程 `Role: worker.RoleActivity`，把 `Workflows` 放进它的选项 | 缺省部署；`tenuto-server` 默认内嵌 Workflow Worker（`WORKFLOW_WORKER_ENABLED`） |
| 业务进程同时跑两种 Worker | 一个 `Role` 留空（或 `RoleBoth`）的 Worker，`Activities` 与 `Workflows` 都设置；或一个 `RoleActivity` + 一个 `RoleWorkflow` | Server 设置 `WORKFLOW_WORKER_ENABLED=false`，或示例与测试 |
| 多个业务进程各管一组 Activity | 每个进程一个 Activity Worker、各自的队列与 BuildID；其中一个携带 `Workflows` | 多团队、多服务；见 `docs/examples/multiprocess/` |

Workflow 定义随 Worker 二进制发布，在 `Start` 时登记。任何 `Role` 的 Worker 都可以携带 `Workflows`（`docs/examples/multiprocess/app1/main.go` 是只跑 Activity 的例子）。

## 只跑 Activity、携带定义

```go
import (
    "context"
    "time"

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

    registry := activity.NewRegistry()
    if err := orderflow.RegisterActivities(registry); err != nil {
        return err
    }
    w, err := worker.New(c, worker.Options{
        Role:                         worker.RoleActivity,
        Activities:                   registry,
        ActivityConcurrency:          8,
        Identity:                     "orders-activity-" + hostname,
        BuildID:                      "orders-v1",
        ActivityWorkerVersionDefault: true,
        Workflows:                    []registration.Definition{orderflow.WorkflowDefinition()},
    })
    if err != nil {
        return err
    }
    w.Start(ctx)
    defer w.Stop()

    readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
    defer cancel()
    if err := w.WaitReady(readyCtx); err != nil {
        return err
    }
    <-ctx.Done()
    return nil
}
```

`w.Run(ctx)` 等于 `Start` + `WaitReady` + 阻塞到 `ctx` 结束 + `Stop`，登记失败时返回错误。一个 Worker 同时跑 Workflow 与 Activity 的写法见 `SKILL.md` 的编写流程第 3 步（来源 `docs/examples/startup-workflow/worker/main.go`）。

`RegisterActivities` 与 `WorkflowDefinition` 由生成器写进 `zz_*_gen.go`（见 `codegen.md`）。跨包的 Activity 各自调用所在包的 `RegisterActivities(registry)`。

## worker.New

`worker.New(c client.Client, options worker.Options) (*worker.Worker, error)`：`c` 必须由 `client.Dial` 创建，Namespace、连接与缺省 Identity、Telemetry 都取自它。与 Temporal 不同，没有 task queue 参数（Workflow Task 不分队列，Activity 队列取自 registry 登记的队列或 `ActivityTaskQueues`），也没有 `RegisterWorkflow` / `RegisterActivity` 方法（Workflow 是生成的定义，Activity 在 `activity.Registry` 里）。

## worker.Options

| 字段 | 缺省 | 含义 |
| --- | --- | --- |
| `Role` | 见右 | `worker.RoleBoth` / `RoleWorkflow` / `RoleActivity`；留空时 `Activities` 登记了 Activity 取 `RoleBoth`，否则 `RoleWorkflow` |
| `Workflows` | 空 | 随二进制发布的定义，`Start` 时登记 |
| `Activities` | 空 | 本 Worker 执行的 Activity，契约随路由上报 |
| `ActivityTaskQueues` | registry 登记的队列 | 消费的 Activity Task Queue。为空时取 registry 登记的队列（生成的 `RegisterActivities` 带注解 `queue`，手写用 `activity.RegisterActivityOnQueue`），有不带队列的 Activity 时加上 `default`；显式设置时必须包含 registry 登记的队列，否则 `worker.New` 报错；`ActivityOptions.TaskQueue` 改派的队列需要显式加上 |
| `WorkflowConcurrency` | `1` | 同时处理的 Workflow Task 数 |
| `ActivityConcurrency` | `1` | 每个 Activity Task Queue 的并发槽位 |
| `WorkflowTaskTimeout` | `10s` | 单个 Workflow Task 的执行超时；领取时报给 Server，Server 按它计时 |
| `GracefulStopTimeout` | `10s` | `Stop` 等待在途 Activity 与 Workflow Task 结束的上限 |
| `Identity` | `client.Options.Identity` | Worker 实例标识（pod name、instance ID） |
| `BuildID` | 编译时的 Git revision | Worker 代码版本（镜像版本、Git commit）。为空时取编译时写入二进制的 Git revision（前 12 位，工作区有未提交改动时加 `-dirty`）；二进制里没有 revision（`go run`、`go test`、`-buildvcs=false`、在 Git 仓库外构建）时 `worker.New` panic |
| `ActivityWorkerVersionDefault` | `false` | Activity 没有指定队列、多个队列承接它时优先选这个登记的队列 |
| `ActivityWorkerVersionWeight` | `0` | 同上情况下选择队列的权重；不决定由哪个 build 执行 |
| `ActivityWorkerVersionHeartbeatInterval` | TTL 的一半 | 路由心跳间隔 |
| `ActivityWorkerVersionHeartbeatTTL` | `30s` | 路由登记的存活时间；进程崩溃后路由在 TTL 之后到期（正常 `Stop` 立刻注销） |
| `ExecutionCache` | `nil` | `*worker.ExecutionCacheOptions{MaxEntries, MaxEstimatedBytes, IdleTTL}`，非 nil 时启用 Sticky Execution；需要稳定的 `Identity` |
| `Telemetry` | `client.Options.Telemetry` | OpenTelemetry providers；Worker 的日志写入其中的 `Logger`，未配置时写入 `slog.Default()` |

## Start、WaitReady、Stop

`Start(ctx)` 的内部顺序：

1. 上报本进程的 Activity 路由与契约。
2. 登记 `Workflows` 里的每份定义：新内容成为 latest，已知 hash 保持 latest 不变；任何一份失败都不开始 poll。
3. 启动版本心跳。
4. 开始 poll Workflow Task 与 Activity Task。

`WaitReady(ctx)` 返回前置步骤的结果；启动后总是调用它并处理错误。

`Stop()` 是优雅停止（与 Temporal 相同的语义）：

1. 停止 poll，在途的长轮询立即返回。应答已开始到达的任务照常在宽限期内处理；Server 在连接断开之后才领到的任务立即交还（Workflow Task 记 `WorkflowTaskTimedOut` 并立即重新调度，Activity 租约退回队列），不等 10s 的 Workflow Task 超时。
2. 关闭 `activity.GetWorkerStopChannel(ctx)`；在途任务的 `ctx` 不取消。
3. 等在途 Activity 与 Workflow Task 结束，最多 `GracefulStopTimeout`；期间结束的任务照常上报。
4. 到期后取消仍在运行的任务的 `ctx` 并等它们返回。因停止而结束的 Activity（宽限期内返回 `activity.ErrWorkerStopping`，或 `ctx` 被取消后返回）不上报失败，Worker 调 `POST /runtime/activity/release` 交还 Attempt，Server 以同一序号立即重新派发、不消耗重试次数；未完成的 Workflow Task 不上报，由 Server 的 Workflow Task 超时重新调度。
5. 向 Server 注销本 Worker 的 Activity 路由（这个 build 立刻退出选路；尽力而为，上限 3 秒），关闭 Execution Cache。

`Stop` 可重复、并发调用，都在停止完成后返回；没有在途任务时立即返回；忽略 `ctx` 的 Activity 会让它一直等。部署平台在 `SIGTERM` 后留出大于 `GracefulStopTimeout` 的终止宽限期。进程崩溃时路由靠心跳 TTL 到期，已开始的 Attempt 靠 StartToClose 或 Heartbeat 超时重试。

定义经 Worker 所用 Client 的连接登记到它的 Namespace。

## 路由要点

- Activity 路由登记的标识是 `namespace + activity type + queue + BuildID`，每个 build 的契约分开保存。Activity 任务只按队列投递（Run 固定了目标 Worker 时只交给这个 Identity），不带 build；队列上任一 build 都能领取，开始执行前按它这个 build 的契约对照 Run 当前 Program 的声明，不一致时拒绝（`worker_contract_mismatch`），任务约 1s 后重新投递。
- Run 走到没有可用 Worker 的 Activity 时，Workflow Task 以 `activity_route_unavailable` 失败并按退避重试，Run 显示 Blocked；Worker 上线后自动继续。因此 Activity Worker 与定义的上线先后不影响最终结果，推荐顺序是 Activity Worker 先就绪。
- 指定了 Task Queue（或目标 Worker）且声明了契约的 Activity，在该队列没有在线 Worker 时同样以 `activity_route_unavailable` 退避等待，不写 `ConfigError`；只有在线 Worker 上报的契约不一致才暂停 Run。
- 路由目录在每种部署形态下都存在、规则相同：没有任何 Worker 登记时是空目录，未指定 Task Queue 的 Activity 同样以 `activity_route_unavailable` 等待 Worker 登记；与 Server 同进程运行的 Worker 也向 Server 登记自己的路由。
- Server 缓存路由目录：本副本上的登记与注销立即生效，其他副本上的最多 5 秒后生效。
- Workflow Task 队列不分 Namespace：Workflow Worker 领取全部 Namespace 的 Workflow Task，Client 的 Namespace 决定 Activity 的登记与领取范围及定义登记的 Namespace。Workflow Worker 的配置、History、状态读取凭所领任务的 task token。
- 新旧 build 并存用不同 BuildID；发布顺序见 `versioning.md`。
- `ActivityConcurrency` 对每个队列分别生效；长耗时 Activity 用独立队列。

## 启动 Server

本地内存后端（不需要数据库，重启后数据清空）：

```bash
WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:7400 go run ./server/cmd/tenuto-server
```

- Runtime HTTP 缺省 `:7400`，Console 缺省 `7401`；仓库里 `make dev` 启动完整的本地环境。
- 健康检查：`GET /health/live`、`GET /health/ready`。
- 持久化后端只有 MySQL（`WORKFLOW_BACKEND=mysql-taskq`）。Server 启动时不建表也不改表：首次部署与每次升级都先执行 `tenuto-server migrate`（或 `make migrate`），它只读 `WORKFLOW_DB_DSN` 与 `TASKQ_DB_DSN`（两者的 `parseTime=true`、`loc=UTC`、`interpolateParams=true` 由 Server 设置，字符集用 `utf8mb4`），在 Workflow 库执行 Tenuto 的迁移、在 TaskQ 库执行 TaskQ 的迁移，记录写在 `tenuto_schema_migrations`。迁移只向前、幂等，多个进程同时执行时由 MySQL `GET_LOCK` 依次进行；某条语句失败时报出迁移名与语句序号，排除原因后重新执行从该语句继续。
- Server 启动时检查 schema 版本：库落后（无迁移记录、有未执行或中途失败的迁移）或已执行迁移的 checksum 不一致时启动失败并提示运行 `tenuto-server migrate`；库比二进制新时记警告 `database schema is newer than this binary` 并启动（迁移保持上一个版本可用，滚动升级与回滚期间旧副本可以重启）。
- 改表（贡献者）：在 `server/infra/sql/migrations/` 新增下一个编号的迁移文件，同步更新 `schema.sql`、PO 与 `MysqlCreateTable()`，在 `server/infra/sql/migrate_test.go` 的 `appliedChecksums` 登记新迁移的 checksum。已发布的迁移不再修改。守卫：`schema_guard_test.go`、`migrate_test.go`，MySQL 上的 `TestTenutoMigrationsBuildSchemaSQL`（迁移与 `schema.sql` 的 `SHOW CREATE TABLE` 逐表相同）与 `TestTaskQMigrationsBuildTaskQSchemaSQL`。
- 持久化后端的完整配置项见站点文档 `docs/deploy/server.md`、`docs/reference/configuration.md`。

## 在仓库外的模块里使用

业务模块依赖 `github.com/donutnomad/tenuto`，import `sdk/workflow`、`sdk/activity`、`sdk/apperror`、`sdk/registration`、`sdk/client`、`sdk/worker`。`internal/` 与 `sdk/internal/` 下的包不能从模块外 import。

## 排查

| 现象 | 检查 |
| --- | --- |
| `WaitReady` 报定义登记失败 | 产物是否由当前源码生成；Server 地址与 Namespace |
| Activity 长时间 Scheduled | Worker 拉取的队列（registry 登记的队列或 `ActivityTaskQueues`）是否包含该队列；`Activities` 是否登记了这个 type；`POST /runtime/workflow/list-workers` 里是否有该 Worker |
| Run Blocked：`activity route unavailable` | 该 Namespace 是否有注册了这个 `type` 的在线 Worker |
| `worker.New` panic：`Options.BuildID is empty and the binary carries no VCS revision` | 用 `go build` 在 Git 仓库里构建，或显式设置 `BuildID`（`go run` 与 `go test` 不写入 revision） |
| Run 暂停：`contract mismatch` | Worker 与 Program 是否来自同一次生成；新旧 build 是否用了不同 BuildID（见 `versioning.md`） |
| Workflow Task 反复超时 | 函数体里的纯计算是否过重；调大 `WorkflowTaskTimeout` 或把计算放进 Activity |
