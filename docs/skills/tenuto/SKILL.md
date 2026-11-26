---
name: tenuto
description: 当编写或修改 Tenuto 的 Workflow、Activity、Worker（@Workflow / @Activity 注解、tenuto-workflow-gen 生成、sdk/workflow 与 sdk/activity API），启动 Run 或向 Run 发送 Signal / Query / Update，升级已上线的 Workflow 或 Activity（GetVersion、pinned / auto_upgrade、BuildID、契约），以及诊断暂停或失败的 Run（ConfigError、UpgradeError、nondeterministic、UnknownUpdate、activity route unavailable、生成器报错）时使用。
---

# Tenuto

Tenuto 是 Go 实现的持久化 Workflow 引擎（模块 `github.com/donutnomad/tenuto`）。作者写带注解的 Go，生成器把 Workflow 函数翻译成 WLang Program，Worker 用解释器按 Event History 执行。

## 心智模型

- **Run**：一次 Workflow 执行，有 Run ID、状态（`running` / `completed` / `failed` / `cancelled` / `terminated` / `continued_as_new`）和独立的 Event History。同一 Namespace + Workflow ID 同时只有一个活跃 Run。
- **Event History 与重放**：Workflow 代码每产生一个副作用就写一条事件（Activity、Timer、Signal、Marker…）。Worker 重启、缓存逐出、版本切换后从头重放代码，重放产生的命令逐条与 History 对照（命令种类、ActivityID、ActivityType）。同一份 Program + History 必须得到同一串命令，这就是确定性。
- **Workflow Task 与 Activity Task**：Workflow Task 推进编排（解释器执行 Workflow 函数直到全部协程阻塞，提交命令）；Activity Task 执行原生 Go 的副作用函数。Workflow 函数的 Go 代码本身不在运行期被调用，`sdk/workflow` 的函数全是定义期标记。
- **Program 版本**：每份生成的 `workflow.json` 有内容 hash；每个 Workflow Type 有一个 latest 指针。新 Run 用 latest；运行中的 Run 按自己的 `policy`（`auto_upgrade` 缺省 / `pinned`）决定是否在下一个 Workflow Task 边界换到 latest。
- **Worker BuildID**：Activity Worker 的代码版本，Server 按它分开保存每个 build 的契约，新旧 build 并存靠不同 BuildID。Activity 任务只按队列投递、不带 build，队列上任一 build 都能领取；开始执行前按领取者这个 build 的契约对照 Run 当前 Program 的声明，不兼容时拒绝（`worker_contract_mismatch`），任务约 1s 后重新投递。单个 Activity 调用不能指定 build。
- **契约**：每个 `@Activity` 生成三维契约 `type`、`input` 指纹、`output` 指纹。Workflow 侧随 Program 声明，Worker 侧随注册表上报，Worker 还上报入参出参的 wire 结构。Server 在 Worker 领取 Activity 时按领取者的上报逐维度对照：指纹相同即兼容，不同时按数据流向比较结构（input：Workflow 写、Worker 读；output：Worker 写、Workflow 读；读方可多字段，写方字段读方都要有、类型相同或无损放宽），不兼容的领取被拒绝、任务约 1s 后重新投递；调度不对照契约，Run 不因契约不兼容暂停。Activity 的含义或结构要变时用新 `type`，旧 `type` 保留到没有 Run 再调用它。

## 编写流程

1. **写带注解的 Go**。一个包目录一条 `@Workflow`；Activity 可在同包或独立包。

   ```go
   // 来源：docs/examples/startup-workflow/workflow.go
   package startupflow

   import (
       "context"
       "time"

       workflow "github.com/donutnomad/tenuto/sdk/workflow"
   )

   type GreetInput struct{ Name string `json:"name"` }
   type GreetOutput struct{ Message string `json:"message"` }
   type SayHelloInput struct{ Name string `json:"name"` }
   type SayHelloOutput struct{ Message string `json:"message"` }

   // @Workflow(type="startup-greeting",name="Startup Greeting",policy=auto_upgrade,version=v1)
   func Greet(ctx workflow.Context, in GreetInput) (GreetOutput, error) {
       out, err := CallSayHello(ctx, SayHelloInput{Name: in.Name},
           workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
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

   `CallSayHello` 由生成器写出；桥名取自 Activity `type` 的最后一段（`startup.say-hello` → `CallSayHello`），第一次生成前编辑器会报未定义。每个 Activity 调用都要设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`，缺少时生成器报出调用点，Server 也拒绝这条调度命令。

2. **运行生成器**（每次改动 Workflow、Activity 签名或注解之后）。业务模块先引入 SDK 并把生成器登记为 tool，再在 Workflow 包里写一条 `//go:generate`：

   ```bash
   go get github.com/donutnomad/tenuto@latest
   go get -tool github.com/donutnomad/tenuto/compiler/cmd/tenuto-workflow-gen@latest
   ```

   ```go
   //go:generate go tool tenuto-workflow-gen generate .
   ```

   之后从模块根目录执行 `go generate ./...`；也可以直接调用 `go tool tenuto-workflow-gen generate ./path/to/pkg`。生成产物与源码一起提交。完整接入步骤见文档站「接入已有项目」（`docs/quickstart/integrate.md`）。

   在 Tenuto 仓库内使用统一入口 `go generate ./...`（唯一的 `//go:generate` 在 `server/cmd/tenuto-server/main.go`，新包目录加到那条指令的参数里）。`go build` 不执行生成。失败时不写任何 JSON 产物，按报错的 `file:line` 修改源码。

3. **接上 Worker**。一个 Worker 同时执行本包的 Workflow 与 Activity：`Start` 先上报 Activity 路由，再登记随二进制发布的定义，然后开始拉取任务。

   ```go
   // 来源：docs/examples/startup-workflow/worker/main.go
   c, err := client.Dial(client.Options{HostPort: url, Namespace: namespace})
   if err != nil {
       return err
   }
   defer c.Close()

   registry := activity.NewRegistry()
   if err := startupflow.RegisterActivities(registry); err != nil {
       return err
   }
   w, err := worker.New(c, worker.Options{
       Workflows:                    []registration.Definition{startupflow.WorkflowDefinition()},
       Activities:                   registry,
       Identity:                     "startup-worker-1",
       BuildID:                      "startup-v1",
       ActivityWorkerVersionDefault: true,
   })
   if err != nil {
       return err
   }
   w.Start(ctx)
   defer w.Stop()
   if err := w.WaitReady(readyCtx); err != nil {
       return err
   }
   <-ctx.Done()
   ```

   import：`sdk/activity`、`sdk/client`、`sdk/registration`、`sdk/worker`（均在 `github.com/donutnomad/tenuto/` 下）。`w.Run(ctx)` 是 `Start` + `WaitReady` + 阻塞 + `Stop` 的简写。Worker 拉取的 Activity 队列来自 registry（`RegisterActivities` 带注解 `queue`），不写 `ActivityTaskQueues`；显式写时必须包含这些队列，否则 `worker.New` 报错。`tenuto-server` 默认内嵌 Workflow Worker，此时业务进程可以只起 `Role: worker.RoleActivity` 的 Worker 并把 `Workflows` 放进它的选项（见 `references/workers.md`）。

4. **启动并与 Run 交互**（同一个 Client `c`）：

   ```go
   run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: workflowID}, "startup-greeting", map[string]any{"name": "tenuto"}) // Workflow Type；ID 留空由 SDK 生成
   err = c.SignalWorkflow(ctx, workflowID, "", "approve", map[string]any{"ok": true})
   value, err := c.QueryWorkflow(ctx, workflowID, "", "progress") // value.Get(&progress)；拒绝时 *client.QueryFailedError
   var out GreetOutput
   err = run.Get(ctx, &out) // 等 Run 结束；跟随 continue-as-new 与重试；失败时 *client.WorkflowExecutionError
   ```

   Update 用 `c.UpdateWorkflow` → `handle.Get`。载荷里的整数经 HTTP 原样保留（int64 / uint64），超过 2^53 不丢精度。SDK 自动重试连接失败、连接中断与 429/502/503/504（Server 滚动重启时）；Start / Signal 带 SDK 生成的 request ID、Update 带 Update ID，重发不重复生效，Reset、Schedule Create/Trigger、批量操作启动只在请求确定没送达时重试（`client.Options.Retry` 调整）。完整签名、错误、重试归类、HTTP 请求体与状态码见 `references/client-api.md`。

## 硬规则

确定性：

- 当前时间用 `workflow.Now(ctx)`，等待用 `workflow.Sleep(ctx, d)`，随机用 `workflow.RandomUUID` / `RandomInt` / `SideEffect`：墙钟与随机数每次重放不同。
- IO、环境变量、数据库、HTTP 放进 Activity。Workflow 里可以调用一组列明的确定性标准库函数（`strings` 的 `ToUpper`/`Split`/`TrimSpace` 等、`strconv.Itoa`/`Atoi`/`ParseInt` 等、`fmt.Sprintf`/`Sprint`/`Errorf`、`math.Max`/`Round`/`Sqrt` 等），结果与 Go 相同；清单之外的标准库函数（`sort.*`、`time.Parse`、`math.Pow`、`strconv.Quote`）与其他包的函数生成期报 `file:line`。清单与 `fmt` 格式串规则见 `references/go-subset.md`。
- Workflow 可以调用同包普通函数与值接收者方法（含其他文件里的、递归、带 `workflow.Context` 调度 Activity 的）：函数体一起翻译进产物，改它就改变内容版本。泛型、可变参数、指针接收者、包级变量、函数当值在生成期报错。
- 决定 Activity 调度顺序、协程启动顺序、Selector case 顺序的数据用 slice。Workflow 里的 `range m` 按 key 升序遍历（确定、可重放），但升序通常不是业务顺序。
- 翻译后的 WLang 与 Go 源码行为一致（结构体是值、slice 共享底层数组、nil slice 输出 `null`、`for` 条件每轮求值）；做不到一致的写法在生成期报错（如定长数组 `[N]T`，改用 slice），清单见 `references/go-subset.md`。
- Workflow 接触的数据类型（子 Workflow / Activity / continue-as-new 实参、Signal、Update、Query 载荷，任意嵌套深度，含递归类型与契约包类型）都登记进类型表；channel、函数、嵌入字段等无法编码的类型在生成期按 `file:line` 与类型路径报错，见 `references/go-subset.md` 的「数据类型」。
- `workflow.Await` 的条件闭包与 Query Handler 只读 Workflow 内存变量：它们在重放时反复求值，Query 产生的命令被丢弃。
- 对已有运行中 Run 的 Workflow，新增、删除、重排 Activity / Timer / Child / `SideEffect` / `RandomUUID` / `RandomInt` / 协程 / Selector case 需要 `GetVersion` 保护或 `pinned`：这些改动改变命令序列。

生成器拒绝的写法（报错带 `file:line` 和替代写法）：

| 写法 | 替代 |
| --- | --- |
| `go` 语句 | `workflow.Go(ctx, func(ctx workflow.Context) { ... })` + `workflow.NewWaitGroup(ctx)` |
| `chan`、`ch <- v`、`<-ch` | `workflow.NewChannel(ctx)` / `NewBufferedChannel(ctx, n)`，`ch.Send(ctx, v)`，`ch.Receive(ctx, &v)` |
| `select` | `workflow.NewSelector(ctx)` 的 `AddReceive` / `AddFuture` / `AddDefault` + `Select(ctx)` |
| `sync`、`sync/atomic` | `workflow.NewWaitGroup(ctx)`、`workflow.Await(ctx, cond)`；协程协作式调度，共享变量直接读写 |
| `time.Now` / `Since` / `Until` | `workflow.Now(ctx)` 与 `time.Time` 的 `Sub` |
| `time.Sleep` / `After` / `NewTimer` / `Tick` / `NewTicker` / `AfterFunc` | `workflow.Sleep(ctx, d)` |
| `math/rand`、`math/rand/v2`、`crypto/rand`、`github.com/google/uuid` | `workflow.RandomInt`、`workflow.RandomUUID`、`workflow.SideEffect` |
| `os`、`os/*`、`net`、`net/*` | Activity |
| `errors.New` 等 `errors` 包函数 | `fmt.Errorf`、`workflow.NewApplicationError`；判断用 `workflow.IsApplicationError` |
| 清单外的标准库函数与类型（`sort.Strings`、`strings.Builder`）、其他包的函数 | Activity |
| 同包函数当值 `f := helper` | 闭包 `func(x int) int { return helper(x) }` |
| `for {}`、`for cond {}`、`goto`、`fn(xs...)`、`float64(v)` / `string(v)` 等转换 | 三段式计数 `for` 或 `range`；`int` / `int64` / `time.Duration` 转换；其余放进 Activity |

`time` 包可用的部分：常量（`time.Second`、`time.RFC3339` 等）、`time.Duration` 整数运算、`time.Time` 值的 `Add` / `Sub` / `Before` / `After` / `Equal` / `Unix` / `UnixMilli` / `Format` / `String`；其他 `time.Time` 方法与 `time.Duration` 的方法生成期报错。

结构与契约：

- Activity 签名固定为 `func(context.Context, In) (Out, error)`，`In` / `Out` 是 struct 或 `map[string]any`：线上是 JSON object，单个值包进 struct 字段。
- Workflow 里只通过桥 `CallX` 或 `workflow.ExecuteActivity` 调度 Activity，桥只能是 `out, err := CallX(...)`、`out, err = CallX(...)`、`return CallX(...)`、`if _, err := CallX(...); err != nil` 的完整右值：直接调用 `@Activity` 实现函数、嵌在表达式里、丢弃全部返回值都被拒绝。
- `SetUpdateHandler` / `SetQueryHandler` 在主函数第一次阻塞之前注册：主协程第一次阻塞（或返回）时还没有同名 Handler 的 Update 以 `UnknownUpdate` 失败完成。
- Workflow 在主函数返回时结束：仍在等待的 `workflow.Go` 协程、未完成的 Activity / Timer 被放弃（Timer 状态置为 `run_closed`，不写事件、不再触发），执行中的 Update 以 `AcceptedUpdateCompletedWorkflow` 失败结束，仍在运行的子 Workflow 按父关闭策略处理（默认终止，`workflow.ChildWorkflowOptions{ParentClosePolicy: workflow.ParentClosePolicyAbandon}` 让它继续运行）。需要它们的结果就在返回前 `wg.Wait` / `Get` / `Receive`。
- 传给 `workflow.Go`、`wg.Go`、`SetUpdateHandler`、`SetQueryHandler`、Selector 的函数写成内联闭包；协程内使用闭包参数 `ctx`：它携带协程路径，缺省 ActivityID 等原语 ID 由此派生。
- `zz_*_gen.go`、`workflow.json` 由生成器维护，改源码后重新生成：手改会让内容 hash、契约指纹与代码不一致。
- 一个包目录一条 `@Workflow`；`@Workflow` / `@Activity` 写在包级函数的 `//` 行注释里：方法、块注释、未知 key 都被拒绝。
- 每个 Activity 调用设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`：attempt 开始后 Worker 立即确认队列租约，attempt 的存活只由这两个超时与 `HeartbeatTimeout` 判定，Worker 消失后靠 StartToClose（或 heartbeat）超时重试。缺少时生成器报出调用点；选项来自变量时由 Server 拒绝调度命令，Workflow Task 以 `bad_schedule_activity_attributes` 失败。
- Activity 至少执行一次（at-least-once），不保证只执行一次：已开始的 attempt 不会因队列重投再执行，但超时重试、Worker 在上报结果前崩溃、Worker 停止时交还 attempt 都会让同一业务操作再执行。每个 Activity 都必须幂等。
- Activity 要可取消就必须 Heartbeat：取消只经 `activity.RecordHeartbeat` 的响应送达，不 Heartbeat 的 Activity 只在返回或超时时结束。Worker 对心跳节流（最小间隔为 `HeartbeatTimeout` 的 80%，未设置时 30s，上限 60s；间隔内只发最新 details，失败/取消/交还结束时补发），可以频繁调用。Heartbeat 不写 History，只更新 Activity 状态（`activity-states` 的 `lastHeartbeat`）；超时的 Attempt 的最后一次详情在 `ActivityTimedOut.lastHeartbeatDetails`，被 Workflow Reset 打断的在新 Run 的 `ActivityFailed.lastHeartbeatDetails`（Console 的 Attempt 详情显示为 Last Heartbeat）。收到 Workflow 的取消后返回 `ctx.Err()` 或取消错误即确认取消（`ActivityCancelled`）。
- `Worker.Stop` 是优雅停止：停 poll，关闭 `activity.GetWorkerStopChannel(ctx)`，等在途任务最多 `GracefulStopTimeout`（缺省 10s），到期才取消 `ctx`。与 Stop 同时到达的任务不会等超时：应答已到达的照常处理，没送到的由 Server 立即交还。长 Activity 监听停止 channel，Heartbeat 保存进度后返回 `activity.ErrWorkerStopping`：attempt 交还 Server，以同一序号重新派发，不消耗重试次数。
- Activity 默认重试，缺省策略与 Temporal 相同、由 Server 补齐：不限次数，`InitialInterval` 1s、`BackoffCoefficient` 2.0、`MaxInterval` 100 × InitialInterval；部分设置的 `workflow.RetryPolicy` 其余字段取缺省值；只执行一次写 `MaxAttempts: 1`。重试在成功、不可重试错误、`MaxAttempts`、`ScheduleToCloseTimeout` 到期（或下一次 Attempt 会落在它之后）时停止；`ScheduleToStartTimeout` 约束每个 Attempt（从首次调度、退避到期或 reset 时起算），到期后直接失败，不重试。所有超时与间隔字段都是 `time.Duration`。
- 调用点的 `ActivityOptions`（`TaskQueue`、超时、重试）只作用于这一次调用，不改变同一 Activity 其他调用点的路由。`ActivityOptions` 没有 `BuildID`。
- `ActivityOptions.ActivityID` 是这次调用在 Run 内的业务 ID（同 Temporal），进 `ActivityScheduled`、`activity.GetInfo(ctx).ActivityID`、Console 与 `reset-activity`；为空时取调用位置（`#a1`、`/0#a2`）。可以运行期计算（`fmt.Sprintf("charge-%d", i)`），但必须确定：重放时 ID 变了是 `non_deterministic_error`。与同一 Run 里仍未关闭的 Activity 重复时 Workflow Task 以 `schedule_activity_duplicate_id` 失败；前一个关闭后可以再用。
- Activity 因超时失败时，`workflow.TimeoutTypeOf(err)` 返回超时种类（`TimeoutTypeStartToClose` 等）；History 的 `ActivityTimedOut.timeoutType` 记录同一个值。
- 幂等键 = 操作名 + 这一次操作的身份：优先输入里的业务 ID；没有时在 Workflow 里 `workflow.RandomUUID` 生成后随输入传入；或用 `activity.GetInfo(ctx)` 的 `WorkflowID` + `ActivityID`（不适用于 continue-as-new 或复用 Workflow ID 的 Workflow）。`Attempt`、时间戳、Activity 里生成的随机值不能进键。见 `references/activities.md#幂等`。
- Query handler 只读：调用 `Await`、`Sleep`、`Future.Get`、channel、Selector、`WaitGroup.Wait` 或 `workflow.Go` 时该次 Query 被拒。
- Workflow Task 的超时由 Server 按领取它的 Worker 的 `WorkflowTaskTimeout` 计时：Worker 领取后崩溃，Server 到期记 `WorkflowTaskTimedOut`、结束 sticky 会话并重新调度，Run 不会停住。
- Workflow 代码与 Update handler 里的 panic（越界、nil 解引用、`panic(...)`，含 `_ = items[i]`、`_ = p.Field`；以及解释器 panic）按 Temporal 缺省的 BlockWorkflow 处理：Workflow Task 以 `workflow_panic` 失败，Run 保持 `running`、显示 Blocked 并退避重试，修复代码发布后继续。只有 Workflow 函数显式返回的 error 让 Run `failed`；Update handler 显式返回的 error 让这个 Update 失败。
- Sticky 与 Temporal 相同：Worker 完成 Workflow Task 后 Run 绑定到它，绑定不随时间失效；之后调度的 Workflow Task 投给它，从调度时刻起 5s 内未被领取才改投普通队列并清除绑定；Workflow Task 超时或失败时清除绑定。
- 单个载荷（Workflow 输入与结果、Signal / Update 参数、Activity 输入、结果与心跳详情、子 Workflow / Continue-As-New 输入、Marker）上限 `WORKFLOW_PAYLOAD_SIZE_LIMIT_BYTES`（默认 2 MiB，最大 3 MiB）：入口请求超限 400；命令超限（单条命令的载荷含 Update 结果，或一次 Workflow Task 的命令合计超过 3.5 MiB，或 Server 以 413 拒绝提交）时 Workflow Task 以 `payload_size_limit_exceeded` 失败（Run Blocked，与 Temporal 的 terminate 不同，可恢复）；Activity 结果或心跳超限时 Attempt 以不可重试的 `PayloadSizeLimitExceeded` 失败。大数据放外部存储、传引用。Task Token 不含载荷，Activity 调用只发 token 与 Worker 身份（见 `references/operations.md`）。
- Worker 收到 Server 不可重试的 4xx 时不立即退回投递：`begin` 按连续拒绝次数退避（1s 起，最长 1 分钟）后退回；Activity 结果被拒绝改报不可重试的 `ActivityResultRejected`；Workflow Task 提交被 413 拒绝时改报 `payload_size_limit_exceeded`，其他上报被拒绝时投递留到可见性超时。`begin` 应答丢失后的重发带同一个 `request_id`，Server 返回原来的开始结果，Activity 照常执行。
- 无故障时投递不丢：长轮询期限到期、调用方取消、多个 matching owner 竞速时，TaskQ 已签发的租约总被读回，没人要的立即放回队列；matching owner 宕机时入队换到其他 owner 的分区；启动恢复在后台运行，不占 `WORKFLOW_STARTUP_TIMEOUT`。
- `blocked`（get-run / list 的派生字段）只描述 `running` 的 Run 的 Workflow Task 连续失败；Run 结束后清空，`blocked_only` 列表不返回已结束的 Run。Console 的失败 Run 显示 Failure 卡片，Pending 页签取自 `activity-states`；Server 连不上时提示 `Cannot connect to Tenuto Server at <地址>.`，与 Server 返回的错误分开显示（见 `references/operations.md`）。
- 跨副本等待：Update 等 `Completed` 与 `run.Get`（`wait-closed` 长轮询）在受理请求的副本上等待；其他副本提交的完成或 Run 结束最多晚 100ms 被看到（有等待者时每副本每 100ms 一条批量读取，没有等待者时不读库）。副本重启时等待中的调用返回连接错误，对同一个句柄再调用一次（见 `references/messaging.md#跨副本等待`）。
- 数据库访问量：TaskQ 分区的行都已投递或确认时 poll 与可见性扫描不读库；内嵌 Workflow Worker 的 Workflow Task 长轮询持续 `TASKQ_POLL_TIMEOUT`（默认 30s）。每个 Workflow 的语句数按入口分组看 `TestStatementBudgetPerWorkflow`（`TENUTO_TEST_MYSQL=1`，见 `references/operations.md`），事务外读 Run 与 execution state 的次数看 `TestRepositoryReadsPerWorkflow`（内存后端）。Server 给 `WORKFLOW_DB_DSN` 与 `TASKQ_DB_DSN` 设置 `parseTime=true`、`loc=UTC`、`interpolateParams=true`，字符集用 `utf8mb4`。
- Server 补偿分两档，都只在 leader 上运行：周期补偿（`WORKFLOW_RECONCILE_INTERVAL`，默认 30s）按到期索引只处理有到期或待处理事项的 Running Run，代价与 Running Run 总数无关；全量补偿（`WORKFLOW_RECONCILE_FULL_SCAN_INTERVAL`，默认 10m）逐页检查全部 Running Run，补建崩溃后缺失的子 Run，补做终态传播、终态写回、父关闭策略与子 Run 取消请求。
- 投递在队列里丢失时 Run 不会停住：已入队却超过等待时长（30s 宽限期起按派发次数翻倍，最长 1 小时）仍没被领取的 open Workflow Task 与 Scheduled Activity attempt 由 leader 的 reconciler 再投一份（日志 `task dispatched but not claimed; redelivering`）。已关闭 WFT 的陈旧投递由 Server 领取时确认丢弃，不交给 Worker。
- 配置了 Workflow RetryPolicy 的 Run 不再重试时，终态 `WorkflowFailed` 的 `retryState` 写明原因（`MaximumAttemptsReached`、`NonRetryableFailure`、`Timeout`）；没有 RetryPolicy 时为空。重试 Run 在退避期内收到的 Signal 只写入 History、等退避到期一并处理；SignalWithStart、取消请求与 Update 让重试 Run 立即开始（同 Temporal 默认行为）。
- 新旧 Activity build 并存时 BuildID 必须不同：相同 BuildID 的登记互相覆盖，引擎分不出新旧契约。
- 旧 build 下线前先排空只有它满足契约的 Activity（`pinned` 在旧 Program 上的 Run）：下线之后这些任务留在队列里被新 build 反复拒绝，直到旧 build 回来或 `ScheduleToStartTimeout` 到期。契约不变、或只做兼容改动（入参新增字段）的发布没有这个限制。
- 取消按 Temporal 语义交给 Workflow 代码：根 Context 被取消，已取消 ctx 上的 Activity / `Sleep` / 子 Workflow 返回取消错误，代码继续运行。补偿发在 `workflow.NewDisconnectedContext(ctx)` 上；返回取消错误 → `cancelled`，返回 nil → `completed`。局部取消用 `workflow.WithCancel`。
- 内置认证缺省关闭：`WORKFLOW_HTTP_AUTH_TOKEN`（或 `_FILE`，逗号分隔多个用于轮换，每个至少 16 字节）让 `/runtime/*` 与 `/admin/*` 要求 `Authorization: Bearer`，缺失或不对时 401；`WORKFLOW_HTTP_TLS_CLIENT_CA_FILE` 在握手阶段要求客户端证书（需同时配服务端证书），探针改用明文的 `WORKFLOW_HEALTH_ADDR`。`/health/*` 不校验，pprof 监听不校验。SDK 用 `client.Options.AuthToken` / `TLS`（Worker 沿用 Client），401 返回 `client.ErrUnauthenticated` 且不重试；Console 用服务端变量 `TENUTO_SERVER_AUTH_TOKEN`、`TENUTO_SERVER_TLS_*`，自身没有登录，对外开放时放在 SSO 之后，token 缺失或不对时页面提示 `Tenuto Server rejected the Console's credentials (HTTP 401).`；`tenuto-workflow-gen -url` 用环境变量 `TENUTO_AUTH_TOKEN`。认证不授权：通过认证的调用方可以读写全部 Namespace、登记 Program、冒充 Worker、调用 `/admin/partitions`（分区数只增不减，上限 1024），按调用方分权在网关按 `docs/deploy/security.md` 的路径分组做。所有 POST 必须带 `Content-Type: application/json`，否则 415；Run List 的 `limit` 最大 1000。WLang 限制单个字符串 16 MiB、数组 10 万元素、map 10 万键，超出时 Run 以 `[E_BUDGET]` 失败、副本照常运行；上限针对单个值，只登记受信流水线构建的 Program。构建 Server 用 `go.mod` 指定的 `toolchain go1.27.4`（修复 `net/http` 的 GO-2026-6859 等标准库公告），`GOTOOLCHAIN=local` 的环境自备 1.27.4 或更高。
- 启动时 Workflow Type（`@Workflow(type=...)`）与 Workflow ID 是两个参数。同一 Namespace + Workflow ID 同时只有一个活跃 Run，再次 `StartWorkflow` 返回 `current run conflict`；同一定义并行多个实例就给每个实例不同的 Workflow ID，或留空让 SDK 生成。

## 我要做 X

| 需求 | API | 参考 |
| --- | --- | --- |
| 声明 Workflow / Activity、运行生成、读懂产物 | `@Workflow`、`@Activity`、`tenuto-workflow-gen generate` | `references/codegen.md` |
| 确认某段 Go 能否写进 Workflow | — | `references/go-subset.md` |
| 调度 Activity、队列如何决定、超时、重试、心跳、取消、Worker 停止、异步完成、幂等 | `CallX`、`workflow.ExecuteActivity`、`workflow.ActivityOptions`、`workflow.TimeoutTypeOf`、`activity.GetInfo`、`activity.RecordHeartbeat`、`activity.GetWorkerStopChannel`、`activity.ErrWorkerStopping`、`activity.ErrPending` | `references/activities.md` |
| 类型化错误与分支 | `apperror.New*`、`workflow.NewApplicationError*`、`workflow.IsApplicationError` | `references/errors.md` |
| 并行、等待最先完成的一个 | `workflow.Go`、`NewWaitGroup`、`NewChannel`、`NewSelector`、`Await` | `references/concurrency.md` |
| 接收外部事件、暴露状态、带结果的命令 | `workflow.WaitSignal`、`SetQueryHandler`、`SetUpdateHandler` | `references/messaging.md` |
| 等待、当前时间、随机值、一次性求值 | `workflow.Sleep`、`Now`、`RandomUUID`、`RandomInt`、`SideEffect`、`MutableSideEffect` | `references/time-and-randomness.md` |
| 拆子流程、父关闭策略、控制 History 长度 | `workflow.ExecuteChildWorkflow`、`ChildWorkflowOptions`、`ContinueAsNew`、`GetInfo` | `references/child-and-continue-as-new.md` |
| 取消（交给 Workflow 代码）、取消后补偿、局部取消、终止、业务时限 | `CancelWorkflow`、`workflow.WithCancel`、`workflow.NewDisconnectedContext`、`workflow.IsCanceledError`、`WaitForCancellation`、`TerminateWorkflow`、Timer + `Await` | `references/cancellation-timeouts.md` |
| 修改已上线的 Workflow / Activity | `workflow.GetVersion`、`policy`、`version`、BuildID | `references/versioning.md` |
| 配置与部署 Worker、优雅停止、启动 Server、MySQL schema 迁移（`tenuto-server migrate`）与改表流程 | `worker.New`、`worker.Options`、`Worker.Run` / `Stop`、`activity.NewRegistry` | `references/workers.md` |
| 从调用端启动、等待结果、查询、Signal、Update、管理 Run | `client.Dial`、`client.Client`、`WorkflowRun.Get`、`/runtime/workflow/*` | `references/client-api.md` |
| 周期启动、列出一个 Schedule 的全部 Run | `CreateSchedule` 等、`TemporalScheduledById` | `references/schedules.md` |
| 按业务字段检索 Run | `UpsertSearchAttributes`、`ListRuns`、保留键 | `references/search-attributes.md` |
| Reset（含重置点上未结束的 Activity / Timer 怎样带进新 Run、`ResetWorkflowError`）、批量操作（异步、可取消）、恢复暂停的 Run | `ResetWorkflow`、`StartBatchOperation`、`CancelBatchOperation`、`SetRunPolicy` | `references/operations.md` |
| 上线前确认保证、生产必需配置、已测场景与实测数字、已知限制 | — | `references/operations.md#生产就绪` |
| 多副本故障注入（kill -9 / SIGSTOP、数据库断连与卡死、副本间分区后核对结果与 History）、长时间浸泡、滚动升级演练、压测与 CPU profile；matching 广播地址与监听地址不同 | `cmd/tenuto-cluster failover` / `soak` / `upgrade` / `bench`、`BenchmarkHarness`、`WORKFLOW_PPROF_ADDR`、`TASKQ_MATCHING_ADVERTISE_ADDR` | `references/operations.md#多副本故障注入与压测` |

## 升级决策

缺省策略是 `auto_upgrade`。发布顺序的基准：新 Activity Worker（新 BuildID）先上线，新 Program 后上线；一个进程同时带 Activity 与 `Workflows` 时 `Start` 内部已按「上报路由 → 登记定义 → poll」执行。内容在开始 Workflow Task 的事务里按 Run 此刻的策略选择，`SetRunPolicy` 返回后所有 Server 副本都按新策略选版；固定目标 Worker 的 Run 在冲突无法判定时本次不换版本。细节与出处见 `references/versioning.md`。

| 改动 | 写什么 | 发布顺序 | `pinned` Run | `auto_upgrade` Run |
| --- | --- | --- | --- | --- |
| Activity 内部实现（结构、语义不变） | 只改实现 | 滚动发布 Worker | 无影响 | 无影响 |
| Activity 输入加字段 | 加字段，`type` 不变（指纹已变）；实现把零值当「未提供」 | 新 build 上线 → 新 Program；旧 build 可以随时下线 | 留在旧 Program，新旧 build 都能执行（入参兼容，新字段是零值） | 下一个 Workflow Task 边界切到新 Program，由新 build 执行 |
| Activity 输出加字段 | 加字段，`type` 不变；Workflow 把零值当「未提供」 | 新 build 与旧 build 并存 → 新 Program → 旧 Program 上的 `pinned` Run 跑完后下线旧 build | 留在旧 Program，任务由旧 build 执行（旧 Workflow 不认识新字段，新 build 领到时被拒绝）；只剩新 build 时任务留在队列里等旧 build，改成 `auto_upgrade` 后由新 build 领取 | 下一个 Workflow Task 边界切到新 Program，由新 build 执行 |
| Activity 语义变了、结构没变 | 新 `type`（如 `fulfillment.quote-taxed`），旧 `type` 的实现原样保留，两个实现注册在同一个 Worker；Workflow 改调新 `type`，在途 Run 的 History 里已记过旧 `type` 的位置用 `GetVersion` 分支 | Worker（含两个 type）→ 新 Program → 不再有 Run 调用旧 `type` 后删除旧实现 | 继续调旧 `type`，新旧 build 都能执行 | 下一个 Workflow Task 边界切到新 Program，之后调度新 `type` |
| Activity 破坏性变更（字段改名、改类型、删除），有 Run 会跨越这次改动 | 新 `type`（如 `order.charge-v2`）+ Workflow 里 `GetVersion` 分支；新旧实现注册在同一个 Worker | Worker（含两个 type）→ 新 Program | 继续调旧 `type` | 重放时 `GetVersion` 读回旧版本走旧 `type`；新执行走新 `type` |
| 改包名、Go 类型名、函数名（Workflow id、Activity type、JSON 字段名与类型不变） | 直接改，重新生成 | 发布新 Program；Activity 契约不变，新旧 build 互相承接 | 留在启动时的 Program | 下一个 Workflow Task 边界切换，新类型表解码旧 JSON 载荷，重放通过 |
| 改 JSON 字段名、删字段、改字段类型（Signal、Update、Workflow 输入、Activity / 子 Workflow 结果、SideEffect 的类型） | 分三步：新版本保留旧字段并新增新字段、代码两边都读 → 发送方改写新字段 → 记录过旧载荷的 Run 结束后删旧字段（`references/versioning.md`「换版前的载荷检查」） | Activity 类型改动同时按契约顺序发布 | 留在启动时的 Program | 换版前检查 History 里已有的载荷（含 Selector 回调里 `Get` 的 future、常量折叠出的名字）：新类型读不回（字段缺失、类型不兼容）时不换版，Run 以 `ConfigError`（`auto upgrade to ... refused: payloads recorded in history ...`）暂停；名字或类型无法静态确定的解码处在这类载荷的类型变了时同样暂停（`... the payload check cannot prove ...`）；只加字段照常换版 |
| 兼容的 Workflow 改动（命令序列对所有存量 History 不变） | 直接改，重新生成 | 发布新 Program | 留在启动时的 Program | 下一个 Workflow Task 边界切换，重放通过 |
| 不兼容的 Workflow 改动（命令序列改变） | `GetVersion(ctx, changeID, workflow.DefaultVersion, 1)` 保留旧分支，或把 Workflow 声明为 `policy=pinned` | 用旧 History 做重放验证 → Worker → 新 Program → 存量 Run 完成后提高 `minSupported` 删旧分支 | 留在旧 Program；旧 Program 用到的 Activity 仍要有 Worker | 有 `GetVersion`：重放通过。没有：Workflow Task 以 `non_deterministic_error` 失败，Run 记 `UpgradeError`，内容保持切换前的版本并按退避重试，发布兼容内容或改成 `pinned` 后恢复 |

## 故障排查

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| Run 仍是 `running`，`config_error` 以 `workflow task cannot be prepared: ` 开头（例如 `workflow config not found: ...`、`invalid branch token`） | Server 准备这个 Run 的 Workflow Task 时遇到每次都会重现的失败（Run 采用的 Program 版本或定义读不到、Run 数据损坏），暂停 Run 而不是反复重投 | 修复原因（重新登记那一版 Program，或修复数据）后 set-policy 恢复；`auto_upgrade` 恢复时换到 latest |
| Run 仍是 `running`，`config_error` 为 `auto upgrade to <version> refused: payloads recorded in history cannot be read by the candidate program: signal "approve": order.Approval -> order2.Approval: order_id: field is missing in the new type (removed or renamed); ...` | `auto_upgrade` 的 Run 换版前发现 History 里已有的载荷在 latest 的类型下读不回（字段删除、改名、改类型），没有换版 | set-policy `pinned` 在当前内容上跑完；或发布保留旧字段的版本后 set-policy `auto_upgrade` |
| `upgrade_error` 为 `auto upgrade to <version> failed at workflow task N: non_deterministic_error: ...; retrying with backoff`，Console 黄色横幅 | `auto_upgrade` 的 Run 用新 Program 重放旧 History 失配 | 发布兼容内容（补 `GetVersion` 分支），下一次重试自动接手；或 set-policy `pinned` 留在旧内容 |
| Workflow Task 失败，原因含 `nondeterministic workflow: history event is ..., replay command is ...` / `missing replay command` / `extra replay command` | 代码产生的命令与 History 不同：改了命令序列、用了不确定的数据源、`GetVersion` 返回值落在 `[min, max]` 之外 | 恢复旧命令序列或补 `GetVersion` 分支后重新发布；Run 在下一次 Workflow Task 自动继续。回滚旧代码时 History 里的版本大于旧 `maxSupported` 同样报此错 |
| Update 的 `handle.Get` 返回 `*apperror.Error`，`Type() == "UnknownUpdate"`，message `no update handler registered for "<name>"` | 名称不一致，或 Handler 在主函数第一次阻塞之后才注册 | 对齐名称；把 `SetUpdateHandler` 移到函数开头 |
| Update 停在 Accepted | Handler 已开始执行，停在 `Await`、Activity 或 Timer 上 | 用 Query `__stack_trace` 看 Run 当前等待的 Activity 或 Timer（`waiting`、`steps`）；推进它等待的事件 |
| Update 的 `handle.Get` 返回 `*apperror.Error`，`Type() == "AcceptedUpdateCompletedWorkflow"` | Handler 完成前 Run 已关闭（主函数返回、失败、取消、终止、continue-as-new） | 让主函数在 Handler 完成前保持运行（`Await` Handler 设置的完成标志） |
| Run 显示 Blocked，Workflow Task 失败原因 `activity_route_unavailable`，`activity route unavailable: ... is not registered in activity route catalog` / `has no active route` | 定义与调用处都没有声明队列，该 Activity `type` 在这个 Namespace 没有在线 Worker | 启动注册了该 `type` 的 Activity Worker（核对 Namespace、registry 登记的队列或 `ActivityTaskQueues`、`ActivityWorkerVersionRegistry` 非空）；Run 按退避自动继续 |
| Activity 长时间 Scheduled，Run 不暂停，Server 日志反复出现 `activity claim refused: worker contract is incompatible`，`err` 为 `activity "..." contract mismatch on worker "..." ...: input expected "wire:..." actual "wire:..."` | 队列上在线的 build 契约都与 Run 当前 Program 的声明不兼容（常见于 `pinned` Run 需要的旧 build 已下线） | 让契约兼容的 build 重新上线，它领取下一次投递，Run 自动继续；或 set-policy `auto_upgrade` 切到最新 Program 后由新 build 领取（前提是新 Program 能重放已有 History）；或给 Activity 设置 `ScheduleToStartTimeout` 限定等待 |
| Run 显示 Blocked，Workflow Task 失败原因 `bad_schedule_activity_attributes`，`activity "..." (type ...) sets neither StartToCloseTimeout nor ScheduleToCloseTimeout` | 调度 Activity 时没有设置关闭超时（选项来自变量，生成期检查不到） | 给该调用的 `ActivityOptions` 设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`，重新生成并发布；Run 按退避自动继续 |
| Run 显示 Blocked，Workflow Task 失败原因 `schedule_activity_duplicate_id`，`activity "..." (type ...): an activity with this ActivityID is still open in this run` | `ActivityOptions.ActivityID` 与同一 Run 里仍未关闭的 Activity 重复（含同一批里两次调度同一 ID） | 让并发的调用用不同的 ID（如带上循环下标），或等前一个 Activity 结束后再用同一 ID；重新生成并发布，Run 按退避自动继续 |
| Activity 长时间 Scheduled | Worker 没有消费该 Task Queue，或心跳登记已过 TTL（声明了队列的调度不看在线 Worker，Server 日志 `activity task queue has no known active worker` 提示队列上没有 Worker） | 核对 Worker 拉取的队列（registry 登记的队列或 `ActivityTaskQueues`）与注解 `queue` / `ActivityOptions.TaskQueue`，查看 Worker 列表 |
| Run 显示 Blocked，`activity_route_unavailable`，`declares no task queue and workers serve it on unrelated task queues` | 定义与调用处都没有声明队列，几个服务在互不相关的队列上提供同一类型 | 在 `@Activity(queue)` 或 `ActivityOptions.TaskQueue` 声明队列后重新生成发布，或停掉其中一个服务；Run 按退避自动继续，见 `references/activities.md` 的「队列如何决定」 |
| `worker.New` 返回 `activity "..." is registered on task queue "...", which ActivityTaskQueues [...] does not include` | 显式设置的 `ActivityTaskQueues` 漏了 registry 登记的队列 | 删掉 `ActivityTaskQueues` 让 Worker 用登记的队列，或把缺的队列加进去 |
| `CreateSchedule`/`UpdateSchedule` 返回 `invalid overlap_policy`；`TriggerSchedule` 返回 409 `previous scheduled run is still running` | Overlap Policy 只接受 `SKIP`、`ALLOW_ALL`；`SKIP` 下上一次触发的执行链仍在运行 | 改用大写取值；等执行链结束，或改用 `ALLOW_ALL`，见 `references/schedules.md` |
| `StartWorkflow` 返回 `current run conflict` | 该 Workflow ID 已有活跃 Run | 换一个 Workflow ID 启动并行实例；或等它结束、取消或终止 |
| Query 被拒：`workflow has not completed its first workflow task yet; query handlers are not registered` | Run 刚启动 | 稍后重试 |
| Activity 一直重试、Run 不结束 | 缺省重试策略不限次数，没有设置 `ScheduleToCloseTimeout` 或 `MaxAttempts` | 给调用设置 `ScheduleToCloseTimeout` 或 `RetryPolicy{MaxAttempts: n}`，永久失败返回 `apperror.NewNonRetryable` |
| 生成器：`<pkg>.<Func> is not supported: WLang binds no such standard-library function` / `call of <pkg>.<Func> is not supported: the package is not bound by the host` | 调用了清单外的标准库函数或其他包的函数 | 换用清单内的函数，或放进 Activity |
| Run 失败，错误 `[E_BUDGET] ... MaxStringBytes exceeded` / `MaxArrayLength exceeded` / `MaxObjectKeys exceeded` / `MaxSteps exceeded` | Workflow 代码超出执行预算：单个字符串 16 MiB、数组 10 万元素、map 10 万键、100 万步 | 把构造大数据的计算放进 Activity，长循环用 Continue-As-New；预算表见 `references/go-subset.md` |
| 生成器：`fmt.Sprintf verb %d does not accept string operand` / `format must be a constant string` / `%q is not supported` | `fmt` 格式串与操作数不符合规则 | 按 `references/go-subset.md` 的 fmt 规则改写 |
| 生成器：`workflow 里不能用 ...；用 ...` | 用了被拒绝的原生语法或包 | 按报错给出的替代写法改 |
| 生成器：`本包函数 X 不能作为值使用` / `generic function X is not supported` / `methods with pointer receivers are not supported` / `package-level variable X is not supported` | 同包函数的写法超出支持范围 | 写闭包、去掉泛型与可变参数、改成值接收者并返回新值、改成常量或参数 |
| 生成器：`unsupported ...: conversion from float64 to int is not supported` / `field access on a computed value is not supported` / `fallthrough is not supported` / `range over string is not supported` 等 | 翻译器无法保持与 Go 相同行为的写法 | 按 `references/go-subset.md` 的「生成期拒绝的语法」改写，诊断带 `file:line:col` |
| 生成器：`Y.Z (Z): embedded field Z is outside json/plain-v1` / `... (chan int): channels cannot cross workflow boundaries` | Workflow 接触的类型里有嵌入字段、channel、函数等无法编码的类型 | 改成具名字段 `Z Z`，其余见 `references/go-subset.md` 的「数据类型」 |
| 生成器：`X 直接调用了 @Activity 实现 ...；activity 只能通过生成的桥 CallX 调度` / `... 只能用作 out, err := ...` / `... 的返回值被丢弃` | 桥调用形态不对 | 改成四种支持的语句形态之一 |
| 生成器：`CallX(ctx, in) 没有设置 StartToCloseTimeout 或 ScheduleToCloseTimeout` | Activity 调用没有关闭超时 | 传 `workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}` |
| 生成器：`@Activity type="..." 的输入类型 ... 不是 JSON object` | Activity I/O 不是 struct 或 `map[string]any` | 包进 struct 字段 |
| 生成器：`Workflow W 的输入类型 ... 不是 JSON object` | Workflow 入参是标量、切片或 `any` | 包进 struct 字段，或用 `map[string]any` |
| 生成器：`@Workflow policy="auto" 未知，支持 auto_upgrade、pinned`、`未知 key`、`缺少 type`、`注解必须写成 // 行注释`、`@Workflow 注解只能有一个` | 注解写法错误 | 按 `references/codegen.md` 的 key 表修正 |
| 生成器：`activity ... 的 marker ... 是变量引用 ... 而不是函数符号` | Activity 函数名在 Workflow 函数体内被同名局部变量遮蔽，或包没通过类型检查 | 改局部变量名；先让包编译通过 |
| Reset 返回 400：`reset requires reset_type (first_workflow_task or last_workflow_task) or reset_event_id` | 没有给重置点 | 二选一提供，见 `references/operations.md` |
| Run 显示 Blocked，Workflow Task 失败原因 `bad_start_timer_attributes` / `bad_start_child_workflow_attributes` / `bad_schedule_activity_attributes` / `bad_record_marker_attributes`，`... has N characters, the limit is M` | Workflow 代码发出的 Timer ID、子 Workflow ID / Type、Activity ID / Type、Task Queue、marker 名与 ID 超过长度上限 | 缩短这个标识，重新生成并发布；Run 按退避自动继续。上限见 `references/operations.md` |
| Runtime API 返回 503 `service temporarily unavailable`（SDK 重试用尽后是 `client.ErrUnavailable`），Server 日志 WARN `runtime http request failed: dependency temporarily unavailable`，`err` 是 `Error 1040: Too many connections` 等 | MySQL 连接数打满、锁等待超时、死锁、连接中断或 matching 分区换主；连接数打满通常是 `副本数 × 2 × WORKFLOW_DB_MAX_OPEN_CONNS` 超过了 MySQL `max_connections` | 调低 `WORKFLOW_DB_MAX_OPEN_CONNS` 或调高 `max_connections`；Reset、Schedule Create / Trigger、批量操作启动得到的是 502，先读状态确认再重发，见 `references/operations.md` |
| Server 日志 WARN `task queue ack failed`，`err` 是 `max retries exceeded` 或 `stale ack token` | TaskQ 分区换主期间 Worker 的 `ack` 没送到；结局已落库，`ack` 应答 200 | 无需处理：任务 60s 后重投，`begin` 判定 `attempt_already_started` 后丢弃；持续出现时查 matching 的分区租约 |
| 开启 `WORKFLOW_RETENTION` 后 `tenuto_runs` 持续增长，`tenuto.retention.gc.backlog.oldest_age` 上升，GC leader 记 WARN `retention gc: deletion backlog is growing` | 删除持续失败（看 `tenuto.retention.gc.failures` 与 `retention gc: tree deletion failed` 的 `err`），或单棵 tree 的删除慢到一轮扫描处理不完到期任务 | 按日志里的错误处理数据库问题；一轮扫描处理所有到期任务，`WORKFLOW_GC_BATCH` 只决定每批大小 |

更多错误原文与处理见 `references/codegen.md`（生成器）与 `references/operations.md`（运行期）。

## 可运行示例

| 目录 | 演示内容 |
| --- | --- |
| `examples/shop/` | 独立 Go 模块的业务 demo：order / payment / warehouse 三个服务进程 + Server，remote 契约包、Saga 补偿、幂等重试、Signal + 持久 Timer、Query、Blocked 恢复；`run-demo.sh` 用内存后端一键运行 |
| `docs/examples/startup-workflow/` | 最短路径：一个 Workflow、一个 Activity、`worker/main.go` 双 Worker 启动与自动登记 |
| `docs/examples/saga-workflow/` | 显式逆序补偿、`ActivityOptions` 重试、`IsApplicationError` 分支、`ApplicationErrorDetail`、Workflow / Activity / DTO 分文件 |
| `docs/examples/versioned-workflow/` | `GetVersion` 分支（v1 → v2）、跨包 Activity、`@RemoteActivity` 契约、用旧 History 重放新 Program 的测试、`curl` 注册与启动流程 |
| `docs/examples/multiprocess/` | Server + 两个 Activity 进程经 HTTP 协作，remote 契约包共享，v1 → v2 → v3 发布与不兼容升级的观察（`run-demo.sh` 需要本机 MySQL） |
| `docs/examples/messaging/` | Update Handler、Query Handler 与 `sdk/client` 调用端的最小片段 |
| `docs/examples/upgrade-demo/` | 升级演练；内容以该目录自带的说明为准 |
