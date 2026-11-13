---
title: 多副本故障注入与压测
description: 用 tenuto-cluster 在一个 MySQL 上运行多个 Server 副本，注入进程、数据库与网络故障后核对结果，做长时间浸泡、滚动升级演练，并测量吞吐与延迟
status: Stable
---

# 多副本故障注入与压测

<FeatureStatus status="Stable" />

`cmd/tenuto-cluster` 在一台机器上按[生产环境拓扑](../deploy/production.md)启动 N 个 `tenuto-server` 副本（共享一个 MySQL 库）、M 个独立的 Activity Worker 进程和它们前面的负载均衡器，有四种模式：

| 模式 | 做什么 | 结果 |
| --- | --- | --- |
| `failover` | 启动一批 Workflow，运行中按计划注入进程故障（`kill -9` leader 副本、随机副本与 Worker，`SIGSTOP` 暂停副本）与经代理的数据库、网络故障；全部 Run 关闭后核对结果、History、Activity 执行记录、库内残留并重放 | `summary.json`，有违规时退出码 1 |
| `soak` | 按固定速率持续数小时启动 Workflow，每隔一段时间随机注入一个故障，后台逐个核对已结束的 Run，每分钟采样资源、表行数与 TaskQ 积压；发现违规立即结束 | `soak.jsonl`、`soak-summary.json`，有违规时退出码 1 |
| `upgrade` | 在二进制 A 上启动集群并持续启动 Workflow，用二进制 B 执行迁移后逐个副本滚动换成 B，最后按 `failover` 的核对项核对 | `summary.json`，有违规时退出码 1 |
| `bench` | 无故障，按目标速率（开环）或在途数（闭环）持续启动轻量 Workflow，测量延迟、吞吐、MySQL 与进程资源 | `bench.json` |

已在一台机器上测得的结果与复现参数见 [生产就绪与验证结果](/operations/production-readiness#验证结果)。

## 前提

- Linux 或 macOS（用到 `kill -9`、`SIGSTOP`、`ps`；`partition`、`isolate` 隔断副本出站流量时用 `lsof` 识别连接来自哪个副本，macOS 自带，Linux 上装 `lsof` 包），Go（`go.mod` 声明的版本），在仓库根目录运行。
- 一个 MySQL 8 服务端。账号需要创建、删除 `-db` 指定的库并在其中建表、读写；`bench` 与 `soak` 另读 `SHOW GLOBAL STATUS`。
- 工具只碰 `-db` 这一个库：库名必须以 `tenuto_e2e` 或 `tenuto_bench` 开头、只含字母数字下划线，事先存在时拒绝运行；运行结束删除它（`-keep` 保留）。工具从不重启、不改配置 MySQL 服务端：数据库故障全部由工具进程内的代理在客户端一侧注入。

## 运行

```bash
GOWORK=off go build -o bin/tenuto-cluster ./cmd/tenuto-cluster
export TENUTO_CLUSTER_DSN='user:password@tcp(127.0.0.1:3306)/'   # 不带库名；也可以用 -dsn

# 故障注入：3 个副本、2 个 Worker、100 个 Workflow
./bin/tenuto-cluster failover -db tenuto_e2e_1 -replicas 3 -workers 2 -workflows 100

# 数据库与网络故障
./bin/tenuto-cluster failover -db tenuto_e2e_2 -workflows 200 \
  -faults '10s:db-drop:all,20s:db-blackhole:leader:15s,40s:partition:random:20s,60s:api-blackhole:1:10s'

# 浸泡：2 小时，每秒 2 个 Workflow，每 2 分钟一个随机故障，保留期 1h 让 GC 在后一小时运行
./bin/tenuto-cluster soak -db tenuto_e2e_soak -duration 2h -keep

# 滚动升级演练（A、B 的构建见下文「upgrade」）
./bin/tenuto-cluster upgrade -db tenuto_e2e_up -server-bin-a ./bin/server-a -server-bin-b ./bin/server-b -workflows 900

# 压测：开环 20 次启动/秒，持续 60 秒
./bin/tenuto-cluster bench -db tenuto_bench_1 -replicas 3 -workers 2 -rate 20 -duration 60s
```

工具先构建 `./server/cmd/tenuto-server`（或用 `-server-bin` 指定），建一个空库，用 `tenuto-server migrate` 在库上执行 Tenuto 与 TaskQ 的迁移（两者共用这个库），再启动副本与 Worker。所有输出写到 `-out`（默认 `.run/tenuto-cluster/<db>`）：

| 文件 | 内容 |
| --- | --- |
| `logs/replica-<i>.log`、`logs/worker-<j>.log` | 各进程的日志，重启追加到同一文件 |
| `activities-worker-<j>.jsonl` | 每次 Activity 执行的开始与结束记录（Run ID、ActivityID、attempt、Worker、时间） |
| `summary.json` / `bench.json` | 结果 |
| `soak.jsonl`、`soak-summary.json` | `soak` 每分钟一行的采样与最终结果 |

Ctrl-C 停止全部子进程并删除库。工具被 `kill -9` 时子进程不会随之退出，按 `logs/` 里的进程名手动清理。

## 集群拓扑

每个副本的环境变量来自[部署 Tenuto Server](../deploy/server.md) 与[生产环境拓扑](../deploy/production.md)：

| 变量 | 取值 | 跨副本 |
| --- | --- | --- |
| `WORKFLOW_BACKEND` | `mysql-taskq` | 相同 |
| `WORKFLOW_DB_DSN`、`TASKQ_DB_DSN` | 同一个库；启用代理时经本副本的数据库代理 `127.0.0.1:<base+301+i>` 连接，其余 DSN 参数沿用 `-dsn` | 库相同 |
| `WORKFLOW_TASK_TOKEN_SECRET`、`WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64` | 每次运行随机生成 | 相同 |
| `WORKFLOW_NODE_ID` | `tenuto-cluster-replica-<i>`，重启后不变 | 唯一 |
| `WORKFLOW_HTTP_ADDR` | `127.0.0.1:<base+1+i>` | 唯一 |
| `TASKQ_MATCHING_ADDR` | `127.0.0.1:<base+101+i>`，重启后不变 | 唯一 |
| `TASKQ_MATCHING_ADVERTISE_ADDR` | 启用代理时是本副本的 matching 代理 `127.0.0.1:<base+201+i>`：成员表里是这个地址，其他副本经它连进来 | 唯一 |
| `WORKFLOW_PPROF_ADDR` | `127.0.0.1:<base+401+i>`：副本的 pprof 监听（见下文「CPU profile」） | 唯一 |

`-server-env KEY=VALUE`（可重复）给每个副本追加变量，例如 `-server-env WORKFLOW_RECONCILE_INTERVAL=10s`。

Workflow Task 由副本内嵌的 Workflow Worker 执行（缺省部署）。Worker 进程是 `tenuto-cluster worker`，`Role: worker.RoleActivity`，启动时登记负载的两个 Workflow 定义。Worker 与工具自己的 Client 都连到 `-base-port` 上的进程内 L4 负载均衡器：新连接轮询分给可用的副本。副本被 kill 或暂停前先摘除，重启后 `/health/ready` 返回 200 再加回；此外负载均衡器每秒直连各副本探测一次 `/health/ready`（1s 超时），失败的副本不再分到新连接，与常见网关的 readiness 探测相同（例如数据库卡死时副本的 readiness 失败）。已建立的连接不迁移，副本被 kill 时连接断开、客户端重连。

### 代理

`-proxies`（`failover`、`soak`、`upgrade` 缺省开启，`bench` 缺省关闭，因为多一跳会计入延迟）在工具进程内为每个副本启动两个 TCP 代理：

| 代理 | 地址 | 经过它的流量 |
| --- | --- | --- |
| 数据库代理 | `127.0.0.1:<base+301+i>` | 副本 i 到 MySQL 的全部连接（Workflow 与 TaskQ 两个连接池） |
| matching 代理 | `127.0.0.1:<base+201+i>` | 其他副本（以及副本 i 自己）到副本 i 的 matching gRPC；副本 i 把它作为 `TASKQ_MATCHING_ADVERTISE_ADDR` 写进成员表 |

代理只在本机转发字节，按故障规则关闭连接、让流量停滞或加延迟；`tenuto-server migrate` 与工具自己的核对查询直连 MySQL。matching 代理用 `lsof` 识别每条连接来自哪个副本（按连接的源端口找发起进程），没有 `lsof` 时启动时警告，`partition` 只隔断入站。

## failover

### 负载

每个 `cluster-order` Run（`-workflows` 个，按 `-start-rate` 启动；`soak`、`upgrade` 用同一个负载与同一组参数）：

1. `-activities` 个顺序 Activity（缺省 2，最多 1000），再 `-parallel` 个并行 Activity（`wg.Go`），每个执行 `-activity`；
2. `-timer` 的 Timer；
3. 等 Signal `proceed`，`-signal-timeout` 超时后走超时分支；工具在启动后 `-signal-delay` 发送 Signal；
4. `-children` 个并行的子 Workflow `cluster-steps`（缺省 1）：各 `-child-steps` 个顺序 Activity 加一个 `-timer` 的 Timer。

Activity 的结果只由输入决定，Run 的输出（顺序与并行结果之和、每个子 Workflow 的结果）因此能在调用端算出。Activity 的 `StartToCloseTimeout` 是 10s，重试间隔 1s 起、最长 5s。

规模参数：

| 参数 | 作用 |
| --- | --- |
| `-workflows` | Run 数，10000 以上可用。工具不保留 History：全部 Run 关闭后逐个读取、核对、丢弃，内存随 Run 数线性增长的只有每个 Activity 的执行记录与完成的 attempt |
| `-activities` | History 长度：每个顺序 Activity 约 7 个事件，500 个约 3500 个事件 |
| `-payload-bytes` | Workflow 输入与每个 Activity 的输入、输出都带这么多字节的填充，Activity 原样带回；核对每个 `ActivityScheduled` 输入与 `ActivityCompleted` 输出里的填充长度 |
| `-children` | 子 Workflow 扇出 |

### 故障计划

`-faults` 是逗号分隔的 `at:fault`，`at` 从开始启动 Workflow 起算。进程故障写作 `action[:down]`，`down` 缺省 10s：

| action | 故障 | 恢复 |
| --- | --- | --- |
| `kill-leader` | `kill -9` 持有 `workflow:reconciler` 锁的副本（`tenuto_leader_lock` 里未过期的 `leader_ip`，它就是该副本的 `WORKFLOW_NODE_ID`）；查不到时退回随机副本，时间线里注明 | `down` 后以同一配置重启 |
| `kill-replica` | `kill -9` 随机副本 | 同上 |
| `pause-replica` | `SIGSTOP` 随机副本，模拟卡顿：它持有的租约与锁不释放 | `down` 后 `SIGCONT` |
| `kill-worker` | `kill -9` 随机 Worker | `down` 后重启 |

数据库与网络故障经[代理](#代理)注入（`api-blackhole` 除外，它作用于负载均衡器），到期自动恢复，进程不重启：

| fault | 故障 | 模拟 |
| --- | --- | --- |
| `db-drop:<target>` | 关闭目标副本经数据库代理的全部现有连接；之后的新连接照常 | MySQL 重启、主从切换后客户端看到的连接断开 |
| `db-blackhole:<target>:<duration>` | 目标副本到 MySQL 的现有连接与新连接都停滞：代理照常接受连接，但不转发任何字节、不发 RST；到期后积压的数据照常送达 | 到数据库的网络分区、卡住的故障切换 |
| `db-latency:<target>:<ms>:<duration>` | 两个方向的每个数据块晚 `ms` 毫秒送达（延迟线，不降低吞吐） | 跨机房、拥塞的数据库链路 |
| `partition:<replica>:<duration>` | 其他副本到这个副本的 matching 流量，以及这个副本到其他副本的 matching 流量停滞；它连自己不受影响，数据库连接不受影响 | 副本之间的网络分区 |
| `isolate:<replica>:<duration>` | `partition` 加上这个副本的 `db-blackhole` | 副本所在机器断网（HTTP 入口除外） |
| `api-blackhole:<replica>:<duration>` | 负载均衡器转给这个副本的 HTTP 连接停滞；负载均衡器直连的 readiness 探测照常通过，所以它继续分到新连接 | 灰色故障：健康检查正常、业务流量卡住 |

`target` 是 `random`（随机一个不在故障中的副本）、`leader`（reconciler leader，查不到时退回随机副本）、副本序号 `1` 或 `replica-1`，`db-*` 另可写 `all`（全部副本）；`replica` 同 `target` 但不能是 `all`。数据库驱动在停滞的连接上等多久由 `-dsn` 的参数决定（`timeout`、`readTimeout`、`writeTimeout`），不设时一直等到恢复。

缺省 `10s:kill-leader:10s,20s:kill-worker:5s,30s:pause-replica:10s,45s:kill-replica:10s` 覆盖每种进程故障一次；一个进程同一时刻只处于一次故障中（`all` 不占用副本）。`-seed` 固定随机目标。时间线里每个经代理的故障在到期时记一条 `heal`。

### 核对

全部 Run 关闭（最多等 `-deadline`）后再等 `-settle`（缺省 45s，覆盖 reconciler 接管的 30s 宽限期加一个周期），然后：

| 核对 | 违规条件 |
| --- | --- |
| 结果 | Run 不是 `completed`（列出它的 Describe 结果），或输出与输入算出的期望值不同（含每个子 Workflow 的结果） |
| History 不变式（父与子 Run） | 事件 ID 不从 1 连续；第一个事件不是 `WorkflowStarted`；关闭事件不是恰好一个且在末尾；同一个 Activity 有多于一个终态事件（`ActivityCompleted` / `Failed` / `Cancelled`、`retryState` 非空的 `ActivityTimedOut`）；同一 attempt 的 `ActivityStarted` 多于一次；仍未关闭的 ActivityID 再次 `ActivityScheduled`；Timer 触发或取消多于一次；子 Workflow 关闭多于一次；同一个 Workflow Task 开始或关闭多于一次；引用了不存在的 scheduled 事件 |
| 调度个数 | 父 Run 调度的 Activity 不是 `activities + parallel` 个、子 Workflow 不是 `children` 个、子 Run 不是 `child-steps` 个，或同一 ActivityID 出现两次（同一个调用位置的命令重复） |
| 填充载荷 | `ActivityScheduled` 的输入或 `ActivityCompleted` 的输出里的填充不是 `payload-bytes` 字节 |
| 子 Workflow | 父 Run 的 `ChildWorkflowCompleted` 记下的结果与子 Run 的 `WorkflowCompleted` 不同 |
| 重放 | 用 Worker 的解释器从头重放 History（与 Workflow Task 执行同一路径）报非确定性错误，或最后一个 Workflow Task 重放出的关闭命令与输出不同 |
| Activity 执行记录 | History 里完成的 attempt 没有对应的执行记录；同一 Activity 的同一 attempt 被执行两次（Server 只让一次 Start 成功） |
| 库内状态（只读） | Run 行与 execution state 的状态不同；已关闭 Run 的 Activity state 或子 Workflow state 不是终态（负载等待了每一个） |

另外报告、不算违规的：

- 每个 Activity 的执行次数分布与重复执行。Activity 是至少一次执行，重复执行允许；工具按时间线推断原因：前一次执行没有结束记录、期间它的 Worker 被 kill，记为 `worker crashed mid-attempt`；前一次执行完了但结果没被记录，归到前一次执行开始到这次执行开始之间注入或仍在生效的最近一次故障（`result not recorded after kill-leader replica-1`；例如 api-blackhole 期间开始的 attempt，结果上报卡在被黑洞的副本上，直到 attempt 超时），没有这样的故障时记为 `result not recorded (no fault in window)`。原因是推断，不是 Server 给出的事实。
- 收到多于一次 Signal 的 Run（Signal 的响应丢失后工具重试）、走了 Signal 超时分支的 Run、启动与 Signal 的重试次数。
- 已关闭 Run 仍为 `scheduled` 的 Timer state（Signal 先到时被放弃的超时 Timer）、各状态的派发记录（派发记录保留到保留期回收）、租约已过期仍为 `leased` 的派发记录、仍记着 open Workflow Task 的已关闭 Run、TaskQ 里仍待投递的任务。
- 执行记录里不完整的行数（Worker 在写入时被 kill）。

终端打印时间线、执行次数分布、重复执行原因、库内残留和全部违规；`summary.json` 有失败 Run 的问题与 Describe 结果（最多 200 个，其余只列在违规里）。退出码：0 通过，1 有违规，2 运行出错。

## soak

```bash
./bin/tenuto-cluster soak -db tenuto_e2e_soak -duration 4h -start-rate 2 -fault-every 2m -keep
```

`soak` 用 `failover` 的负载与核对，按 `-start-rate` 持续启动 `-duration`，期间每 `-fault-every`（缺省 2m）从 `-fault-set` 里随机取一个故障注入（写法同 `-faults`，不带时间；缺省每种故障一项：`kill-leader:10s`、`kill-replica:10s`、`kill-worker:5s`、`pause-replica:10s`、`db-drop:random`、`db-drop:all`、`db-blackhole:random:20s`、`db-latency:all:50:30s`、`partition:random:20s`、`isolate:random:20s`、`api-blackhole:random:20s`），`-seed` 固定故障顺序与目标。`-duration` 结束后不再启动与注入，最多等 `-drain` 让运行中的 Run 结束，再等 `-settle` 后只读核对库内残留。

后台持续核对，发现违规立即结束（退出码 1）：

| 核对 | 违规条件 |
| --- | --- |
| 已结束的 Run | 每个 Run 结束 `-verify-delay`（缺省 10s）后按 `failover` 的核对项核对它与它的子 Run（结果、History 不变式、填充、重放），并与它们的执行记录对照（完成的 attempt 有执行记录、同一 attempt 不执行两次） |
| 卡住的 Run | 启动 `-run-timeout`（缺省 15m）后仍在运行 |
| 恢复 | 故障结束 `-recover-timeout`（缺省 5m）后仍有过期的 Run |
| 进程 | 不在故障中的副本或 Worker 退出了 |
| 保留期 | 运行时长超过保留期 + 30 分钟，`tenuto_runs` 的行数仍不少于启动过的 Run（含子 Run）总数 |
| 库内状态 | 结束时同 `failover` |

过期的 Run：仍在运行、距启动超过无故障时的时长（由负载参数算出）加 `-overdue-slack`（缺省 30s）。一次故障的恢复时间是从故障结束（进程重启就绪、`SIGCONT`、代理规则到期）到过期 Run 第一次清零，按秒计；`db-drop` 的故障结束就是注入时刻。

内存有界：工具只保留运行中的 Run 与尚未核对的执行记录，History 逐个读取、核对后丢弃；执行记录按 Run 增量读取，核对时取走，30 分钟内没有被取走的（attempt 在 Run 核对之后才写完）计入 `unmatched_records`。

保留期：`-retention`（缺省 `1h`，Server 接受的最小值）给每个副本设 `WORKFLOW_RETENTION`，并设 `WORKFLOW_RETENTION_JITTER=1m`、`WORKFLOW_GC_SCAN_INTERVAL=1m`（`-server-env` 里的同名变量优先）。结束 1h 之后的 Run 开始被回收，`-duration` 至少 `1h30m` 才能看到 GC 的效果；更短时结果里注明 GC 没有运行。`-retention 0` 关闭 GC。

`soak.jsonl` 每 `-sample-every`（缺省 1m）一行：

| 字段 | 内容 |
| --- | --- |
| `started`、`closed`、`verified`、`in_flight`、`overdue` | 累计启动、结束、核对的 Run 数，此刻运行中与过期的 Run 数 |
| `processes_in_fault` | 此刻处于故障中的进程 |
| `processes` | 每个副本与 Worker 的 PID、RSS 与两次采样之间的 CPU%（`ps`）。采样不读 goroutine 数，RSS 是泄漏指标；需要时从副本的 pprof 端口读 `/debug/pprof/goroutine?debug=1` |
| `rows` | 库里每张表的 `COUNT(*)` |
| `taskq_backlog` | `taskq_tasks` 里尚未投递的任务数，按队列 |
| `mysql_threads_connected` | `SHOW GLOBAL STATUS` 的 `Threads_connected`（全局） |
| `redeliveries` | 副本日志里累计的 `task dispatched but not claimed; redelivering` 行数 |
| `re_executions`、`activity_retries` | 已核对 Run 里多出的 Activity 执行次数、Server 记录的 Activity 重试次数 |
| `tool` | 工具自身的堆内存与 goroutine 数 |

`soak-summary.json` 与终端摘要：每种故障的次数、每次恢复与最长恢复时间（总的与按故障种类）、补投次数、重复执行与原因、泄漏指标（每个进程 RSS 的最小二乘斜率 MiB/小时，取同一 PID 最长的一段，去掉 soak 开始与该进程启动后的前 10 分钟预热（副本启动后 RSS 约 10 分钟从 ~65 MiB 涨到 ~140 MiB 后持平，重启后的进程同样如此）；工具堆内存的斜率；每张表的行数斜率）、库内残留、违规与警告。RSS 按斜率外推的增长超过窗口起点的 25% 时给出警告。用 `-keep` 保留库，违规后可以直接查询。

## upgrade

```bash
# 两个真实提交：各自构建一个 tenuto-server
git worktree add ../tenuto-a <commit-A>
git worktree add ../tenuto-b <commit-B>
(cd ../tenuto-a && GOWORK=off go build -o "$PWD/../server-a" ./server/cmd/tenuto-server)
(cd ../tenuto-b && GOWORK=off go build -o "$PWD/../server-b" ./server/cmd/tenuto-server)
./bin/tenuto-cluster upgrade -db tenuto_e2e_up -server-bin-a ../server-a -server-bin-b ../server-b -workflows 900
git worktree remove ../tenuto-a && git worktree remove ../tenuto-b
```

`upgrade` 用 `-server-bin-a`（缺省 `-server-bin` 或当前源码构建）执行迁移并启动集群，按 `failover` 的负载开始启动 Workflow；`-upgrade-at`（缺省 15s）后用 `-server-bin-b`（缺省与 A 相同，用来演练机制本身）执行 `tenuto-server migrate`，再逐个副本：从负载均衡器摘除、`SIGTERM`（`-stop-timeout` 缺省 30s 后 `SIGKILL`）、换成 B 以同一配置启动、`/health/ready` 返回 200 后加回，下一个副本前等 `-upgrade-gap`（缺省 10s）。Workflow 在整个过程中持续启动（启动窗口 `-workflows / -start-rate` 短于升级所需时间时工具给出警告）；全部 Run 关闭后按 `failover` 的核对项核对。B 的迁移失败、副本没有在 `-stop-timeout` 内退出、副本在 B 上没有就绪都是违规。Worker 进程与负载代码不升级（演练的是 Server）。`summary.json` 的 `notes` 记下 A、B 的路径与 SHA-256 前 12 位与升级耗时，时间线里是 `migrate`、`upgrade-stop`、`upgrade-start`。

A 与 B 跨越表结构变更时，演练验证的是[数据库 Schema](/deploy/server#数据库-schema) 的约定：B 的迁移保持 A 可用，B 迁移之后、换成 B 之前，A 的副本照常运行。

## bench

| 参数 | 含义 |
| --- | --- |
| `-rate` | 开环：每秒启动数，与 `-concurrency` 二选一 |
| `-concurrency` | 闭环：在途 Workflow 数（都不设时取 20），每个 Workflow 结束后启动下一个 |
| `-duration` | 启动 Workflow 的时长；之后最多再等 `-deadline` 让在途的结束 |
| `-steps`、`-timer`、`-activity` | 负载 `cluster-steps`：`steps` 个顺序 Activity（各执行 `activity`）后一个 `timer`（0 表示没有 Timer） |
| `-history-sample` | 读取多少个已完成 Run 的 History 计算服务端延迟，0 表示全部 |
| `-workflows` | 最多启动这么多个 Workflow 后停止启动（0 表示只受 `-duration` 限制） |
| `-payload-bytes` | Workflow 输入与每个 Activity 的输入、输出都带这么多字节的填充 |

`-steps` 即 History 长度（最多 1000）。`bench` 缺省不经代理（`-proxies=false`）。

测量：

| 指标 | 来源 |
| --- | --- |
| 启动调用延迟 | `ExecuteWorkflow` 的耗时 |
| 完成延迟（调用端） | 启动到 `WorkflowRun.Get` 返回。`Get` 经 `wait-closed` 长轮询等待：Run 由受理这次等待的副本结束时立即返回，由其他副本结束时最多晚 100ms（[跨副本唤醒](/develop/updates#跨副本唤醒)的读取间隔），因此应与 History 完成延迟相差约一个 HTTP 往返加至多 100ms |
| 完成延迟（History） | `WorkflowStarted` 到关闭事件的时间戳差 |
| Workflow Task 调度延迟 | `WorkflowTaskScheduled` 到 `WorkflowTaskStarted` |
| Activity 调度延迟 | `ActivityScheduled` 到第一次 attempt 的 `ActivityStarted` |
| 吞吐 | `-duration` 窗口内完成数 / 窗口秒数；全部完成数 / 首次启动到最后完成的秒数 |
| 错误 | 启动失败、`Get` 失败、结果与期望不同（有结果错误时退出码 1） |
| MySQL | `SHOW GLOBAL STATUS` 的 `Questions`、`Com_select`、`Com_insert`、`Com_update`、`Com_delete`、`Innodb_row_lock_waits`、`Innodb_row_lock_time` 前后差值（总量、每秒、每个完成的 Workflow）与采样到的 `Threads_connected` 最大值。只读，不改服务端变量；计数器是全局的，同一服务端上其他库的负载也计入 |
| 进程资源 | 每 `-sample-interval` 用 `ps` 采样各副本与 Worker 的累计 CPU 时间与 RSS，报告平均、最大 CPU%（按单核计）与 RSS |

分位数用 nearest-rank 法（p95 是排序后第 ⌈0.95·n⌉ 个值），单位毫秒。

无故障的 `bench` 里不应出现 `task dispatched but not claimed; redelivering`：它是投递丢失的兜底，补投在 30s 宽限期之后，出现即构成 p99 与 max 尾延迟，记下 `run_id` 与 `scheduled_event_id` 查原因。

`bench` 的 MySQL 计数器只给总量。要看语句来自哪里，在同一台 MySQL 上运行单进程的语句计数测试：负载相同（3 个零耗时 Activity 加 100ms Timer，闭环并发 5，20 个 Workflow），按操作、表与发起语句的入口（`engine.PollWorkflowTask`、`engine.HandleWorkflowTaskCompleted`、`taskq.queue.(*taskRuntime).tryGetTask` 等）列出每个 Workflow 的语句数，再列出空闲 3 秒里每秒的语句数。不含 `BEGIN` / `COMMIT`；超出预算（每个 Workflow 300 条，即按代码推算的约 259 条加约 15%；空闲每秒约 26 条，按载入的 12 个分区的锁续约、锁成员行与 GC 节拍计算）时测试失败。连接带 `interpolateParams=true`（与 Server 相同），每条语句一次往返：

```bash
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='user:pw@tcp(127.0.0.1:3306)/' \
  GOWORK=off go test ./server/integration -run TestStatementBudgetPerWorkflow -count=1 -v
```

结果取决于硬件、MySQL 配置，以及所有进程与 MySQL 是否在同一台机器上：比较两次结果时保持这些条件相同，在报告里写明机器与 MySQL 的配置。

保留期回收的吞吐用单进程测试测量：在 MySQL 上造 1000 棵已到期的单 Run tree（每棵 8 个事件），跑一轮 GC 扫描，断言一轮全部删完、每分钟至少 1000 棵，并按表列出每棵 tree 的语句数。作为对照，`soak` 缺省每秒启动 2 个 Workflow、各带 1 个子 Workflow，每分钟最多 240 个 Run（每个 Run 一棵 tree）：

```bash
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='user:pw@tcp(127.0.0.1:3306)/' \
  GOWORK=off go test ./server/integration -run TestRetentionGCThroughput -count=1 -v
```

不需要 MySQL 的对应测试 `TestRepositoryReadsPerWorkflow`（内存后端，同一负载逐个执行）统计每个 Workflow 在事务之外读 Run 与 execution state 的次数，按入口分组写进日志，超出上界时失败：

```bash
GOWORK=off go test ./server/integration -run TestRepositoryReadsPerWorkflow -count=1 -v
```

### CPU profile

每个副本以 `WORKFLOW_PPROF_ADDR=127.0.0.1:<base+401+i>` 启动。`bench` 运行期间采副本 0（`-base-port` 17400 时是 17801）的 30 秒 CPU profile 与锁争用：

```bash
go tool pprof -top -cum 'http://127.0.0.1:17801/debug/pprof/profile?seconds=30'
go tool pprof -top http://127.0.0.1:17801/debug/pprof/mutex
go tool pprof -top http://127.0.0.1:17801/debug/pprof/block
```

### 进程内负载

`BenchmarkHarness`（`cmd/tenuto-cluster/harness_bench_test.go`）在一个进程里跑内存后端的完整 `tenuto-server`（HTTP、引擎、内嵌 Workflow Worker）与经 HTTP 连接的 Activity Worker，闭环并发跑 `b.N` 个负载 Workflow，不需要 MySQL：

| 子基准 | 负载 | 并发 |
| --- | --- | --- |
| `seq50` | `cluster-order`：50 个顺序 Activity，无并行、无子 Workflow，1ms Timer 与 1ms Signal 等待 | 16 |
| `small` | `cluster-steps`：3 个顺序 Activity 加 10ms Timer（`bench` 的负载） | 64 |

报告 `tasks/s`（Workflow Task 与 Activity Task 合计，按 History 里的 `WorkflowTaskStarted` 与 `ActivityStarted` 计数）、`cpu-µs/task`（进程的用户态加内核态 CPU，含 Activity Worker 与 Client）、`allocs/task` 与 `B/task`。`TENUTO_HARNESS_TX=1` 时注入记录指标的 MeterProvider，另报 `tx/task` 并按操作列出每个任务的持久化事务数（指标记录计入 CPU，测 CPU 时不设）。

```bash
GOWORK=off go test ./cmd/tenuto-cluster -run '^$' -bench Harness -benchtime 200x -cpuprofile cpu.out -memprofile mem.out
TENUTO_HARNESS_TX=1 GOWORK=off go test ./cmd/tenuto-cluster -run '^$' -bench Harness -benchtime 50x -v
```

内存后端的事务由一把全局锁串行，Run 与事件的读写是进程内的复制；这个基准量的是引擎、内嵌 Workflow Worker、WLang 解释器、HTTP 与编解码，不含 GORM、MySQL 驱动与 TaskQ 的开销，后者用上面的副本 CPU profile 与语句计数测试量。

## 通用参数

| 参数 | 缺省 | 含义 |
| --- | --- | --- |
| `-dsn` | `TENUTO_CLUSTER_DSN` | 不带库名的 MySQL DSN |
| `-db` | 必填 | 要创建的库 |
| `-keep` | `false` | 结束后保留库 |
| `-replicas`、`-workers` | 3、2 | 副本数、Worker 进程数 |
| `-worker-concurrency` | 16 | 每个 Worker 的 Activity 槽位 |
| `-base-port` | 17400 | 负载均衡器端口；副本 i 的 HTTP 在 base+1+i，matching 在 base+101+i，matching 代理在 base+201+i，数据库代理在 base+301+i，pprof 在 base+401+i |
| `-server-bin` | 构建 | `tenuto-server` 二进制 |
| `-proxies` | 故障模式 `true`，`bench` `false` | 经进程内代理连接 MySQL 与其他副本的 matching（`db-*`、`partition`、`isolate` 需要） |
| `-server-env` | — | 追加给每个副本的 `KEY=VALUE` |
| `-db-max-open-conns` | 20 | 每个副本的 `WORKFLOW_DB_MAX_OPEN_CONNS`（Workflow 与 TaskQ 两个连接池各自的上限，`WORKFLOW_DB_MAX_IDLE_CONNS` 取它与 10 的较小值）。三副本合计最多 120 条，低于 MySQL 缺省的 `max_connections`（151）；`0` 用 Server 的缺省值 50，三副本最多 300 条，共用缺省配置的 MySQL 时会遇到 `Error 1040`。`-server-env` 里的同名变量优先 |
| `-auth` | `false` | 开启 Bearer 认证：每次运行随机生成一个 token 写进副本的 `WORKFLOW_HTTP_AUTH_TOKEN`，Worker 子进程（经环境变量 `TENUTO_AUTH_TOKEN`）与工具自己的 Client 带上它；负载均衡器的 `/health/ready` 探测不需要 token。`upgrade` 的两个二进制都要支持认证 |
| `-out` | `.run/tenuto-cluster/<db>` | 输出目录 |
| `-namespace` | `cluster` | 负载的 Namespace |
| `-ready-timeout` | 90s | 副本或 Worker 就绪的上限 |

负载代码在 `cmd/tenuto-cluster/workload`（`order`、`steps` 两个 Workflow 包由统一的 `go generate ./...` 生成），在内存 Server 上的端到端测试是 `cmd/tenuto-cluster/workload_e2e_test.go`（含较长的顺序链、子 Workflow 扇出与填充载荷）。代理、故障解析与注入、soak 的记账有不需要数据库的单元测试：`proxy_test.go`、`faults_test.go`、`soak_test.go`。
