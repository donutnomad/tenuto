---
title: 生产就绪与验证结果
description: Tenuto 的正确性保证、生产必需配置、多副本故障注入与压测的实测数字、已知限制，以及在自己的基础设施上复现验证的方法
---

# 生产就绪与验证结果

本页给运维方一个上线判断依据：Tenuto 保证什么、生产环境必须配置什么、哪些场景测过以及实测数字、已知限制，以及如何在自己的环境里重跑同样的验证。各项的完整定义在链接的页面里。

## 保证

| 范围 | 保证 | 定义 |
| --- | --- | --- |
| Activity | 至少执行一次（at-least-once）。超时重试、Worker 在上报结果前崩溃都会让同一个业务操作再执行，每个 Activity 都必须幂等 | [投递与执行次数](/develop/activity-execution#投递与执行次数)、[Activity 幂等](/best-practices/activity-idempotency) |
| Workflow 状态转换 | 每次转换恰好提交一次：History 追加与 mutable state 在同一个事务提交；写事务先以 `SELECT ... FOR UPDATE` 锁住 Run 的 execution state 行再判定，多个副本处理同一个 Run 时由行锁互斥；Workflow Task、Activity、Timer、Child 的关闭按 first-wins 生效，旧 Attempt 与旧任务由 generation stamp 拦下 | [原子边界](/reference/data-model#原子边界)、[幂等](/reference/data-model#幂等)、[Server 副本](/deploy/production#server-副本) |
| 确定性重放 | Workflow 代码由 History 重放得到同一命令序列；改变命令序列的修改由 `GetVersion` 或 `pinned` 保护，失配时 Run 进入 Blocked State，修复后继续 | [确定性约束](/develop/determinism)、[Event History 与重放](/concepts/event-history) |
| 升级兼容 | `pinned` 的 Run 固定启动时的 Program；`auto_upgrade` 的 Run 在 Workflow Task 边界换到 latest，换版前检查 History 里已有的载荷能否被 latest 的类型读回，读不回时以 `ConfigError` 暂停而不换版。Schema 迁移只向前，每个迁移保持上一个版本的 Server 可用 | [Run 版本策略](/develop/versioning#run-版本策略)、[换版前的载荷检查](/develop/versioning#换版前的载荷检查)、[数据库 Schema](/deploy/server#数据库-schema) |
| 保留期回收 | 开启 `WORKFLOW_RETENTION` 后，GC leader 回收终态满保留期的整棵 History tree，孤儿兜底扫描回收主路径够不到的残留 | [Retention GC](/reference/configuration#retention-gc)、[保留期回收](/operations/capacity-limits#保留期回收) |

### 故障时的行为

| 故障 | 行为 |
| --- | --- |
| 副本崩溃（`kill -9`） | 它持有的 TaskQ 分区由其他副本接管，期间投递短暂停顿（见[已知限制](#已知限制与非目标)）；成员表约 20s 后剔除它，在此之前入队换到其他 owner 的分区；它进程内的定时器、超时与在途 Workflow Task 由其他副本与 leader 的 reconciler 接管 |
| 副本卡顿（`SIGSTOP`） | 租约与锁在恢复前不释放，到期后由其他副本接管，与崩溃相同 |
| MySQL 连接中断、主从切换 | 进行中的事务失败；Runtime API 对可以重发的调用应答 503（带 `Retry-After`），对 Reset、Schedule Create / Trigger、批量操作启动应答 502；Go SDK 重试 503 与连接错误。见 [HTTP API 失败应答](/reference/http-api#失败应答) |
| MySQL 卡死（连接不断、无应答） | 副本的 readiness 失败，网关不再分给它新请求；驱动在停滞连接上等多久由 DSN 的 `timeout`、`readTimeout`、`writeTimeout` 决定，不设时等到恢复 |
| 副本之间网络分区 | Run 的正确性由数据库行锁与 fencing 保证，与副本之间的连通性无关；投递在分区恢复后继续（F2 验证过的时长见下文） |

数据库可用性本身由运维方负责，见[已知限制](#已知限制与非目标)。

## 生产配置清单

| 项目 | 要求 | 参考 |
| --- | --- | --- |
| 数据库 | MySQL 8.0 或更高（表使用 `utf8mb4_0900_bin`）。首次部署与每次升级先执行新版本的 `tenuto-server migrate`，再启动或滚动替换副本；DDL 权限只给执行 migrate 的账号 | [数据库 Schema](/deploy/server#数据库-schema) |
| DSN | Server 强制设置 `parseTime=true`、`loc=UTC`、`interpolateParams=true`；连接字符集 `utf8mb4` | [必需配置](/deploy/server#必需配置) |
| 连接池 | `副本数 × 2 × WORKFLOW_DB_MAX_OPEN_CONNS + 余量 < max_connections`（每个副本 Workflow 与 TaskQ 两个池） | [数据库连接数](/deploy/production#数据库连接数) |
| 认证 | 开启 `WORKFLOW_HTTP_AUTH_TOKEN`（或 `_FILE`）与 / 或 mTLS（`WORKFLOW_HTTP_TLS_CLIENT_CA_FILE`）；开启 mTLS 时配置明文的 `WORKFLOW_HEALTH_ADDR` 供探针使用；HTTPS 用 `WORKFLOW_HTTP_TLS_CERT_FILE` / `_KEY_FILE` | [安全部署](/deploy/security) |
| Console | 放在你自己的 SSO 或鉴权代理之后；Server 凭证只配在 Console 服务端（`TENUTO_SERVER_AUTH_TOKEN`、`TENUTO_SERVER_TLS_*`），不用 `NEXT_PUBLIC_API_URL` | [Console 权限](/deploy/security#console-权限) |
| 跨副本密钥 | `WORKFLOW_TASK_TOKEN_SECRET` 与 `WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64` 在所有副本相同；`WORKFLOW_NODE_ID` 每个副本唯一 | [Server 副本](/deploy/production#server-副本) |
| matching 地址 | 副本之间可达 matching 端口（默认 7235）；经 NAT、代理或 Kubernetes Service 才能到达时设 `TASKQ_MATCHING_ADVERTISE_ADDR` | [Server 副本](/deploy/production#server-副本) |
| 保留期 | 设置 `WORKFLOW_RETENTION`（至少 `1h`），并对 `tenuto.retention.gc.backlog.oldest_age` 告警 | [Retention GC](/reference/configuration#retention-gc) |
| 遥测 | `TENUTO_OTEL_ENABLED=true` 与 OTLP endpoint；按告警建议配置连接池、503、GC backlog 告警 | [可观测性](/operations/observability) |
| 优雅退出 | 网关在 `SIGTERM` 前摘除副本；`terminationGracePeriodSeconds` 大于 `WORKFLOW_HTTP_SHUTDOWN_TIMEOUT`（缺省 20s）加 `TENUTO_OTEL_SHUTDOWN_TIMEOUT`（缺省 5s），例如 30s | [优雅退出](/deploy/server#优雅退出) |
| pprof | `WORKFLOW_PPROF_ADDR` 保持空（缺省关闭），排查时只绑定本机或运维网段 | [可选 CPU Profile](/deploy/server#可选-cpu-profile) |
| 构建 | Go ≥ 1.27.4（`go.mod` 的 `toolchain`）；`GOTOOLCHAIN=local` 的环境自备 | [构建](/deploy/server#构建) |

代码、发布与回滚侧的检查项见[生产上线检查清单](/quickstart/production-checklist)。

## 验证结果

测试环境：一台笔记本，MySQL 与其他数据库共用同一个实例，3 个 Server 副本与 2 个 Activity Worker 进程和 MySQL 在同一台机器上（`tenuto-cluster` 的缺省拓扑，见[多副本故障注入与压测](/operations/cluster-testing)）。数字是这套环境下的结果，独立的数据库与 Server 机器上应当更好，作为下限看待。

### 故障注入与升级

| ID | 场景 | 结果 |
| --- | --- | --- |
| F1 | `tenuto-cluster failover`，3 副本、2 Worker，150 个 Workflow（各含 Activity、Timer、Signal、子 Workflow）；`kill -9` leader、kill Worker、`SIGSTOP` 副本 10 s、kill 副本 | 投递修复之后连续 4 轮 150/150 通过；History 不变式、重放与库内残留核对全部干净 |
| F2 | 60 个 Workflow，数据库与网络故障：断开全部 48 条数据库连接、leader 的数据库黑洞 8 s、全部副本数据库延迟 +50 ms、副本分区 10 s、副本隔离 10 s、API 黑洞 8 s | 60/60 通过 |
| F3 | 规模：300 个 Workflow × 50 个顺序 Activity + 3 个子 Workflow + 4 KB 载荷，期间 kill leader、断开数据库连接、kill Worker | 300/300 通过，18 600 次 Activity，1 次重复执行 |
| F4 | 滚动升级（`tenuto-cluster upgrade`，600 个 Workflow）：migrate 后在流量下逐个替换 3 个副本，耗时 21.5 s | 600/600 通过 |

### 压测

| ID | 场景 | 结果 |
| --- | --- | --- |
| B1 | `tenuto-cluster bench` 闭环并发 10，Workflow = 3 个零耗时 Activity + 100 ms Timer | 集群合计约 25 Workflow/s（约 200 task/s）；完成延迟 p50 约 0.3 s，p99 约 0.9–2.6 s（尾部来自启动后紧接着的 TaskQ 分区重新分配）；每个 Workflow 约 256 条 MySQL 语句；空闲时约 24 条语句/s |
| B2 | 保留期回收吞吐（MySQL 门控的 `TestRetentionGCThroughput`） | 约每分钟 10 600 棵 tree，每棵 16 条语句 |

### 浸泡

| ID | 场景 | 结果 |
| --- | --- | --- |
| S1 | Soak #1：2 h，3 副本，每 2 分钟从 10 种故障里随机注入一个，共 56 次故障 | 13 522 个 Workflow，107 688 次 Activity 执行；除数据库延迟 +50 ms（38 s）外，每种故障的最长恢复时间 1 s。它发现的 GC 吞吐与瞬时错误问题已修复 |
| S2 | Soak #2：修复后重跑 2 h，开启 bearer-token 认证，同样的随机故障共 59 次（kill leader 3、kill 副本 7、kill Worker 9、SIGSTOP 副本 5、断开数据库连接 9、数据库黑洞 4、数据库延迟 6、分区 3、隔离 7、API 黑洞 6），保留期 1 h | 14 400/14 400 个 Workflow 完成并通过核对，0 违规，启动与 Signal 重试 0 次；115 200 个 Activity，其中 237 个因故障执行了不止一次（Worker 崩溃或结果未落库后重试），无未匹配记录。最长恢复时间：kill leader 16 s、SIGSTOP 副本 22 s（等待租约过期后重新分配），其余 8 种 1 s。第二个小时起 GC 与写入持平，`tenuto_runs` 稳定在约 14 800 行；副本 RSS 结束时约 140 MiB、斜率 ≤ 10 MiB/h；MySQL 连接数峰值 79 |


### Console

Console 在真实浏览器里完整走查两遍，一遍开启认证、一遍关闭认证。

## 已知限制与非目标

| 限制 | 说明 |
| --- | --- |
| 单个 MySQL 主库是扩展与可用性上限 | 没有分片；所有副本共享 Workflow 库与 TaskQ 库（`TASKQ_DB_DSN` 可以指向另一个实例分担负载）。数据库高可用、备份与故障切换由运维方负责，见[备份、恢复与灾难演练](/operations/backup-recovery) |
| TaskQ 分区易主时投递停顿 | 副本加入或离开时分区 ownership 变化，期间投递停顿 2–3 s；B1 的 p99 尾部即来自这里 |
| 下线副本的定时器接管有延迟 | 由 leader 的 reconciler 接管，最长约 30s 宽限期加一个 `WORKFLOW_RECONCILE_INTERVAL` |
| WLang 只限制单个值 | 单个字符串 16 MiB、数组 10 万元素、对象 10 万键，没有每次执行的总内存上限；`Budget.MaxAllocBytes` 接受但不执行。合计内存由副本的内存限制兜底，只登记受信流水线构建的 Program。见[资源上限](/deploy/security#资源上限) |
| 没有内置多租户授权 | 认证通过的调用方拥有全部 Namespace 的全部权限，Namespace 只是寻址范围；按调用方或 Namespace 分权在网关按路径分组做，见[网关授权](/deploy/security#网关授权) |
| Activity 至少一次 | 同上文「保证」，Activity 必须幂等 |
| History 与请求大小 | 单个 Run 的 History 上限 51,200 个事件或 50 MiB，请求体 4 MiB；长期运行的 Workflow 用 Continue-As-New，见 [History 容量管理](/operations/history-scaling) |
| 长时间分区没有测量数据 | 验证只覆盖 F2 里的分区、隔离与黑洞时长；数据库卡死时连接等待时长取决于 DSN 超时参数 |
| Preview 能力 | Workflow / Activity Reset、Schedule、Batch Operation、Signal-With-Start、异步 Activity 完成、Search Attributes、Trace 是 Preview，见[能力与成熟度](/reference/capabilities) |
| 单集群 | 没有多集群复制、Namespace Failover 与 History Archival |
| 独立 Workflow Worker 的升级顺序 | `WORKFLOW_WORKER_ENABLED=false` 时先升级全部 Server，再升级 Worker，见 [Server 与 Worker 升级](/operations/upgrades) |

## 在自己的环境里复现

在仓库根目录构建工具，DSN 不带库名，库名以 `tenuto_e2e` 或 `tenuto_bench` 开头且事先不存在。工具只创建、删除 `-db` 这一个库，从不重启或改配置 MySQL 服务端。参数与核对项的完整说明见[多副本故障注入与压测](/operations/cluster-testing)。

```bash
GOWORK=off go build -o bin/tenuto-cluster ./cmd/tenuto-cluster
export TENUTO_CLUSTER_DSN='user:password@tcp(db-host:3306)/'

# F1：缺省进程故障计划（kill leader、kill Worker、SIGSTOP、kill 副本）
./bin/tenuto-cluster failover -db tenuto_e2e_f1 -workflows 150

# F2：数据库与网络故障
./bin/tenuto-cluster failover -db tenuto_e2e_f2 -workflows 60 -start-rate 5 \
  -faults '5s:db-drop:all,10s:db-blackhole:leader:8s,25s:db-latency:all:50:10s,30s:partition:random:10s,45s:isolate:random:10s,60s:api-blackhole:1:8s'

# F3：长 History、子 Workflow 扇出与载荷
./bin/tenuto-cluster failover -db tenuto_e2e_f3 -workflows 300 -start-rate 20 -activities 50 -children 3 -payload-bytes 4096 \
  -activity 0s -timer 1s -signal-delay 2s -deadline 40m \
  -faults '60s:kill-leader:10s,120s:db-drop:all,180s:kill-worker:5s'

# F4：滚动升级；A、B 用两个提交各自构建的 tenuto-server，缺省 A = B 演练机制本身
./bin/tenuto-cluster upgrade -db tenuto_e2e_f4 -workflows 600 -server-bin-a ./server-a -server-bin-b ./server-b

# S1：2 小时浸泡，每秒 2 个 Workflow，每 2 分钟一个随机故障，保留期 1h 让 GC 在后一小时运行
./bin/tenuto-cluster soak -db tenuto_e2e_soak -duration 2h -keep

# B1：闭环压测
./bin/tenuto-cluster bench -db tenuto_bench_1 -concurrency 10 -steps 3 -timer 100ms -duration 60s

# B2：保留期回收吞吐与每个 Workflow 的语句数
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN="$TENUTO_CLUSTER_DSN" \
  GOWORK=off go test ./server/integration -run 'TestRetentionGCThroughput|TestStatementBudgetPerWorkflow' -count=1 -v
```

在生产形态的环境里验证时，加 `-auth` 以 Bearer 认证运行，`-server-env` 传入与生产相同的变量，`-db-max-open-conns` 按目标 MySQL 的 `max_connections` 设置。

通过标准：

| 模式 | 通过 |
| --- | --- |
| `failover`、`upgrade` | 退出码 0，`summary.json` 没有违规：全部 Run `completed` 且输出符合期望，History 不变式、重放、Activity 执行记录与库内状态核对通过。重复执行与库内残留只报告，不算违规 |
| `soak` | 退出码 0，`soak-summary.json` 没有违规与 RSS 警告；每次故障在 `-recover-timeout` 内恢复；运行超过保留期 + 30 分钟时 `tenuto_runs` 确有回收 |
| `bench` | 没有结果错误；副本日志里没有 `task dispatched but not claimed; redelivering`；吞吐与延迟和本页、你自己的上一次结果对比，比较时保持硬件与 MySQL 配置相同 |
| 两个测试 | `go test` 通过：GC 一轮删完且每分钟至少 1000 棵；每个 Workflow 的语句数在预算内 |
