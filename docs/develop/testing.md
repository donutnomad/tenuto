---
title: Workflow 测试
description: 确定性、重放、故障恢复与端到端验证方法
status: Stable
---

# Workflow 测试

<FeatureStatus status="Stable" />

Workflow 测试需要同时验证编排逻辑、事件历史、重试语义和外部副作用边界。

## 测试层次

| 层次 | 目标 | 依赖 |
| --- | --- | --- |
| 单元测试 | 验证纯业务函数和输入输出 | Go 标准测试 |
| Replay 测试 | 验证相同 History 得到相同命令 | WLang Runtime |
| Engine 集成测试 | 验证 Run、Activity、Timer、Child | memory store |
| 存储集成测试 | 验证事务、索引和恢复 | GORM/MySQL |
| 端到端测试 | 验证 Server、TaskQ、Worker、Console | 完整环境 |

## Replay 测试

Replay 测试的输入是 Workflow Program 与 Event History，输出是下一批 Commands。测试应覆盖：

- 空 History 的首次执行
- Activity Completed、Failed 和 Retry
- Timer Fired、Signal、Update
- 并发 lane 的固定顺序
- Child Workflow 完成、失败和取消
- Continue-As-New 后的新 Run
- Reset 后的 History Branch

同一份 History 重放两次时，命令类型、步骤 ID、Task Queue 和输入必须一致。时间、随机数、UUID 和外部结果通过确定性 API 固化。

## Activity 测试

Activity 测试使用真实业务函数和可控外部客户端。外部服务使用接口替身，测试重点放在：

1. 成功结果的序列化与反序列化。
2. 可重试错误与不可重试错误的分类。
3. Heartbeat 后取消的清理行为。
4. 相同幂等键重复执行的结果。
5. 超时、网络断开和空响应。

## 故障注入矩阵

```text
Workflow Task: 领取后进程崩溃 → Server 恢复 open task
Activity:      Started 后 Worker 崩溃 → Heartbeat/StartToClose 推进重试
Timer:         Server 重启 → 从 TimerState 重建计时器
Child:         父子任一侧重启 → 从 ChildWorkflowState 恢复关联
数据库:        事务回滚 → History 与 mutable state 同时回滚
TaskQ:         投递失败或投递丢失 → reconciler 重新投递
```

## 两种后端上的测试

`go test ./...` 只用内存后端（`../../server/infra/memory`），不需要数据库与 Docker。仓储的领域契约
（`executiontest`、`dispatchtest`、`definitiontest`、`scheduletest`、`batchtest`）同时在内存实现与 GORM 实现上运行。

GORM 持久化只支持 MySQL。验证 GORM 仓储、TaskQ、保留期回收与应用入口的测试由环境变量 `TENUTO_TEST_MYSQL` 门控，
不设时这些测试以 `t.Skip` 跳过并打印原因：

| 包 | 只在 MySQL 上运行的内容 |
| --- | --- |
| `server/infra/persistence/...` | 各 GORM 仓储、仓储契约的 GORM 实现、并发与行锁、默认 `sql_mode` 下的写入 |
| `server/integration` | 真实 TaskQ 的端到端场景、保留期回收端到端、按库计数的配置版本场景 |
| `server/app` | 内嵌 matching、TaskQ 分区管理接口 |
| `server/biz/service/engine` | 单连接连接池下的事务死锁回归、Worker 契约重启后重建 |
| `server/infra/sql` | 迁移建出的表与 `schema.sql` 逐字相同、并发执行 migrate、启动检查、迁移中途失败后继续 |

`server/integration` 里的重启恢复与 Schedule 驱动场景默认在内存仓储上运行，`TENUTO_TEST_MYSQL=1` 时换成 MySQL 上的 GORM Store。

### 怎么跑

```bash
# 默认：内存后端
GOWORK=off go test ./...

# 连接已有的 MySQL 8 服务端：每个测试新建一个库并执行全部迁移，结束后删除
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='root:<password>@tcp(127.0.0.1:3306)/' GOWORK=off go test ./server/... -count=1

# 没有现成的服务端：只设 TENUTO_TEST_MYSQL=1，测试经 testcontainers 启动一个共享的 mysql:8.0 容器（需要 Docker）
TENUTO_TEST_MYSQL=1 GOWORK=off go test ./server/... -count=1
```

`TENUTO_TEST_MYSQL_DSN` 只给 host、port 与凭据，库名由测试分配；设置它时测试不启动容器。

### 建表与隔离

- 每个测试一个独立的库（`tenuto_test_<n>`），建库后执行 `../../server/infra/sql/migrations` 的全部迁移；
  TaskQ 的测试库执行 `../../server/infra/sql/taskq_migrations`。测试与 `tenuto-server migrate` 走同一条迁移路径，测试里没有其他建表路径。
- 独立库不依赖执行顺序、支持 `t.Parallel()`，也不必维护"该清哪些表"的清单；代价是每库建库约 40ms。
- 连接池留 32 条连接，goroutine 物理并行，`SELECT ... FOR UPDATE` 是 InnoDB 行锁。
- 容器沿用 MySQL 8 的默认 `sql_mode`（含 `STRICT_TRANS_TABLES` 与 `NO_ZERO_DATE`），与生产一致。

### 表结构守卫

`../../server/infra/persistence/schema_guard_test.go` 不需要数据库，默认运行：

- 每个 PO 的 `MysqlCreateTable()` 与 `schema.sql` 里同名表的语句一致；
- `schema.sql` 的每张表都有 PO，或登记为由其他组件读写的表（leader 选主的两张表）；
- PO 的每个 `column` tag 都是表里的列，表里的每一列也都有 PO 字段。

`../../server/infra/sql/migrate_test.go` 不需要数据库，默认运行：迁移文件名为 `NNNN_name.sql`、编号从 0001 起连续，
每个迁移文件的 SHA-256 与登记的值相同（已发布的迁移不能修改），以及库里的迁移记录与迁移文件比较的各种情况。

在 MySQL 上：

- `TestTenutoMigrationsBuildSchemaSQL` 在两个空库上分别执行全部迁移与 `schema.sql`，逐表比较 `SHOW CREATE TABLE`；
  `TestTaskQMigrationsBuildTaskQSchemaSQL` 对 TaskQ 的迁移与本次构建所用 TaskQ 模块的 `store/gormstore/schema.sql` 做同样的比较；
- `TestSchemaSQLBuildsEveryTableOnMySQL` 确认迁移建出全部 PO 的表与列。

### 修改表结构

一次表结构变更在同一个提交里改四处，守卫测试核对它们一致：

1. 在 `server/infra/sql/migrations/` 新增下一个编号的迁移文件（如 `0002_add_run_owner.sql`），写 `ALTER TABLE`、`CREATE TABLE` 等语句，每条语句以行尾分号结束，整行 `--` 注释可以出现在语句之间；
2. 更新 `server/infra/sql/schema.sql`，使它等于执行完全部迁移后的表结构；
3. 更新对应 PO 的字段与 `MysqlCreateTable()`（与 `schema.sql` 里同名表的语句相同），再运行 `go generate ./...`；
4. 在 `server/infra/sql/migrate_test.go` 的 `appliedChecksums` 里登记新迁移的 checksum（测试失败信息给出该值）。

已发布的迁移不再修改：`appliedChecksums` 的守卫在测试里拦住改动，已执行过的库在 `migrate` 与启动检查时报 checksum 不一致。修正已发布迁移的错误时新增一个迁移。
迁移保持上一个版本的 Server 可用：删除列、改名、收紧约束分两个版本进行，先让新代码不再依赖旧结构，下一个版本再删除。

升级 TaskQ 带来表结构变化时，`TestTaskQMigrationsBuildTaskQSchemaSQL` 失败：在 `server/infra/sql/taskq_migrations/` 新增迁移，使执行完全部迁移的结果等于新版 TaskQ 的 `store/gormstore/schema.sql`，并登记 checksum。

### 并发与行锁

`mysql_concurrency_test.go` 在物理并行与真实行锁下验证 TOCTOU 防护：

| 用例 | 验证什么 |
| --- | --- |
| `TestMySQLConcurrentAppend_NodeIDConvergence` | 并发 Append 后 node_id 唯一且连续 |
| `TestMySQLConcurrentSetCurrentRunCAS_FirstSet` | 首设 current 的 TOCTOU 由 `current_marker` 唯一索引兜底，最终恰好一个 current |
| `TestMySQLConcurrentAppendWorkflowTaskScheduled_SingleOpenWFT` | 一个 run 同时只有一个 open WFT（dedupe_key 唯一索引） |
| `TestMySQLConcurrentActivityStateForUpdate` | `GetActivityStateForUpdate` 的 `FOR UPDATE` 真行锁，无丢失更新 |
| `TestMySQLConcurrentTimerStateForUpdate` | `GetTimerStateForUpdate` 的 `FOR UPDATE` 真行锁，无重复 fire |

各用例统一用 `isDeadlockErr` 甄别 InnoDB 死锁 / 锁等待超时（`Error 1213` / `1205`）并显式报错。

### CI

`.github/workflows/ci.yml` 的 `go` job 跑 `go test ./...`（内存后端）；`mysql` job 用 GitHub Actions 的
`mysql:8.0` service container，设置 `TENUTO_TEST_MYSQL=1` 与 `TENUTO_TEST_MYSQL_DSN` 后跑 `go test ./server/... -count=1`，
建表由测试自己完成：

```yaml
mysql:
  runs-on: ubuntu-latest
  services:
    mysql:
      image: mysql:8.0
      env:
        MYSQL_ALLOW_EMPTY_PASSWORD: "yes"
      ports: ["3306:3306"]
      options: >-
        --health-cmd="mysqladmin ping -h 127.0.0.1"
        --health-interval=5s --health-timeout=5s --health-retries=30
  env:
    TENUTO_TEST_MYSQL: "1"
    TENUTO_TEST_MYSQL_DSN: root@tcp(127.0.0.1:3306)/
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version-file: go.mod }
    - run: go test ./server/... -count=1
```

## 测试命令

```bash
GOWORK=off go test ./...
GOWORK=off go test -race ./server/...
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='root:<password>@tcp(127.0.0.1:3306)/' GOWORK=off go test ./server/... -count=1
make build-docs
```

生产发布前在真实 MySQL 上运行多副本故障注入与压测（`cmd/tenuto-cluster`，见[多副本故障注入与压测](../operations/cluster-testing.md)），并保存失败 Run 的 History 作为回归样本。
