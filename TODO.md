# TODO

## MySQL 下的验证（测试库 10.1.1.129:3306 不可用期间没有跑）

从 5e61822 起，合入 main 的改动只在 memory 后端验证过。MySQL 恢复后补跑：

- [ ] `TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='tenuto_test:root@tcp(10.1.1.129:3306)/' go test -p 1 ./server/... -timeout 40m`
  - 重点：执行层契约测试的 MySQL 部分，包括新增的 `ExecutionTx.GetEventIDByDedupeKey`、History 硬上限关闭、Signal 先去重后做关闭检查。
- [ ] `UD_BACKEND=mysql ./docs/examples/upgrade-demo/run-demo.sh`，期望 RESULT PASS。
- [ ] `./docs/examples/multiprocess/run-demo.sh`（MySQL），期望 RESULT PASS。
- [ ] 端到端故障注入（场景代码在 review5-e2e worktree 的 `cmd/tenuto-cluster` r4/r5 子命令里，未提交）：
  - Server 副本与 leader 在故障场景下的表现。
  - 2 副本 MySQL soak，至少 30 分钟。观测 retention GC、数据库连接数、TaskQ 积压，以及内存与 goroutine 是否平稳。
- [ ] 删除残留的测试库：`tenuto_e2e_r5_worker2`、`tenuto_e2e_r5_can`，以及其他以 `tenuto_e2e_` 开头的库。

## 已知遗留

### 依赖模块

- [ ] TaskQ（github.com/donutnomad/taskq）：MySQL 连接断开时，`dispatchTask` 的 UPDATE 结果不确定，正在交付的任务会丢失。Run 要等派发租约（30s）加 reconciler 周期才会补投，实测停顿约 54s。建议在 TaskQ 里把这一行记为 uncertain，然后退避重试（约 30–50 行）。

### wlang 上游

以下各项都已写进 `docs/develop/go-subset.md` 的差异表。

- [ ] 类型化的值经 `[]any`、`map[string]any` 或 struct 的 any 字段传递后，类型名会丢失。上游修法是给 any 载体做接口装箱，涉及数百行，风险高。
- [ ] any 中的具名基础类型按底层值比较。依赖上一项；修了之后还要撤回 sdkgen 对具名类型的统一。
- [ ] any 中 `int` 与 `int64` 不区分。需要新增 int 语言类型，300 行以上。
- [ ] 非 struct 指针按内容比较，而不是按地址。需要引入指针单元，200 行以上。

### 与 Temporal 的有意差异

以下各项都已写进文档。

- 单条命令超限或 workflow panic 时 Run 进入 Blocked，而不是 terminate；每次退避重试多写 3 条 History，Temporal 用 transient WFT，不写入 History。
- History 超过硬上限时，写入 WorkflowFailed(HistoryLimitExceeded)，Temporal 写的是 Terminated。
- DST 回拨时重复的那个小时只触发一次。
- 迟到的 Activity 完成落在已关闭的 Run 上时，Worker 收到的是成功，Temporal 返回 NotFound。

### 其他

- [ ] Worker 停止时的 `shutdown-worker` 只结束接到请求的副本上的长轮询。多副本部署时，落在其他副本上的长轮询仍由 Worker 直接断开，那一刻出队的任务要等可见性超时（Activity）或 Workflow Task 超时才重投。彻底解决需要跨副本广播 session 关闭（例如经 DB 或 TaskQ 通知）。
- [ ] Reset 在 CAS 冲突时会留下孤儿 History 分支。
- [ ] update handler 优先运行（SDKPriorityUpdateHandling）没有按 History 设开关。等到需要兼容已部署的 Run 时再补。
- [ ] Schedule 启动失败的重试状态只保存在内存里，leader 切换后会重新计时。
- [ ] 批量 Reset 中途崩溃后重做时，已经 Reset 过的 Run 被计为 skipped。
- [ ] 启动恢复失败时，Timer 会晚约 45s 触发。
- [ ] 多属性过滤时，驱动属性按键名选择，而不是按选择性选择。
- [ ] 待观察：`compiler/sdkgen` 的 `TestGenerateDoesNotDependOnGeneratedFiles` 在高负载下失败过一次（与 MySQL 全量测试并发，耗时 95s，`open testdata/tmp-orders-*/zz_orders_contract_gen.go: no such file or directory`），单独与整包重跑都通过。
- [ ] 待观察：upgrade-demo（memory）在 21fc843e 上失败过一次，之后连续 12 次都通过，失败时的服务端日志没有保留。
  - 失败位置：场景 4，app-a v1 下线后，pinned Run 在 Signal 之后调度 fulfillment.restock，ActivityScheduled 写入后 30s 内没有被领取，driver 等待超时。
  - 疑似原因：投递丢失，要等 reconciler 30s 宽限期后补投；也可能只是负载下的计时问题。
  - 再次出现时，先保存 `.run/upgrade-demo/logs/` 再排查。
