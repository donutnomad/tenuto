# TODO

## MySQL 下的验证

2026-11-27 在测试库 172.16.0.6:3306 上补跑完成（这个 MySQL 实例上还有其他项目的库，清理时只动 `tenuto_` 开头的库）：

- [x] `go test -p 1 ./server/...`（`TENUTO_TEST_MYSQL=1`）：47e4a9ee 与 67030ac7 均通过。
- [x] upgrade-demo（MySQL）RESULT PASS；multiprocess（MySQL）RESULT PASS。
- [x] `tenuto-cluster r5 -scenario server|worker|upgrade`（2 副本 + 2 Worker）全部 PASS。server 场景首次运行发现 Update 跨 Continue-As-New 重复执行，结论是与 Temporal 相同的 Run 内去重语义：文档已补充，r5acct 负载改为在 Workflow 里去重。
- [x] 2 副本 MySQL soak 31 分钟（kill-worker 8 次、kill-replica 1 次、pause-replica 5 次）：1800 个 Run 全部结束并核对通过，每次故障 1s 内恢复，26 次 Activity 重复执行全部是 Worker 被 kill 时正在执行的 attempt。数据库连接数 38–45，TaskQ 积压接近 0，overdue 始终为 0。副本 RSS 斜率 9–17 MiB/h。
- [ ] retention GC 没有覆盖到：`-retention` 最小 1h，要跑 90 分钟以上。`tenuto_task_dispatches` 每小时增长约 8.4 万行（约 10 行/Run），需要确认 GC 会回收。
- [ ] soak 报告 `unmatched records: 100`（核对之后才写完、没被取走的执行记录），没有逐条核对。
- [ ] 删除残留的测试库：`tenuto_e2e_r5_worker2`，以及 `tenuto_test_*`（中断的测试留下的）。

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
