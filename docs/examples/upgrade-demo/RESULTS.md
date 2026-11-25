# upgrade-demo 运行结果

2026-11-25 在项目根目录执行 `./docs/examples/upgrade-demo/run-demo.sh` 的真实输出。默认模式：`tenuto-server`（内存后端，`WORKFLOW_WORKER_ENABLED=false`）、`app-a`、`app-b` 三个独立进程，由 `driver` 驱动。

MySQL 模式（`UD_BACKEND=mysql`）与带 Console 的模式（`UD_CONSOLE=1`）没有运行过。

Activity 任务按队列投递、不带 build。新旧 build 同时在线且契约不同时，任务可能先被契约不一致的 build 领到；这次领取被拒绝，任务约 1s 后重新投递。这次运行的 Server 日志里有 9 次这样的拒绝，是总耗时约 14s 的主要来源。

## 默认模式的完整输出

```text
upgrade-demo → http://localhost:7400 namespace=upgrade-demo

━━ 准备：app-a v1 与 app-b v1 上线 ━━
→ 启动 app-a v1
  ok   app-a-v1 在线 = true
→ 启动 app-b v1
  ok   app-b-v1 在线 = true
  在线 build app-a-v1  fulfillment.inspect fulfillment.quote fulfillment.reserve fulfillment.restock fulfillment.ship
  在线 build app-b-v1  orders.price
  ok   order-flow 的 latest = v1
  ok   return-flow 的 latest = v1

━━ 写法基础：一张订单走完全程（app-b 的 Workflow 跨进程调用 app-a 的 Activity） ━━
→ Start：按 Workflow Type order-flow 启动 Run，Workflow ID 是订单号 order-basic
  ok   Workflow ID = order-basic
  ok   Workflow Type = order-flow
→ Update "place"：按 Workflow ID 发送，同步拿到 Handler 的返回值
  order_ref=0a24ae05-d7a1-4c4c-ac35-a2a9ab13c0de placed_at=2026-11-25T06:44:54Z run=01a21173-3d93-7eba-ad1b-30322c759d9f
→ Query "status"：读 Workflow 的内存状态
  ok   stage = awaiting-confirm
  ok   amount（本地 Activity orders.price 的结果） = 2400
  ok   order_ref 与 Update 返回的一致 = 0a24ae05-d7a1-4c4c-ac35-a2a9ab13c0de
  ok   policy（来自 @Workflow 注解） = auto_upgrade
  ok   Program = order-flow v1 (b52f1b6b)
→ Signal "confirm"：客户确认，Run 继续
  ok   status = shipped
  ok   order_ref（RandomUUID，重放得到同一个值） = 0a24ae05-d7a1-4c4c-ac35-a2a9ab13c0de
  ok   warehouse = west
  ok   ship_fee = 1000
  ok   shipped_via = fulfillment.ship@app-a-v1
  ok   调度过的 Activity = orders.price fulfillment.reserve fulfillment.quote fulfillment.ship
  ok   reserve 与 quote 并行（两条 ActivityScheduled 之间没有 ActivityCompleted） = true
       这个 Run 的 Workflow Task 执行者：app-a-v1-1 app-b-v1-1
→ Timer：确认窗口 1 秒、不发 Signal，订单过期
  ok   status = expired
  ok   调度过的 Activity = orders.price

━━ 写法基础：一次退货（app-a 自己的 Workflow 调用自己的 Activity） ━━
  ticket=d8268fae-ead9-40c5-aa10-b657ba778d02 run=01a21173-421b-717d-8a71-ad03994fee97
  ok   policy（来自 @Workflow 注解） = pinned
  ok   ticket = d8268fae-ead9-40c5-aa10-b657ba778d02
  ok   label_fee = 500
  ok   restocked = true

━━ 准备：升级之前启动一批 Run（同一个 Workflow Type，各自的 Workflow ID），全部停在 Signal 等待点 ━━
  order-auto             policy=auto_upgrade Program=order-flow v1 (b52f1b6b)
  order-pinned           policy=pinned       Program=order-flow v1 (b52f1b6b)
  order-paused           policy=pinned       Program=order-flow v1 (b52f1b6b)
  order-to-latest        policy=pinned       Program=order-flow v1 (b52f1b6b)
  return-pinned          policy=pinned       Program=return-flow v1 (2595d328)
  return-auto            policy=auto_upgrade Program=return-flow v1 (2595d328)
  return-old-type        policy=pinned       Program=return-flow v1 (2595d328)
  return-broken-pin      policy=auto_upgrade Program=return-flow v1 (2595d328)
  return-broken-publish  policy=auto_upgrade Program=return-flow v1 (2595d328)

━━ 发布第 1 步：app-a v2 上线，与 app-a v1 并存（场景 2、5、7） ━━
→ 启动 app-a v2
  ok   app-a-v2 在线 = true
  在线 build app-a-v1  fulfillment.inspect fulfillment.quote fulfillment.reserve fulfillment.restock fulfillment.ship
  在线 build app-a-v2  fulfillment.inspect fulfillment.notify fulfillment.quote fulfillment.quote-taxed fulfillment.reserve fulfillment.restock fulfillment.ship fulfillment.ship-v2
  在线 build app-b-v1  orders.price
  ok   return-flow 的 latest（新 Program 随 app-a v2 的 Worker 登记） = v2
  ok   order-flow 的 latest（app-b 还没发布） = v1
→ 场景 7：新 Worker 已在线、新 Program 还没发布。旧 Program 上的新订单按旧契约调度，reserve 派给旧 build
  ok   Program = order-flow v1 (b52f1b6b)
  ok   fulfillment.reserve 领取它的 build = app-a-v1
  ok   fulfillment.reserve 实际执行的 build = app-a-v1
  ok   fulfillment.quote 实际执行的 build 与领取它的 build 一致 = app-a-v2
       fulfillment.quote 由 app-a-v2 执行（契约在新旧 build 上相同，两个 build 都可以承接）
→ 场景 5：新启动的 return-flow Run 用 v2 Program，走新步骤 fulfillment.notify
  ok   Program = return-flow v2 (d6c7f59d)
  ok   notified = true
  ok   调度过的 Activity = fulfillment.inspect fulfillment.notify fulfillment.quote-taxed fulfillment.restock
  ok   label_fee（含税报价） = 550
  ok   fulfillment.quote-taxed 领取它的 build = app-a-v2
  ok   fulfillment.quote-taxed 实际执行的 build = app-a-v2
→ 场景 5：pinned 的在途 Run 留在 v1 Program
  ok   Program 没有变化 = 2595d328cbd9c112943946c16256545f
  ok   Program = return-flow v1 (2595d328)
  ok   notified = false
  ok   label_fee（旧 type fulfillment.quote：不含税） = 500
  ok   fulfillment.quote 实际执行的 build 与领取它的 build 一致 = app-a-v2
       fulfillment.quote 由 app-a-v2 执行（契约在新旧 build 上相同，两个 build 都可以承接）
→ 场景 5：auto_upgrade 的在途 Run 在下一个 Workflow Task 边界切到 v2 Program
  ok   Signal 之前的 Program = return-flow v1 (2595d328)
  ok   Program = return-flow v2 (d6c7f59d)
  ok   启动时的 Program 保留在 original 字段 = 2595d328cbd9c112943946c16256545f
  ok   notified（GetVersion 得到 DefaultVersion，走旧路径） = false
  ok   调度过的 Activity = fulfillment.inspect fulfillment.quote-taxed fulfillment.restock
  ok   label_fee（新 type fulfillment.quote-taxed：含税） = 550
  ok   fulfillment.quote-taxed 领取它的 build = app-a-v2
  ok   fulfillment.quote-taxed 实际执行的 build = app-a-v2

━━ 发布第 2 步：app-b v2 上线，替换 app-b v1（场景 1、2、3） ━━
→ 启动 app-b v2
  ok   app-b-v2 在线 = true
→ 停止 app-b v1
  ok   app-b-v1 在线（Worker.Stop 已注销路由） = false
  在线 build app-a-v1  fulfillment.inspect fulfillment.quote fulfillment.reserve fulfillment.restock fulfillment.ship
  在线 build app-a-v2  fulfillment.inspect fulfillment.notify fulfillment.quote fulfillment.quote-taxed fulfillment.reserve fulfillment.restock fulfillment.ship fulfillment.ship-v2
  在线 build app-b-v2  orders.price
  ok   order-flow 的 latest = v2
→ 新订单：v2 Program，声明新契约
  ok   Program = order-flow v2 (8f69a32a)
  ok   fulfillment.reserve 领取它的 build = app-a-v2
  ok   fulfillment.reserve 实际执行的 build = app-a-v2
  ok   场景 1：warehouse（新字段生效） = east
  ok   fulfillment.quote-taxed 领取它的 build = app-a-v2
  ok   fulfillment.quote-taxed 实际执行的 build = app-a-v2
  ok   场景 2：ship_fee（新 type：含税） = 1100
  ok   场景 3：发货走新 type = fulfillment.ship-v2@app-a-v2
→ pinned 的在途订单：留在 v1 Program，声明旧契约；reserve 派给旧 build，旧 type quote 两个 build 都可承接
  ok   Program = order-flow v1 (b52f1b6b)
  ok   fulfillment.reserve 领取它的 build = app-a-v1
  ok   fulfillment.reserve 实际执行的 build = app-a-v1
  ok   场景 1：warehouse（旧调用方不传新字段） = west
  ok   fulfillment.quote 实际执行的 build 与领取它的 build 一致 = app-a-v2
       fulfillment.quote 由 app-a-v2 执行（契约在新旧 build 上相同，两个 build 都可以承接）
  ok   场景 2：ship_fee（旧 type：不含税） = 1000
  ok   场景 3：发货走旧 type = true
→ auto_upgrade 的在途订单：下一个 Workflow Task 边界切到 v2 Program
  ok   Program = order-flow v2 (8f69a32a)
  ok   fulfillment.reserve 领取它的 build = app-a-v2
  ok   fulfillment.reserve 实际执行的 build = app-a-v2
  ok   场景 1：warehouse = east
  ok   fulfillment.quote-taxed 领取它的 build = app-a-v2
  ok   fulfillment.quote-taxed 实际执行的 build = app-a-v2
  ok   场景 2：ship_fee（新 type：含税） = 1100
  ok   场景 3：发货走旧 type（GetVersion 得到 DefaultVersion） = true
       shipped_via = fulfillment.ship@app-a-v1（旧 type 的契约在两个 build 上相同，两个 build 都可以承接）

━━ 场景 4：旧 build（app-a v1）下线，旧 Program 上的 pinned Run 还要调度旧契约 ━━
→ 停止 app-a v1
  ok   app-a-v1 在线（Worker.Stop 已注销路由） = false
  在线 build app-a-v2  fulfillment.inspect fulfillment.notify fulfillment.quote fulfillment.quote-taxed fulfillment.reserve fulfillment.restock fulfillment.ship fulfillment.ship-v2
  在线 build app-b-v2  orders.price
→ return-old-type（pinned，return-flow v1）：Signal 之后调度旧 type fulfillment.quote
  ok   Program = return-flow v1 (2595d328)
  ok   ConfigError = 
  ok   label_fee（旧 type fulfillment.quote：不含税） = 500
  ok   fulfillment.quote 领取它的 build = app-a-v2
  ok   fulfillment.quote 实际执行的 build = app-a-v2
→ order-paused 与 order-to-latest（pinned，order-flow v1）：Signal 之后要调度 fulfillment.reserve
  ok   status（暂停，仍是 Running） = running
  order-paused ConfigError: activity "fulfillment.reserve" contract mismatch on worker "app-a-v2-1" (namespace "upgrade-demo" queue "fulfillment" build "app-a-v2"): input expected "wire:a3853d0050fc8a978880bdfc9dc776f4" actual "wire:ca79189195eccac12b159552f2a291cf"
  ok   ConfigError 包含 activity "fulfillment.reserve" contract mismatch 与 build "app-a-v2" 与 input expected "wire:
  ok   fulfillment.reserve 没有开始执行 = 
  ok   status（暂停，仍是 Running） = running
  order-to-latest ConfigError: activity "fulfillment.reserve" contract mismatch on worker "app-a-v2-1" (namespace "upgrade-demo" queue "fulfillment" build "app-a-v2"): input expected "wire:a3853d0050fc8a978880bdfc9dc776f4" actual "wire:ca79189195eccac12b159552f2a291cf"
  ok   ConfigError 包含 activity "fulfillment.reserve" contract mismatch 与 build "app-a-v2" 与 input expected "wire:
  ok   fulfillment.reserve 没有开始执行 = 
→ 恢复方式一：旧 build 保持离线，把 order-to-latest 改成 auto_upgrade
  ok   order-to-latest 的 ConfigError 已清除 = 
  ok   Program = order-flow v2 (8f69a32a)
  ok   启动时的 Program 保留在 original 字段 = b52f1b6b9c0780ded4ed766fc51b21c8
  ok   恢复在 History 里留下一条 WorkflowTaskFailed(cause=run_resumed) = 1
  ok   fulfillment.reserve 领取它的 build = app-a-v2
  ok   fulfillment.reserve 实际执行的 build = app-a-v2
  ok   场景 1：warehouse = east
  ok   fulfillment.quote-taxed 领取它的 build = app-a-v2
  ok   fulfillment.quote-taxed 实际执行的 build = app-a-v2
  ok   场景 2：ship_fee（新 type：含税） = 1100
  ok   场景 3：发货走旧 type（ship 的 GetVersion 位于 v1 History 已经走过的位置，得到 DefaultVersion） = true
→ 恢复方式二：旧 build 重新上线，对 order-paused 执行 set-policy pinned
→ 启动 app-a v1
  ok   app-a-v1 在线 = true
  ok   return-flow 的 latest（旧二进制重启不会把 latest 改回去） = v2
  ok   set-policy 之前 Run 仍然暂停 = true
  ok   order-paused 的 ConfigError 已清除 = 
  ok   Program = order-flow v1 (b52f1b6b)
  ok   fulfillment.reserve 领取它的 build = app-a-v1
  ok   fulfillment.reserve 实际执行的 build = app-a-v1
  ok   fulfillment.quote 实际执行的 build 与领取它的 build 一致 = app-a-v1
       fulfillment.quote 由 app-a-v1 执行（契约在新旧 build 上相同，两个 build 都可以承接）
  ok   场景 2：ship_fee（旧 type：不含税） = 1000

━━ 场景 6：app-a v3 发布了一份没有版本保护的不兼容 Program ━━
→ 启动 app-a v3
  ok   app-a-v3 在线 = true
  ok   return-flow 的 latest = v3
→ 新启动的 Run 用 v3 Program，正常完成
  ok   Program = return-flow v3 (8b2cf863)
  ok   调度过的 Activity = fulfillment.notify fulfillment.inspect fulfillment.quote-taxed fulfillment.restock
→ auto_upgrade 的在途 Run：候选 Program 重放失败，Run 记下 UpgradeError，内容保持不变
  return-broken-pin UpgradeError: auto upgrade to 8b2cf863bfed8e5c43ee2380c92e6016 failed at workflow task 16: non_deterministic_error: nondeterministic workflow: history event is ActivityScheduled(id=#a1, type=fulfillment.inspect), replay command is ScheduleActivity(id=#a1, type=fulfillment.notify) (di=1, hi=1); retrying with backoff
  ok   status = running
  ok   Program 没有变化 = 2595d328cbd9c112943946c16256545f
  ok   UpgradeError 包含 auto upgrade to 与 non_deterministic_error
  WorkflowTaskFailed: cause=non_deterministic_error reason=nondeterministic workflow: history event is ActivityScheduled(id=#a1, type=fulfillment.inspect), replay command is ScheduleActivity(id=#a1, type=fulfillment.notify) (di=1, hi=1)
  return-broken-publish UpgradeError: auto upgrade to 8b2cf863bfed8e5c43ee2380c92e6016 failed at workflow task 16: non_deterministic_error: nondeterministic workflow: history event is ActivityScheduled(id=#a1, type=fulfillment.inspect), replay command is ScheduleActivity(id=#a1, type=fulfillment.notify) (di=1, hi=1); retrying with backoff
  ok   status = running
  ok   Program 没有变化 = 2595d328cbd9c112943946c16256545f
  ok   UpgradeError 包含 auto upgrade to 与 non_deterministic_error
  WorkflowTaskFailed: cause=non_deterministic_error reason=nondeterministic workflow: history event is ActivityScheduled(id=#a1, type=fulfillment.inspect), replay command is ScheduleActivity(id=#a1, type=fulfillment.notify) (di=1, hi=1)
→ 恢复方式一：把 Run 改成 pinned，继续用它当前的 v1 Program
  ok   UpgradeError 已清除 = 
  ok   Program = return-flow v1 (2595d328)
→ 恢复方式二：把兼容的 v2 Program 重新发布为 latest，Run 在下一次重试时接上
  ok   return-flow 的 latest = v2
  ok   UpgradeError 已清除 = 
  ok   policy = auto_upgrade
  ok   Program = return-flow v2 (d6c7f59d)
  ok   notified（v2 的 GetVersion 走旧路径） = false

━━ 汇总：每个 Run 启动时与结束时的 Program ━━
  workflow id            status    policy       启动时                          结束时
  order-basic            completed auto_upgrade order-flow v1 (b52f1b6b)     order-flow v1 (b52f1b6b)
  order-expire           completed auto_upgrade order-flow v1 (b52f1b6b)     order-flow v1 (b52f1b6b)
  return-basic           completed pinned       return-flow v1 (2595d328)    return-flow v1 (2595d328)
  order-auto             completed auto_upgrade order-flow v1 (b52f1b6b)     order-flow v2 (8f69a32a)
  order-pinned           completed pinned       order-flow v1 (b52f1b6b)     order-flow v1 (b52f1b6b)
  order-paused           completed pinned       order-flow v1 (b52f1b6b)     order-flow v1 (b52f1b6b)
  order-to-latest        completed auto_upgrade order-flow v1 (b52f1b6b)     order-flow v2 (8f69a32a)
  return-pinned          completed pinned       return-flow v1 (2595d328)    return-flow v1 (2595d328)
  return-auto            completed auto_upgrade return-flow v1 (2595d328)    return-flow v2 (d6c7f59d)
  return-old-type        completed pinned       return-flow v1 (2595d328)    return-flow v1 (2595d328)
  return-broken-pin      completed pinned       return-flow v1 (2595d328)    return-flow v1 (2595d328)
  return-broken-publish  completed auto_upgrade return-flow v1 (2595d328)    return-flow v2 (d6c7f59d)
  order-between          completed auto_upgrade order-flow v1 (b52f1b6b)     order-flow v1 (b52f1b6b)
  return-new             completed pinned       return-flow v2 (d6c7f59d)    return-flow v2 (d6c7f59d)
  order-new              completed auto_upgrade order-flow v2 (8f69a32a)     order-flow v2 (8f69a32a)
  return-on-v3           completed pinned       return-flow v3 (8b2cf863)    return-flow v3 (8b2cf863)

total 13.68s
RESULT PASS
```

## 进程内测试

`scenario/scenario_test.go` 的 `TestUpgradeScenarios` 用 `server/app/assembly` 组装的内存后端 Server 与真实 HTTP 运行同一套场景：

```text
ok  	github.com/donutnomad/tenuto/docs/examples/upgrade-demo/scenario	13.788s
```
