package scenario

import (
	"strings"
	"time"

	appav2 "github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appa/v2"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/client"
)

const (
	orderFlow  = "order-flow"
	returnFlow = "return-flow"

	policyPinned      = "pinned"
	policyAutoUpgrade = "auto_upgrade"
)

// placeResult 与 openResult 是调用方自己的 DTO，JSON 字段与 Update Handler 的返回值一致。
type placeResult struct {
	OrderRef string `json:"order_ref"`
	PlacedAt string `json:"placed_at"`
}

type openResult struct {
	Ticket string `json:"ticket"`
}

// Run 依次执行全部场景。任何断言不成立时返回错误。
func (e *Env) Run() (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			failed, ok := recovered.(failure)
			if !ok {
				panic(recovered)
			}
			err = failed.err
		}
	}()
	if e.Timeout <= 0 {
		e.Timeout = 30 * time.Second
	}
	e.sdk, err = client.Dial(client.Options{HostPort: e.URL, Namespace: e.Namespace})
	e.must(err)
	defer e.sdk.Close()
	e.admin = &adminAPI{url: e.URL}

	started := time.Now()
	e.printf("upgrade-demo → %s namespace=%s\n", e.URL, e.Namespace)

	e.section("准备：app-a v1 与 app-b v1 上线")
	e.start("app-a", "v1")
	e.start("app-b", "v1")
	e.printWorkers()
	e.expect("order-flow 的 latest", e.latest(orderFlow), "v1")
	e.expect("return-flow 的 latest", e.latest(returnFlow), "v1")

	e.basics()
	parked := e.park()
	e.rolloutAppA(parked)
	e.rolloutAppB(parked)
	e.oldBuildOffline(parked)
	e.incompatibleProgram(parked)
	e.summary()

	e.checkpoint("全部场景完成")
	e.printf("\ntotal %s\nRESULT PASS\n", time.Since(started).Round(time.Millisecond))
	return nil
}

// placeOrder 下单：启动一个 order-flow Run，订单号就是它的 Workflow ID。
// 入参的 JSON 字段与 Workflow 的入参一致；city / street 是 v2 Program 才读取的字段。
func (e *Env) placeOrder(id string, confirmWindowSeconds int) string {
	return e.startRun(orderFlow, id, map[string]any{
		"order_id": id, "sku": "sku-1", "qty": 2,
		"address": "1 Main St, Springfield", "city": "Springfield", "street": "1 Main St",
		"confirm_window_seconds": confirmWindowSeconds,
	})
}

// openReturn 发起退货：启动一个 return-flow Run，退货单号就是它的 Workflow ID。
func (e *Env) openReturn(id string) string {
	return e.startRun(returnFlow, id, map[string]any{"order_id": id, "qty": 1})
}

// ---------- 写法基础 ----------

func (e *Env) basics() {
	e.section("写法基础：一张订单走完全程（app-b 的 Workflow 跨进程调用 app-a 的 Activity）")
	e.step("Start：按 Workflow Type order-flow 启动 Run，Workflow ID 是订单号 order-basic")
	runID := e.placeOrder("order-basic", 3600)
	started := e.run(runID)
	e.expect("Workflow ID", started.WorkflowID, "order-basic")
	e.expect("Workflow Type", started.WorkflowType, orderFlow)

	e.step("Update \"place\"：按 Workflow ID 发送，同步拿到 Handler 的返回值")
	var placed placeResult
	e.update("order-basic", "place", &placed)
	e.printf("  order_ref=%s placed_at=%s run=%s\n", placed.OrderRef, placed.PlacedAt, runID)
	if placed.OrderRef == "" || placed.PlacedAt == "" {
		e.failf("update place returned an empty result: %+v", placed)
	}

	e.step("Query \"status\"：读 Workflow 的内存状态")
	status := e.waitStage(runID, "awaiting-confirm")
	e.expect("stage", str(status, "stage"), "awaiting-confirm")
	e.expect("amount（本地 Activity orders.price 的结果）", num(status, "amount"), int64(2400))
	e.expect("order_ref 与 Update 返回的一致", str(status, "order_ref"), placed.OrderRef)
	run := e.run(runID)
	e.expect("policy（来自 @Workflow 注解）", run.Policy, policyAutoUpgrade)
	e.expect("Program", e.program(runID), e.programAt(runID, run.OriginalWorkflowConfigVersion))

	e.step("Signal \"confirm\"：客户确认，Run 继续")
	e.signal(runID, "confirm")
	done := e.waitCompleted(runID)
	e.expect("status", str(done.Output, "status"), "shipped")
	e.expect("order_ref（RandomUUID，重放得到同一个值）", str(done.Output, "order_ref"), placed.OrderRef)
	e.expect("warehouse", str(done.Output, "warehouse"), "west")
	e.expect("ship_fee", num(done.Output, "ship_fee"), int64(1000))
	e.expect("shipped_via", str(done.Output, "shipped_via"), "fulfillment.ship@app-a-v1")
	e.expect("调度过的 Activity", strings.Join(e.scheduledTypes(runID), " "), "orders.price fulfillment.reserve fulfillment.quote fulfillment.ship")
	e.expect("reserve 与 quote 并行（两条 ActivityScheduled 之间没有 ActivityCompleted）", e.parallel(runID, "fulfillment.reserve", "fulfillment.quote"), true)

	// Program 存在 Server 上，Workflow Task 不按应用划分：哪个 Worker 先领到就由哪个执行。
	e.printf("       这个 Run 的 Workflow Task 执行者：%s\n", strings.Join(e.workflowTaskWorkers(runID), " "))

	e.step("Timer：确认窗口 1 秒、不发 Signal，订单过期")
	expiring := e.placeOrder("order-expire", 1)
	expired := e.waitCompleted(expiring)
	e.expect("status", str(expired.Output, "status"), "expired")
	e.expect("调度过的 Activity", strings.Join(e.scheduledTypes(expiring), " "), "orders.price")

	e.section("写法基础：一次退货（app-a 自己的 Workflow 调用自己的 Activity）")
	returnID := e.openReturn("return-basic")
	var opened openResult
	e.update("return-basic", "open", &opened)
	e.printf("  ticket=%s run=%s\n", opened.Ticket, returnID)
	e.waitStage(returnID, "awaiting-parcel")
	e.expect("policy（来自 @Workflow 注解）", e.run(returnID).Policy, policyPinned)
	e.signal(returnID, "received")
	returned := e.waitCompleted(returnID)
	e.expect("ticket", str(returned.Output, "ticket"), opened.Ticket)
	e.expect("label_fee", num(returned.Output, "label_fee"), int64(500))
	e.expect("restocked", returned.Output["restocked"], true)
}

// parallel 报告 a 与 b 是否在同一批里调度：两条 ActivityScheduled 之间没有任何 Activity 完成。
func (e *Env) parallel(runID, a, b string) bool {
	seen := 0
	for _, event := range e.events(runID) {
		if scheduled := event.ActivityScheduled; scheduled != nil && (scheduled.ActivityType == a || scheduled.ActivityType == b) {
			seen++
			if seen == 2 {
				return true
			}
		}
		if seen == 1 && event.ActivityCompleted != nil {
			return false
		}
	}
	return false
}

// ---------- 在途 Run ----------

// parkedRuns 是升级之前启动、停在 Signal 等待点上的 Run。
type parkedRuns struct {
	orderAuto, orderPinned, orderPaused, orderToLatest string
	returnPinned, returnAuto, returnOldType            string
	returnBrokenToPin, returnBrokenToPublish           string
}

func (e *Env) park() parkedRuns {
	e.section("准备：升级之前启动一批 Run（同一个 Workflow Type，各自的 Workflow ID），全部停在 Signal 等待点")
	var p parkedRuns
	order := func(id, policy string) string {
		runID := e.placeOrder(id, 3600)
		if policy == policyPinned {
			e.setPolicy(runID, policyPinned)
		}
		e.waitStage(runID, "awaiting-confirm")
		e.printf("  %-22s policy=%-12s Program=%s\n", id, e.run(runID).Policy, e.program(runID))
		return runID
	}
	ret := func(id, policy string) string {
		runID := e.openReturn(id)
		if policy == policyAutoUpgrade {
			e.setPolicy(runID, policyAutoUpgrade)
		}
		e.waitStage(runID, "awaiting-parcel")
		e.printf("  %-22s policy=%-12s Program=%s\n", id, e.run(runID).Policy, e.program(runID))
		return runID
	}
	// order-flow 的注解是 auto_upgrade，需要固定的 Run 用 set-policy 改成 pinned。
	p.orderAuto = order("order-auto", policyAutoUpgrade)
	p.orderPinned = order("order-pinned", policyPinned)
	p.orderPaused = order("order-paused", policyPinned)
	p.orderToLatest = order("order-to-latest", policyPinned)
	// return-flow 的注解是 pinned，需要跟随最新 Program 的 Run 用 set-policy 改成 auto_upgrade。
	p.returnPinned = ret("return-pinned", policyPinned)
	p.returnAuto = ret("return-auto", policyAutoUpgrade)
	p.returnOldType = ret("return-old-type", policyPinned)
	p.returnBrokenToPin = ret("return-broken-pin", policyAutoUpgrade)
	p.returnBrokenToPublish = ret("return-broken-publish", policyAutoUpgrade)
	e.checkpoint("9 个 Run 停在 Signal 等待点，全部在 v1 Program 上")
	return p
}

// ---------- 发布 app-a v2 ----------

func (e *Env) rolloutAppA(p parkedRuns) {
	e.section("发布第 1 步：app-a v2 上线，与 app-a v1 并存（场景 2、5、7）")
	e.start("app-a", "v2")
	e.printWorkers()
	e.expect("return-flow 的 latest（新 Program 随 app-a v2 的 Worker 登记）", e.latest(returnFlow), "v2")
	e.expect("order-flow 的 latest（app-b 还没发布）", e.latest(orderFlow), "v1")

	e.step("场景 7：新 Worker 已在线、新 Program 还没发布。旧 Program 上的新订单按旧契约调度，reserve 派给旧 build")
	mid := e.placeOrder("order-between", 3600)
	e.waitStage(mid, "awaiting-confirm")
	e.signal(mid, "confirm")
	midDone := e.waitCompleted(mid)
	e.expect("Program", e.program(mid), "order-flow v1 ("+short(midDone.WorkflowConfigVersion)+")")
	e.expectRoute(mid, "fulfillment.reserve", "app-a-v1", str(midDone.Output, "reserved_by"))
	e.expectServed(mid, "fulfillment.quote", str(midDone.Output, "quoted_by"))

	e.step("场景 5：新启动的 return-flow Run 用 v2 Program，走新步骤 fulfillment.notify")
	fresh := e.openReturn("return-new")
	e.waitStage(fresh, "awaiting-parcel")
	e.signal(fresh, "received")
	freshDone := e.waitCompleted(fresh)
	e.expect("Program", e.program(fresh), "return-flow v2 ("+short(freshDone.WorkflowConfigVersion)+")")
	e.expect("notified", freshDone.Output["notified"], true)
	e.expect("调度过的 Activity", strings.Join(e.scheduledTypes(fresh), " "), "fulfillment.inspect fulfillment.notify fulfillment.quote-taxed fulfillment.restock")
	e.expect("label_fee（含税报价）", num(freshDone.Output, "label_fee"), int64(550))
	e.expectRoute(fresh, "fulfillment.quote-taxed", "app-a-v2", str(freshDone.Output, "quoted_by"))

	e.step("场景 5：pinned 的在途 Run 留在 v1 Program")
	before := e.run(p.returnPinned)
	e.signal(p.returnPinned, "received")
	pinned := e.waitCompleted(p.returnPinned)
	e.expect("Program 没有变化", pinned.WorkflowConfigVersion, before.WorkflowConfigVersion)
	e.expect("Program", e.program(p.returnPinned), "return-flow v1 ("+short(pinned.WorkflowConfigVersion)+")")
	e.expect("notified", pinned.Output["notified"], false)
	e.expect("label_fee（旧 type fulfillment.quote：不含税）", num(pinned.Output, "label_fee"), int64(500))
	e.expectServed(p.returnPinned, "fulfillment.quote", str(pinned.Output, "quoted_by"))

	e.step("场景 5：auto_upgrade 的在途 Run 在下一个 Workflow Task 边界切到 v2 Program")
	before = e.run(p.returnAuto)
	e.expect("Signal 之前的 Program", e.program(p.returnAuto), "return-flow v1 ("+short(before.WorkflowConfigVersion)+")")
	e.signal(p.returnAuto, "received")
	auto := e.waitCompleted(p.returnAuto)
	e.expect("Program", e.program(p.returnAuto), "return-flow v2 ("+short(auto.WorkflowConfigVersion)+")")
	e.expect("启动时的 Program 保留在 original 字段", auto.OriginalWorkflowConfigVersion, before.WorkflowConfigVersion)
	e.expect("notified（GetVersion 得到 DefaultVersion，走旧路径）", auto.Output["notified"], false)
	e.expect("调度过的 Activity", strings.Join(e.scheduledTypes(p.returnAuto), " "), "fulfillment.inspect fulfillment.quote-taxed fulfillment.restock")
	e.expect("label_fee（新 type fulfillment.quote-taxed：含税）", num(auto.Output, "label_fee"), int64(550))
	e.expectRoute(p.returnAuto, "fulfillment.quote-taxed", "app-a-v2", str(auto.Output, "quoted_by"))
}

// ---------- 发布 app-b v2 ----------

func (e *Env) rolloutAppB(p parkedRuns) {
	e.section("发布第 2 步：app-b v2 上线，替换 app-b v1（场景 1、2、3）")
	e.start("app-b", "v2")
	e.stop("app-b", "v1")
	e.printWorkers()
	e.expect("order-flow 的 latest", e.latest(orderFlow), "v2")

	e.step("新订单：v2 Program，声明新契约")
	fresh := e.placeOrder("order-new", 3600)
	e.waitStage(fresh, "awaiting-confirm")
	e.signal(fresh, "confirm")
	freshDone := e.waitCompleted(fresh)
	e.expect("Program", e.program(fresh), "order-flow v2 ("+short(freshDone.WorkflowConfigVersion)+")")
	e.expectRoute(fresh, "fulfillment.reserve", "app-a-v2", str(freshDone.Output, "reserved_by"))
	e.expect("场景 1：warehouse（新字段生效）", str(freshDone.Output, "warehouse"), "east")
	e.expectRoute(fresh, "fulfillment.quote-taxed", "app-a-v2", str(freshDone.Output, "quoted_by"))
	e.expect("场景 2：ship_fee（新 type：含税）", num(freshDone.Output, "ship_fee"), int64(1100))
	e.expect("场景 3：发货走新 type", str(freshDone.Output, "shipped_via"), "fulfillment.ship-v2@app-a-v2")

	e.step("pinned 的在途订单：留在 v1 Program，声明旧契约；reserve 派给旧 build，旧 type quote 两个 build 都可承接")
	e.signal(p.orderPinned, "confirm")
	pinned := e.waitCompleted(p.orderPinned)
	e.expect("Program", e.program(p.orderPinned), "order-flow v1 ("+short(pinned.WorkflowConfigVersion)+")")
	e.expectRoute(p.orderPinned, "fulfillment.reserve", "app-a-v1", str(pinned.Output, "reserved_by"))
	e.expect("场景 1：warehouse（旧调用方不传新字段）", str(pinned.Output, "warehouse"), "west")
	e.expectServed(p.orderPinned, "fulfillment.quote", str(pinned.Output, "quoted_by"))
	e.expect("场景 2：ship_fee（旧 type：不含税）", num(pinned.Output, "ship_fee"), int64(1000))
	e.expect("场景 3：发货走旧 type", strings.HasPrefix(str(pinned.Output, "shipped_via"), "fulfillment.ship@"), true)

	e.step("auto_upgrade 的在途订单：下一个 Workflow Task 边界切到 v2 Program")
	e.signal(p.orderAuto, "confirm")
	auto := e.waitCompleted(p.orderAuto)
	e.expect("Program", e.program(p.orderAuto), "order-flow v2 ("+short(auto.WorkflowConfigVersion)+")")
	e.expectRoute(p.orderAuto, "fulfillment.reserve", "app-a-v2", str(auto.Output, "reserved_by"))
	e.expect("场景 1：warehouse", str(auto.Output, "warehouse"), "east")
	e.expectRoute(p.orderAuto, "fulfillment.quote-taxed", "app-a-v2", str(auto.Output, "quoted_by"))
	e.expect("场景 2：ship_fee（新 type：含税）", num(auto.Output, "ship_fee"), int64(1100))
	e.expect("场景 3：发货走旧 type（GetVersion 得到 DefaultVersion）", strings.HasPrefix(str(auto.Output, "shipped_via"), "fulfillment.ship@"), true)
	e.printf("       shipped_via = %s（旧 type 的契约在两个 build 上相同，两个 build 都可以承接）\n", str(auto.Output, "shipped_via"))
}

// ---------- 旧 build 下线 ----------

func (e *Env) oldBuildOffline(p parkedRuns) {
	e.section("场景 4：旧 build（app-a v1）下线，旧 Program 上的 pinned Run 还要调度旧契约")
	e.stop("app-a", "v1")
	e.printWorkers()

	e.step("return-old-type（pinned，return-flow v1）：Signal 之后调度旧 type fulfillment.quote")
	// app-a v2 原样保留了旧 type，契约与 v1 相同：旧 build 下线后，旧 Program 上的 Run 由新 build 承接。
	e.signal(p.returnOldType, "received")
	oldType := e.waitCompleted(p.returnOldType)
	e.expect("Program", e.program(p.returnOldType), "return-flow v1 ("+short(oldType.WorkflowConfigVersion)+")")
	e.expect("ConfigError", oldType.ConfigError, "")
	e.expect("label_fee（旧 type fulfillment.quote：不含税）", num(oldType.Output, "label_fee"), int64(500))
	e.expectRoute(p.returnOldType, "fulfillment.quote", "app-a-v2", str(oldType.Output, "quoted_by"))

	e.step("order-paused 与 order-to-latest（pinned，order-flow v1）：Signal 之后要调度 fulfillment.reserve")
	var paused *runtimeapi.Run
	for _, runID := range []string{p.orderPaused, p.orderToLatest} {
		e.signal(runID, "confirm")
		paused = e.waitRun(runID, "config error", func(run *runtimeapi.Run) bool { return run.ConfigError != "" })
		e.expect("status（暂停，仍是 Running）", paused.Status, history.StatusRunning)
		e.printf("  %s ConfigError: %s\n", paused.WorkflowID, paused.ConfigError)
		e.expectContains("ConfigError", paused.ConfigError, `activity "fulfillment.reserve" contract mismatch`, `build "app-a-v2"`, `input expected "wire:`)
		e.expect("fulfillment.reserve 没有开始执行", e.routed(runID, "fulfillment.reserve"), "")
	}

	e.checkpoint("order-paused 与 order-to-latest 因契约不一致暂停，Run 详情页顶部是红色横幅")

	e.step("恢复方式一：旧 build 保持离线，把 order-to-latest 改成 auto_upgrade")
	// set-policy 关闭暂停时打开的那个 Workflow Task 并开启一个新的；选版在新的 Workflow Task 上重新运行，
	// Run 切到 latest（v2）。v2 Program 声明 reserve 的新入参与 quote-taxed，在线的 app-a-v2 可以承接。
	e.setPolicy(p.orderToLatest, policyAutoUpgrade)
	upgraded := e.waitCompleted(p.orderToLatest)
	e.expect("order-to-latest 的 ConfigError 已清除", upgraded.ConfigError, "")
	e.expect("Program", e.program(p.orderToLatest), "order-flow v2 ("+short(upgraded.WorkflowConfigVersion)+")")
	e.expect("启动时的 Program 保留在 original 字段", upgraded.OriginalWorkflowConfigVersion, paused.WorkflowConfigVersion)
	e.expect("恢复在 History 里留下一条 WorkflowTaskFailed(cause=run_resumed)", e.workflowTaskFailures(p.orderToLatest, "run_resumed"), 1)
	e.expectRoute(p.orderToLatest, "fulfillment.reserve", "app-a-v2", str(upgraded.Output, "reserved_by"))
	e.expect("场景 1：warehouse", str(upgraded.Output, "warehouse"), "east")
	e.expectRoute(p.orderToLatest, "fulfillment.quote-taxed", "app-a-v2", str(upgraded.Output, "quoted_by"))
	e.expect("场景 2：ship_fee（新 type：含税）", num(upgraded.Output, "ship_fee"), int64(1100))
	e.expect("场景 3：发货走旧 type（ship 的 GetVersion 位于 v1 History 已经走过的位置，得到 DefaultVersion）", strings.HasPrefix(str(upgraded.Output, "shipped_via"), "fulfillment.ship@"), true)

	e.step("恢复方式二：旧 build 重新上线，对 order-paused 执行 set-policy pinned")
	e.start("app-a", "v1")
	e.expect("return-flow 的 latest（旧二进制重启不会把 latest 改回去）", e.latest(returnFlow), "v2")
	e.expect("set-policy 之前 Run 仍然暂停", e.run(p.orderPaused).ConfigError != "", true)

	e.setPolicy(p.orderPaused, policyPinned)
	resumed := e.waitCompleted(p.orderPaused)
	e.expect("order-paused 的 ConfigError 已清除", resumed.ConfigError, "")
	e.expect("Program", e.program(p.orderPaused), "order-flow v1 ("+short(resumed.WorkflowConfigVersion)+")")
	e.expectRoute(p.orderPaused, "fulfillment.reserve", "app-a-v1", str(resumed.Output, "reserved_by"))
	e.expectServed(p.orderPaused, "fulfillment.quote", str(resumed.Output, "quoted_by"))
	e.expect("场景 2：ship_fee（旧 type：不含税）", num(resumed.Output, "ship_fee"), int64(1000))
}

// workflowTaskFailures 数 Run 的 History 里某个 cause 的 WorkflowTaskFailed 事件。
func (e *Env) workflowTaskFailures(runID, cause string) int {
	count := 0
	for _, event := range e.events(runID) {
		if failed := event.WorkflowTaskFailed; failed != nil && failed.Cause == cause {
			count++
		}
	}
	return count
}

// ---------- 不兼容的 Program ----------

func (e *Env) incompatibleProgram(p parkedRuns) {
	e.section("场景 6：app-a v3 发布了一份没有版本保护的不兼容 Program")
	e.start("app-a", "v3")
	e.expect("return-flow 的 latest", e.latest(returnFlow), "v3")

	e.step("新启动的 Run 用 v3 Program，正常完成")
	fresh := e.openReturn("return-on-v3")
	e.waitStage(fresh, "awaiting-parcel")
	e.signal(fresh, "received")
	freshDone := e.waitCompleted(fresh)
	e.expect("Program", e.program(fresh), "return-flow v3 ("+short(freshDone.WorkflowConfigVersion)+")")
	e.expect("调度过的 Activity", strings.Join(e.scheduledTypes(fresh), " "), "fulfillment.notify fulfillment.inspect fulfillment.quote-taxed fulfillment.restock")

	e.step("auto_upgrade 的在途 Run：候选 Program 重放失败，Run 记下 UpgradeError，内容保持不变")
	for _, runID := range []string{p.returnBrokenToPin, p.returnBrokenToPublish} {
		before := e.run(runID)
		e.signal(runID, "received")
		broken := e.waitRun(runID, "upgrade error", func(run *runtimeapi.Run) bool { return run.UpgradeError != "" })
		e.printf("  %s UpgradeError: %s\n", broken.WorkflowID, broken.UpgradeError)
		e.expect("status", broken.Status, history.StatusRunning)
		e.expect("Program 没有变化", broken.WorkflowConfigVersion, before.WorkflowConfigVersion)
		e.expectContains("UpgradeError", broken.UpgradeError, "auto upgrade to", "non_deterministic_error")
		cause, reason := e.lastWorkflowTaskFailure(runID)
		e.printf("  WorkflowTaskFailed: cause=%s reason=%s\n", cause, reason)
	}

	e.checkpoint("return-broken-pin 与 return-broken-publish 带着 UpgradeError 重试，Run 详情页顶部是黄色横幅")

	e.step("恢复方式一：把 Run 改成 pinned，继续用它当前的 v1 Program")
	e.setPolicy(p.returnBrokenToPin, policyPinned)
	pinned := e.waitCompleted(p.returnBrokenToPin)
	e.expect("UpgradeError 已清除", pinned.UpgradeError, "")
	e.expect("Program", e.program(p.returnBrokenToPin), "return-flow v1 ("+short(pinned.WorkflowConfigVersion)+")")

	e.step("恢复方式二：把兼容的 v2 Program 重新发布为 latest，Run 在下一次重试时接上")
	_, workflowJSON, err := appav2.WorkflowDefinition().Load()
	e.must(err)
	// 与 `tenuto-workflow-gen generate -url <server> -namespace <ns> -publish ./appa/v2` 等价：
	// 登记内容并把 latest 指向它。
	e.must(e.admin.post(e.ctx(), runtimeapi.WorkflowRuntimeRegisterPath,
		runtimeapi.WorkflowRuntimeRegisterRequest{Namespace: e.Namespace, Workflow: workflowJSON}, nil))
	e.expect("return-flow 的 latest", e.latest(returnFlow), "v2")
	published := e.waitCompleted(p.returnBrokenToPublish)
	e.expect("UpgradeError 已清除", published.UpgradeError, "")
	e.expect("policy", published.Policy, policyAutoUpgrade)
	e.expect("Program", e.program(p.returnBrokenToPublish), "return-flow v2 ("+short(published.WorkflowConfigVersion)+")")
	e.expect("notified（v2 的 GetVersion 走旧路径）", published.Output["notified"], false)
}

func (e *Env) lastWorkflowTaskFailure(runID string) (cause, reason string) {
	for _, event := range e.events(runID) {
		if failed := event.WorkflowTaskFailed; failed != nil {
			cause, reason = failed.Cause, failed.Reason
		}
	}
	return cause, reason
}

// ---------- 汇总 ----------

func (e *Env) summary() {
	e.section("汇总：每个 Run 启动时与结束时的 Program")
	e.printf("  %-22s %-9s %-12s %-28s %s\n", "workflow id", "status", "policy", "启动时", "结束时")
	for _, tracked := range e.runs {
		run := e.run(tracked.runID)
		e.printf("  %-22s %-9s %-12s %-28s %s\n", tracked.workflowID, run.Status, run.Policy, tracked.started, e.program(tracked.runID))
		if run.Status != history.StatusCompleted {
			e.failf("run %s (%s) ended %s", tracked.workflowID, tracked.runID, run.Status)
		}
	}
}
