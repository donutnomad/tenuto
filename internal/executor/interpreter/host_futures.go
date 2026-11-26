package interpreter

import (
	"encoding/json"
	"errors"

	"github.com/donutnomad/tenuto/internal/executor"
	coroutine2 "github.com/donutnomad/tenuto/internal/executor/coroutine"
	"github.com/donutnomad/tenuto/sdk/apperror"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

// futureBridge 把 engine 侧 future 的事实表 ready 态桥接到 coroutine.Future：
// AddFuture 时未 ready 的 future 登记于此，每批 applyBatch 后 flush 逐个 poll，ready → Set。
// Temporal 对应物：handleActivityTaskCompleted 在事件 apply 时调 settable.Set；这里事件按批灌表，
// Set 时机收敛为「批 apply 后统一 flush」。
type futureBridge struct {
	cofut *coroutine2.Future
	// poll 返回显式 error：nil 表示没有失败。空 message 的 typed failure 也必须是非 nil error，
	// 否则 selector 会把空 message 失败当成功。
	poll func() (out map[string]any, failure error, ready bool)
}

// flushFutureBridges 批间事实增长的桥接：遍历 pending 逐个 poll，
// ready → cofut.Set（→ channel.Close → 触发 selector 已登记的 callback → 设 readyIndex）→ 移出。
// 随后 RunBatch 推进，park 在 Select 的协程醒来经 ReadyIndex() 命中——与 Temporal「apply 事件时 Set、
// 批尾推进时命中」的时序精确对齐。失败也是 ready（Set(nil, err)，照 Temporal Set 语义）。
func (e *WorkflowExecutor) flushFutureBridges() {
	remaining := e.pendingFutureBridges[:0]
	for _, b := range e.pendingFutureBridges {
		if out, failure, ready := b.poll(); ready {
			b.cofut.Set(out, failure)
			continue
		}
		remaining = append(remaining, b)
	}
	e.pendingFutureBridges = remaining
}

type ActivityFuture struct {
	executor     *WorkflowExecutor
	call         *activityCall
	activityType string
	taskQueue    string
	input        map[string]any
}

// debugEffect 是这次调用在 phase 上的调试效果。
func (f ActivityFuture) debugEffect(phase string) executor.DebugEffect {
	return executor.DebugEffect{
		Kind:             "activity",
		Phase:            phase,
		ActivityID:       f.call.id,
		Type:             f.activityType,
		ScheduledEventID: f.call.scheduledEventID,
	}
}

// Get 只等：调度已由 ExecuteActivity 立即产命令完成，Get 不再产命令。
// 检查-yield 循环，照 Temporal channelImpl.Receive：
//
//	for { 命中 completed/failed → progressed()+return；未命中 → yield（park 冻结栈） }
//
// 被 RunBatch 每轮无条件 resume 后从 yield 点续跑回到循环顶重新检查（当前 WFT 内 activity 不会
// 变 ready，故实质每轮又立刻 yield、keptBlocked 保持 true → 该协程稳定 block、参与收敛）。命中时调
// progressed()（→ scheduler.Unblocked() 清当前协程 keptBlocked）标记本轮取得进展。
// 不设 blocked：等待中的协程停在 park 上，Workflow 是否结束只看 main 是否已返回（见 emitConvergedCommand）。
func (f ActivityFuture) Get(ctx workflowContext, out *map[string]any) error {
	if f.executor == nil {
		return errors.New("workflow executor missing")
	}
	f.executor.emitDebugEffect(f.debugEffect("awaited"))
	for {
		if output, failure, ready := f.poll(); ready {
			f.executor.emitDebugEffect(f.debugEffect("resolved"))
			if f.executor.debugWaitingActivities != nil && f.executor.sched != nil {
				delete(f.executor.debugWaitingActivities, f.executor.sched.CurrentRoutineID())
			}
			if failure != nil { // 历史里这一步已经【失败】→ 返回带 type/details 的结构化错误。
				if activity := f.call.debug; activity != nil {
					activity.Status = "failed"
					activity.Error = failure.Error()
				}
				f.executor.progressed()
				return failure
			}
			// 历史里这一步已经【完成】→ 直接把历史结果塞给 out，progressed()+return（replay 快进）。
			if out != nil {
				*out = output
			}
			if activity := f.call.debug; activity != nil {
				activity.Status = "completed"
				activity.Output = output
			}
			f.executor.progressed()
			return nil
		}
		if f.executor.debugWaitingActivities != nil && f.executor.sched != nil {
			f.executor.debugWaitingActivities[f.executor.sched.CurrentRoutineID()] = executor.DebugWaitingActivity{
				ActivityID: f.call.id,
				Type:       f.activityType,
			}
		}
		// 尚未 ready（已由 ExecuteActivity 调度、等待调度事件或结果）→ yield 挂起，
		// 被下一轮 resume 后回循环顶重查（照 Temporal 的 yield → 循环重试）。
		f.executor.park()
	}
}

// poll 查事实表报告 future 当前是否 ready（不 park、不产命令、不 progressed）——ready 语义照 Temporal：
// 完成或失败都是 ready（失败对应 Set(nil, err)）。Get 的检查-yield 循环与 selectorHandle.AddFuture 的
// 桥共用本方法（单一事实源）。
func (f ActivityFuture) poll() (out map[string]any, failure error, ready bool) {
	if f.call.canceled {
		return nil, workflow.ErrCanceled, true
	}
	if f.call.scheduledEventID == 0 {
		return nil, nil, false
	}
	if completed, ok := f.executor.completed[f.call.scheduledEventID]; ok {
		return completed, nil, true
	}
	if failed, ok := f.executor.failed[f.call.scheduledEventID]; ok {
		// 以事实表存在性判失败，不以 Message 非空判失败：typed error 允许 message 为空。
		return nil, apperror.FromFailure(*failed, apperror.JSONDecoder), true
	}
	return nil, nil, false
}

// SignalFuture 等待某个名称的一条 Signal。语义与 Temporal 的 Signal Channel 一样是队列：Future 第一次被消费
// （Get 返回、或 Selector 选中它并执行回调）时取走该名称下一条未消费的 Signal，之后 Get 一直返回这一条。
// 没被消费的 Future（例如 Selector 循环里没被选中、随后丢弃的那个）不占序号。
//
// 消费发生在协程执行里，协程的调度只由 History 决定，所以每条 Signal 分给哪个 Future 在 live 与重放时相同。
type SignalFuture struct {
	executor *WorkflowExecutor
	name     string
	// claim 由 Future 的各个副本共享（WLang 值按值传递）。
	claim *signalClaim
}

type signalClaim struct {
	claimed bool
	index   int
}

// poll 报告 Future 是否 ready（纯查表，不消费、不 park、不产命令）：已消费的 Future 恒 ready；未消费的 Future
// 在该名称还有未消费的 Signal 时 ready。Signal 无失败态。Get 与 selector 桥共用。
func (f SignalFuture) poll() (out map[string]any, failure error, ready bool) {
	values := f.executor.signals[f.name]
	if f.claim.claimed {
		return values[f.claim.index], nil, true
	}
	if next := f.executor.signalsConsumed[f.name]; next < len(values) {
		return values[next], nil, true
	}
	return nil, nil, false
}

// consume 让 Future 取走该名称下一条未消费的 Signal；已消费或暂时没有 Signal 时不变。返回 Future 是否已消费。
func (f SignalFuture) consume() bool {
	if f.claim.claimed {
		return true
	}
	next := f.executor.signalsConsumed[f.name]
	if next >= len(f.executor.signals[f.name]) {
		return false
	}
	f.claim.claimed, f.claim.index = true, next
	f.executor.signalsConsumed[f.name] = next + 1
	return true
}

// Get 检查-yield 循环（照 Temporal channelImpl.Receive）：能消费 → progressed()+返回这条 Signal；没有未消费的
// Signal → blocked+yield，被下轮 resume 后回循环顶重查（Signal 在同一 WFT 内不会新到，故实质每轮又立刻 yield、
// keptBlocked 保持 → 稳定 block；下一 WFT replay 若 Signal 已到则放行）。
func (f SignalFuture) Get(ctx workflowContext, out *map[string]any) error {
	if f.executor == nil {
		return errors.New("workflow executor missing")
	}
	for {
		if f.consume() {
			if out != nil {
				*out = f.executor.signals[f.name][f.claim.index]
			}
			f.executor.progressed()
			return nil
		}
		// 没有对应 Signal 时，本轮决策停在等待点，等待新事件触发下一轮 replay。
		f.executor.blocked = true
		f.executor.park()
	}
}

type SideEffectFuture struct {
	executor *WorkflowExecutor
	id       string
	// name 是 marker name：空 = "SideEffect"（SideEffect/RandomUUID/RandomInt 复用，行为不变）；
	// MutableSideEffect 设 "MutableSideEffect"，令 Get 读回对应 marker 桶（复用同一 assignAny）。
	name string
}

// Get 读回 marker 的 value 并赋给 out（任意类型 *T）。marker 由 SideEffect()/MutableSideEffect() 保证已记录
// （首见执行闭包后 recordMarker，或 replay 命中 / 缺则已 panic）——故此处只纯读，不再产 marker/shadow/panic。
func (f SideEffectFuture) Get(ctx workflowContext, out any) error {
	if f.executor == nil {
		return errors.New("workflow executor missing")
	}
	name := f.name
	if name == "" {
		name = "SideEffect"
	}
	existing, ok := f.executor.marker(name, f.id)
	if !ok {
		return errors.New(name + " marker missing")
	}
	return assignAny(out, existing["value"])
}

// assignAny 把 marker value 归一到用户目标类型 out(*T)，用 JSON round-trip（对齐 Temporal
// EncodedValue.Get/DataConverter → JSON payload）。这是必选机制而非可选缓解：
//   - first-run：out.Go() 存进 marker 的是原生 Go 值（int64 / []string / struct）。
//   - replay：同一 marker 经 gormstore DB round-trip（marshalMap/unmarshalMap）必变 float64 / []any / map。
//
// 若用 reflect.Set 直赋，first-run 拿 int64（成）、replay 拿 float64（崩/漂移）= 非确定性。JSON round-trip
// 让两路都经同一次 json.Marshal → Unmarshal(到 *T) 归一（json 按 out 反射类型解码 float64→int64），两路一致。
// ⚠️ 死角：>2^53 整数经 DB JSON round-trip 已丢精度，JSON round-trip 也救不回（大数用 string，见 marker 注释）。
func assignAny(out any, v any) error {
	blob, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, out)
}

type ChildWorkflowFuture struct {
	executor     *WorkflowExecutor
	childID      string
	workflowType string
	input        map[string]any
}

// poll 查 childCompleted/childFailed 事实表报告 ready（纯查表，不 park/产命令/progressed）——
// completed → (out, "", true)；failed → (nil, errMsg, true)；仅 started（未闭合）或全无 → 未 ready。
// 发起+shadow 已上移到 ExecuteChildWorkflow，本 poll 与 Get / selector 桥共用。
func (f ChildWorkflowFuture) poll() (out map[string]any, failure error, ready bool) {
	if f.executor.childCanceled[f.childID] {
		return nil, workflow.ErrCanceled, true
	}
	if completed, ok := f.executor.childCompleted[f.childID]; ok {
		return completed, nil, true
	}
	if failure, ok := f.executor.childFailures[f.childID]; ok {
		// 子工作流失败以事实表存在性判定，空 message 也视为失败；结构化载荷与 Activity 失败同一条
		// 还原路径，父 run 的 errors.As / workflow.IsApplicationError / details 读取因此同样可用。
		return nil, apperror.FromFailure(*failure, apperror.JSONDecoder), true
	}
	return nil, nil, false
}

// Get 纯检查-yield 循环（照 Temporal 挂起原语）：子工作流已完成 →
// progressed()+返结果；已失败 → 返错误；未 ready → blocked+yield，被下轮 resume 后回循环顶重查。
// 发起 CmdStartChildWorkflow + replay shadow 配对已上移到 ExecuteChildWorkflow 调用点，
// Get 只剩纯检查-yield（不再产命令，避免 resume 后重复产）。
func (f ChildWorkflowFuture) Get(ctx workflowContext, out *map[string]any) error {
	if f.executor == nil {
		return errors.New("workflow executor missing")
	}
	f.executor.emitDebugEffect(executor.DebugEffect{
		Kind:              "child-workflow",
		Phase:             "awaited",
		ChildID:           f.childID,
		ChildWorkflowType: f.workflowType,
	})
	for {
		if output, failure, ready := f.poll(); ready {
			f.executor.emitDebugEffect(executor.DebugEffect{
				Kind:              "child-workflow",
				Phase:             "resolved",
				ChildID:           f.childID,
				ChildWorkflowType: f.workflowType,
			})
			if failure != nil {
				f.executor.progressed()
				return failure
			}
			if out != nil {
				*out = output
			}
			f.executor.progressed()
			return nil
		}
		f.executor.blocked = true
		f.executor.park()
	}
}
