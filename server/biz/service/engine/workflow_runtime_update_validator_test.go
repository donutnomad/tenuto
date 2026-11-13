package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// ============================================
// 无状态 input validator 测试（阶段 B'）
// ============================================
// 测试策略（对标 Temporal validator 的"拒绝零 history"，但只做无状态：只看 name+input）:
// - Error Case: 注册拒绝 validator → UpdateWorkflow 返 error 且历史无 accepted 事件（零 history）
// - Happy Path: 未注册 validator → 正常 accepted（加法不改法，老行为）
// - Edge Case: 拒绝路径无副作用 —— 不调度 WFT、不注册 waiter
// ============================================

// countWorkflowTaskScheduled 统计历史里 WorkflowTaskScheduled 事件条数（判断是否 ScheduleWFT）。
func countWorkflowTaskScheduled(t *testing.T, env *updateWaitTestEnv) int {
	t.Helper()
	events, err := env.server.GetEvents(env.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range events {
		if ev.WorkflowTaskScheduled != nil {
			n++
		}
	}
	return n
}

// wfQueueLen 读 workflow task queue 当前排队任务数（自带锁，避免与 Enqueue race）。
func wfQueueLen(q *testTaskQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// TestUpdateValidator_RejectsBadInput_ZeroHistory
// Error Case: 注册一个拒绝 validator，非法 input 的 UpdateWorkflow 返回 error（可 errors.Is ErrUpdateRejected），
// 且历史里无该 updateID 的 accepted 事件 —— 拒绝零 history（写 accepted 之前就被拦下）。
func TestUpdateValidator_RejectsBadInput_ZeroHistory(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-validator-reject")
	ctx := context.Background()
	const updateID = "up-reject"

	errBadInput := errors.New("priority is required")
	env.runtime.RegisterUpdateValidator("set_priority", func(name string, args protocol.Payloads) error {
		input := firstUpdateArg(args)
		if input["priority"] == nil {
			return errBadInput
		}
		return nil
	})

	// input 缺 priority → validator 拒绝。
	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{}))
	if err == nil {
		t.Fatalf("expected reject error, got nil (result=%+v)", result)
	}
	if !errors.Is(err, protocol.ErrUpdateRejected) {
		t.Fatalf("error must wrap ErrUpdateRejected, got %v", err)
	}
	if !errors.Is(err, errBadInput) {
		t.Fatalf("error must preserve validator cause in chain, got %v", err)
	}

	// 零 history：无 accepted、无 completed。
	acc, comp := countUpdateEvents(t, env, updateID)
	if acc != 0 || comp != 0 {
		t.Fatalf("reject must write zero history, got accepted=%d completed=%d", acc, comp)
	}
}

// TestUpdateValidator_PassThroughWhenNone
// Happy Path: 未注册 validator（默认全通过）→ update 正常 accepted，历史落一条 accepted —— 老行为不变。
func TestUpdateValidator_PassThroughWhenNone(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-validator-none")
	ctx := context.Background()
	const updateID = "up-passthrough"

	// 不注册任何 validator。
	result, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Accepted, updateArgs(map[string]any{"priority": "high"}))
	if err != nil {
		t.Fatalf("no validator must pass through, got err=%v", err)
	}
	if result.Stage != protocol.UpdateReachedStageEnums.Accepted || result.AcceptedEventID == 0 {
		t.Fatalf("expected accepted result, got %+v", result)
	}

	acc, comp := countUpdateEvents(t, env, updateID)
	if acc != 1 || comp != 0 {
		t.Fatalf("pass-through must write one accepted, got accepted=%d completed=%d", acc, comp)
	}
}

// TestUpdateValidator_RejectDoesNotScheduleWFT
// Edge Case: 拒绝路径无副作用 —— 不注册 waiter、不调度 WFT。
// 对比：拒绝前 WFT 计数与 queue 长度，拒绝后应完全不变。
func TestUpdateValidator_RejectDoesNotScheduleWFT(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-validator-no-wft")
	ctx := context.Background()
	const updateID = "up-reject-no-wft"

	env.runtime.RegisterUpdateValidator("set_priority", func(name string, args protocol.Payloads) error {
		return errors.New("always reject")
	})

	wftBefore := countWorkflowTaskScheduled(t, env)
	queueBefore := wfQueueLen(env.wfQueue)

	// 用 Completed stage：若拒绝路径漏网会 Register waiter，正好一并验证不泄漏。
	_, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{"priority": "high"}))
	if !errors.Is(err, protocol.ErrUpdateRejected) {
		t.Fatalf("expected ErrUpdateRejected, got %v", err)
	}

	// 不调度 WFT：历史 WorkflowTaskScheduled 计数与 queue 长度不变。
	if got := countWorkflowTaskScheduled(t, env); got != wftBefore {
		t.Fatalf("reject must not schedule WFT: scheduled count %d -> %d", wftBefore, got)
	}
	if got := wfQueueLen(env.wfQueue); got != queueBefore {
		t.Fatalf("reject must not enqueue WFT task: queue len %d -> %d", queueBefore, got)
	}
	// 不注册 waiter：EntryCount 应为 0（拒绝在 Register 之前，无需 cancel）。
	if c := env.server.runWatch.EntryCount(); c != 0 {
		t.Fatalf("reject must not register waiter, count=%d", c)
	}
	// 零 history 兜底。
	if acc, comp := countUpdateEvents(t, env, updateID); acc != 0 || comp != 0 {
		t.Fatalf("reject must write zero history, got accepted=%d completed=%d", acc, comp)
	}
}

// Error Case（缺陷 5）：validator 的 typed 应用错误原样留在拒绝原因里。
// errors.Is 同时命中 engine 与共享哨兵，errors.As 拿到 *apperror.Error 的 Type/NonRetryable，
// UpdateRejectionCause 能取回同一个原因（传输层据此编成 protocol.Failure）；拒绝仍是同步零副作用。
func TestUpdateValidatorRejectionKeepsTypedCause(t *testing.T) {
	env := newUpdateWaitTestEnv(t, "run-validator-typed")
	ctx := context.Background()
	const updateID = "up-reject-typed"

	validatorErr := apperror.NewNonRetryable("card has expired", "CardExpired", nil,
		map[string]any{"reason": "insufficient_funds"})
	env.runtime.RegisterUpdateValidator("set_priority", func(name string, args protocol.Payloads) error {
		return validatorErr
	})

	wftBefore := countWorkflowTaskScheduled(t, env)
	_, err := env.runtime.UpdateWorkflow(ctx, "", env.run.WorkflowID, env.run.ID, "set_priority", updateID, protocol.UpdateWaitStageEnums.Completed, updateArgs(map[string]any{}))
	if err == nil {
		t.Fatal("expected reject error, got nil")
	}
	if !errors.Is(err, protocol.ErrUpdateRejected) {
		t.Fatalf("errors.Is(ErrUpdateRejected) = false, err = %v", err)
	}
	if !errors.Is(err, protocol.ErrUpdateRejected) {
		t.Fatalf("errors.Is(protocol.ErrUpdateRejected) = false, err = %v", err)
	}
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Type() != "CardExpired" || !appErr.NonRetryable() {
		t.Fatalf("typed validator cause lost: %+v", err)
	}
	cause, ok := UpdateRejectionCause(err)
	if !ok || cause != validatorErr {
		t.Fatalf("UpdateRejectionCause = (%v, %v), want the validator error", cause, ok)
	}

	// 同步拒绝零副作用：零 history、不注册 waiter、不调度 WFT。
	if acc, comp := countUpdateEvents(t, env, updateID); acc != 0 || comp != 0 {
		t.Fatalf("reject must write zero history, got accepted=%d completed=%d", acc, comp)
	}
	if c := env.server.runWatch.EntryCount(); c != 0 {
		t.Fatalf("reject must not register waiter, count=%d", c)
	}
	if got := countWorkflowTaskScheduled(t, env); got != wftBefore {
		t.Fatalf("reject must not schedule WFT: %d -> %d", wftBefore, got)
	}
}
