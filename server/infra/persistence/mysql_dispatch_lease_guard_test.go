package persistence

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// 本文件锁定 ReleaseTaskDispatch 的 lease 守卫（P0 正确性缺陷）。
//
// 缺陷：ReleaseTaskDispatch 原先无条件改写整行（status=failed, lease_id='',
// lease_expires_at=NULL），没有任何守卫——而紧邻的 MarkTaskDispatchFailed 有
// `.Where("lease_id = ?", leaseID)`。
//
// 失败场景：A 实例刚 Acquire 到未过期租约、正在投递；B 实例的一次兜底清理
//（崩溃恢复 / reset 重派 / sticky 超时回落都会调 Release，且都不持 leaseID）
// 把 A 的活跃租约清成 failed。AcquireTaskDispatch 的去重门控只拒绝 dispatched 与
//**未过期的** leased，租约被清后该 task 立刻可被重新 Acquire →
// 同一 activity 被两个 worker 并发执行。stamp 只能在完成回调时拒绝陈旧结果，
// 扣款/发消息这类副作用早已跑了两次。
//
// 守卫的 WHERE 分支依赖 MySQL 的 NULL/DATETIME 比较语义。

// TestReleaseTaskDispatchDoesNotClearActiveLease 是缺陷 1 的核心回归用例。
//
// 断言两件事（缺一不可）：
//  1. B 的 Release **清不掉** A 持有的未过期活跃租约（行状态原样保留）；
//  2. Release 之后 B **依然 Acquire 不到**——这才是真正会造成"同一 activity 跑两次"
//     的那一步。只断言 (1) 不够：若把 Release 改成"清 lease_id 但保留 status=leased"，
//     (1) 会过而 (2) 会挂。
func TestReleaseTaskDispatchDoesNotClearActiveLease(t *testing.T) {
	store := requireMySQL(t)

	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeActivity,
		RunID: "run-lease-guard", ScheduledEventID: 7, Attempt: 1,
		ActivityType: "charge", TaskQueue: "default",
	}

	// A 实例抢到租约，TTL 一分钟——在整个用例期间都不会过期。
	leaseA, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatalf("A AcquireTaskDispatch: %v", err)
	}
	if !ok {
		t.Fatal("A 应当抢到租约")
	}

	// B 实例做兜底清理（不持 leaseID，与全部 4 个真实调用点一致）。
	if err := store.Dispatches.ReleaseTaskDispatch(task, "recovery"); err != nil {
		t.Fatalf("B ReleaseTaskDispatch: %v", err)
	}

	// 断言 1：A 的活跃租约原封不动。
	state, found, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil {
		t.Fatalf("GetTaskDispatch: %v", err)
	}
	if !found {
		t.Fatal("dispatch 行不应消失")
	}
	if state.LeaseID != leaseA.LeaseID {
		t.Fatalf("A 的活跃租约被 B 的 Release 清掉了（缺陷 1 回归）: lease_id = %q, want %q",
			state.LeaseID, leaseA.LeaseID)
	}
	if state.Status != dispatch.StatusLeased {
		t.Fatalf("A 的租约状态被改写（缺陷 1 回归）: status = %q, want %q",
			state.Status, dispatch.StatusLeased)
	}

	// 断言 2：租约仍然挡得住 B 的抢占——同一 activity 不会被并发执行。
	_, ok, err = store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil {
		t.Fatalf("B AcquireTaskDispatch: %v", err)
	}
	if ok {
		t.Fatal("B 在 A 持有活跃租约期间抢到了租约（缺陷 1 回归）——同一 activity 会被两个 worker 并发执行")
	}
}

// TestReleaseTaskDispatchClearsExpiredLease 确认守卫没有做成死锁：
// 租约**已过期**时 Release 必须照常清理，否则 sticky 回落 / 崩溃恢复会永久卡住
// （sticky_routing.go 的注释：Release 不成功则 task 永久孤立）。
func TestReleaseTaskDispatchClearsExpiredLease(t *testing.T) {
	store := requireMySQL(t)

	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-lease-expired", ScheduledEventID: 11,
	}

	// 负 TTL：落库即已过期，无需 sleep。
	if _, ok, err := store.Dispatches.AcquireTaskDispatch(task, -time.Minute); err != nil || !ok {
		t.Fatalf("AcquireTaskDispatch ok=%v err=%v", ok, err)
	}

	if err := store.Dispatches.ReleaseTaskDispatch(task, "sticky schedule-to-start timeout"); err != nil {
		t.Fatalf("ReleaseTaskDispatch: %v", err)
	}

	state, found, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil || !found {
		t.Fatalf("GetTaskDispatch found=%v err=%v", found, err)
	}
	if state.LeaseID != "" {
		t.Fatalf("过期租约未被清理: lease_id = %q, want \"\"", state.LeaseID)
	}
	if state.Status != dispatch.StatusFailed {
		t.Fatalf("过期租约未被置为 failed: status = %q", state.Status)
	}

	// 清理后必须可以重新抢到——这是回落路径不孤立 task 的前提。
	if _, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute); err != nil || !ok {
		t.Fatalf("清理后应可重新 Acquire: ok=%v err=%v", ok, err)
	}
}

// TestReleaseTaskDispatchClearsWhenNoActiveLease 覆盖守卫的其余两支：
// 行不存在（Release 建行）、以及 status=dispatched（不是活跃租约，可清）。
func TestReleaseTaskDispatchClearsWhenNoActiveLease(t *testing.T) {
	store := requireMySQL(t)

	// (a) 行不存在：Release 是 no-op，不得报错。
	missing := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeWorkflow,
		RunID: "run-lease-missing", ScheduledEventID: 3,
	}
	if err := store.Dispatches.ReleaseTaskDispatch(missing, "recovery"); err != nil {
		t.Fatalf("对不存在的行 Release 不应报错: %v", err)
	}

	// (b) status=dispatched：租约已交付队列，不属于"活跃租约"，Release 必须能清
	//     （redispatchActivityTask 的 reset 重派路径依赖这一点）。
	task := protocol.Task{
		Namespace: protocol.DefaultNamespace, Type: protocol.TaskTypeActivity,
		RunID: "run-lease-dispatched", ScheduledEventID: 5, Attempt: 1,
		ActivityType: "notify", TaskQueue: "default",
	}
	lease, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireTaskDispatch ok=%v err=%v", ok, err)
	}
	if err := store.Dispatches.MarkTaskDispatchDispatched(task, lease.LeaseID); err != nil {
		t.Fatalf("MarkTaskDispatchDispatched: %v", err)
	}

	if err := store.Dispatches.ReleaseTaskDispatch(task, "redispatch"); err != nil {
		t.Fatalf("ReleaseTaskDispatch: %v", err)
	}
	state, found, err := store.Dispatches.GetTaskDispatch(task)
	if err != nil || !found {
		t.Fatalf("GetTaskDispatch found=%v err=%v", found, err)
	}
	if state.Status != dispatch.StatusFailed || state.LeaseID != "" {
		t.Fatalf("dispatched 行未被 Release 清理: status=%q lease_id=%q", state.Status, state.LeaseID)
	}
	// reset 重派的关键前提：清理后能重新 Acquire。
	if _, ok, err := store.Dispatches.AcquireTaskDispatch(task, time.Minute); err != nil || !ok {
		t.Fatalf("清理后应可重新 Acquire: ok=%v err=%v", ok, err)
	}
}
