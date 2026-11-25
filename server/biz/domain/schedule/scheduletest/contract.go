// Package scheduletest 提供 schedule.Repository 的契约测试，各实现的测试用同一套断言运行。
package scheduletest

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
)

// RunRepositoryContract 对 newRepo 返回的实现运行仓储契约；每个子测试拿到一个空仓储。
func RunRepositoryContract(t *testing.T, newRepo func(t *testing.T) schedule.Repository) {
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, newRepo(t)) })
	t.Run("GetAndDeleteMissing", func(t *testing.T) { testGetAndDeleteMissing(t, newRepo(t)) })
	t.Run("CreateDuplicate", func(t *testing.T) { testCreateDuplicate(t, newRepo(t)) })
	t.Run("ListSchedulesByNamespace", func(t *testing.T) { testListSchedulesByNamespace(t, newRepo(t)) })
	t.Run("ListDueSchedules", func(t *testing.T) { testListDueSchedules(t, newRepo(t)) })
	t.Run("UpdateAndDelete", func(t *testing.T) { testUpdateAndDelete(t, newRepo(t)) })
	t.Run("UpdateMissing", func(t *testing.T) { testUpdateMissing(t, newRepo(t)) })
	t.Run("ReadsAreIsolatedFromCallers", func(t *testing.T) { testReadsAreIsolated(t, newRepo(t)) })
	t.Run("UpdateWritesOnlyChangedFields", func(t *testing.T) { testUpdateWritesOnlyChangedFields(t, newRepo(t)) })
	t.Run("NamespaceIsNormalized", func(t *testing.T) { testNamespaceIsNormalized(t, newRepo(t)) })
	t.Run("ListOrder", func(t *testing.T) { testListOrder(t, newRepo(t)) })
	t.Run("NextRunAtCAS", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) { testCASSuccess(t, newRepo(t)) })
		t.Run("Fail", func(t *testing.T) { testCASFail(t, newRepo(t)) })
		t.Run("NilOld", func(t *testing.T) { testCASNilOld(t, newRepo(t)) })
	})
}

func newTestSchedule(id, namespace, workflowType, cron string) *schedule.Schedule {
	now := time.Now().UTC().Truncate(time.Millisecond)
	nextRunAt := now.Add(time.Hour)
	return &schedule.Schedule{
		ID:             id,
		Namespace:      namespace,
		WorkflowType:   workflowType,
		Description:    "test schedule",
		CronExpression: cron,
		Timezone:       "UTC",
		Input:          map[string]any{"key": "value"},
		Paused:         false,
		OverlapPolicy:  "SKIP",
		NextRunAt:      &nextRunAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func testGetAndDeleteMissing(t *testing.T, store schedule.Repository) {
	// Error Case: 读取与删除不存在的 schedule 都返回错误
	if _, err := store.GetSchedule("default", "sch-missing"); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("GetSchedule on a missing schedule: err = %v, want schedule.ErrNotFound", err)
	}
	if err := store.DeleteSchedule("default", "sch-missing"); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("DeleteSchedule on a missing schedule: err = %v, want schedule.ErrNotFound", err)
	}
}

func testListSchedulesByNamespace(t *testing.T, store schedule.Repository) {
	// Happy Path: ListSchedules 只返回指定 namespace 的 schedule
	for _, sch := range []*schedule.Schedule{
		newTestSchedule("sch-a1", "ns-a", "wf-1", "* * * * *"),
		newTestSchedule("sch-a2", "ns-a", "wf-2", "* * * * *"),
		newTestSchedule("sch-b1", "ns-b", "wf-1", "* * * * *"),
	} {
		if err := store.CreateSchedule(sch); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ListSchedules("ns-a")
	if err != nil {
		t.Fatalf("ListSchedules failed: %v", err)
	}
	ids := map[string]bool{}
	for _, sch := range got {
		ids[sch.ID] = true
	}
	if len(got) != 2 || !ids["sch-a1"] || !ids["sch-a2"] {
		t.Errorf("expected sch-a1 and sch-a2 in ns-a, got %v", ids)
	}
}

func testRoundTrip(t *testing.T, store schedule.Repository) {
	// Happy Path: 全字段写入后读取，验证所有字段完整保留

	now := time.Now().UTC().Truncate(time.Millisecond)
	lastRunAt := now.Add(-time.Hour)
	nextRunAt := now.Add(time.Hour)

	sch := &schedule.Schedule{
		ID:             "sch-roundtrip",
		Namespace:      "default",
		WorkflowType:   "wf-1",
		Description:    "a test schedule",
		CronExpression: "*/5 * * * *",
		Timezone:       "Asia/Shanghai",
		Input:          map[string]any{"order_id": "o-1", "amount": float64(100)},
		Paused:         false,
		OverlapPolicy:  "SKIP",
		LastRunAt:      &lastRunAt,
		NextRunAt:      &nextRunAt,
		LastRunID:      "run-prev",
		RunCount:       42,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := store.CreateSchedule(sch); err != nil {
		t.Fatalf("CreateSchedule failed: %v", err)
	}

	got, err := store.GetSchedule("default", "sch-roundtrip")
	if err != nil {
		t.Fatalf("GetSchedule failed: %v", err)
	}

	if got.ID != sch.ID {
		t.Errorf("ID: want %q, got %q", sch.ID, got.ID)
	}
	if got.Namespace != sch.Namespace {
		t.Errorf("Namespace: want %q, got %q", sch.Namespace, got.Namespace)
	}
	if got.WorkflowType != sch.WorkflowType {
		t.Errorf("WorkflowType: want %q, got %q", sch.WorkflowType, got.WorkflowType)
	}
	if got.Description != sch.Description {
		t.Errorf("Description: want %q, got %q", sch.Description, got.Description)
	}
	if got.CronExpression != sch.CronExpression {
		t.Errorf("CronExpression: want %q, got %q", sch.CronExpression, got.CronExpression)
	}
	if got.Timezone != sch.Timezone {
		t.Errorf("Timezone: want %q, got %q", sch.Timezone, got.Timezone)
	}
	if got.Input["order_id"] != "o-1" {
		t.Errorf("Input[order_id]: want %q, got %v", "o-1", got.Input["order_id"])
	}
	if got.Paused != sch.Paused {
		t.Errorf("Paused: want %v, got %v", sch.Paused, got.Paused)
	}
	if got.OverlapPolicy != sch.OverlapPolicy {
		t.Errorf("OverlapPolicy: want %q, got %q", sch.OverlapPolicy, got.OverlapPolicy)
	}
	if got.LastRunID != sch.LastRunID {
		t.Errorf("LastRunID: want %q, got %q", sch.LastRunID, got.LastRunID)
	}
	if got.RunCount != sch.RunCount {
		t.Errorf("RunCount: want %d, got %d", sch.RunCount, got.RunCount)
	}
	if got.LastRunAt == nil || !got.LastRunAt.Equal(lastRunAt) {
		t.Errorf("LastRunAt: want %v, got %v", lastRunAt, got.LastRunAt)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(nextRunAt) {
		t.Errorf("NextRunAt: want %v, got %v", nextRunAt, got.NextRunAt)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt: want %v, got %v", now, got.CreatedAt)
	}
}

func testListDueSchedules(t *testing.T, store schedule.Repository) {
	// Happy Path: 到期的被返回，Paused 不返回，未到期不返回

	now := time.Now().UTC()

	// 1. 到期 schedule（应返回）
	due := newTestSchedule("sch-due", "default", "wf-due", "* * * * *")
	past := now.Add(-1 * time.Minute)
	due.NextRunAt = &past
	if err := store.CreateSchedule(due); err != nil {
		t.Fatal(err)
	}

	// 2. 暂停的 schedule（不应返回）
	paused := newTestSchedule("sch-paused", "default", "wf-paused", "* * * * *")
	past2 := now.Add(-2 * time.Minute)
	paused.NextRunAt = &past2
	paused.Paused = true
	if err := store.CreateSchedule(paused); err != nil {
		t.Fatal(err)
	}

	// 3. 未到期 schedule（不应返回）
	future := newTestSchedule("sch-future", "default", "wf-future", "* * * * *")
	futureTime := now.Add(10 * time.Minute)
	future.NextRunAt = &futureTime
	if err := store.CreateSchedule(future); err != nil {
		t.Fatal(err)
	}

	// 4. NextRunAt 为 nil 的 schedule（不应返回）
	nilTime := newTestSchedule("sch-nil", "default", "wf-nil", "* * * * *")
	nilTime.NextRunAt = nil
	if err := store.CreateSchedule(nilTime); err != nil {
		t.Fatal(err)
	}

	result, err := store.ListDueSchedules(now)
	if err != nil {
		t.Fatalf("ListDueSchedules failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 due schedule, got %d", len(result))
	}
	if result[0].ID != "sch-due" {
		t.Errorf("expected sch-due, got %s", result[0].ID)
	}
}

func testUpdateAndDelete(t *testing.T, store schedule.Repository) {
	// Happy Path: 更新后读取验证，然后删除，再读取应报错

	sch := newTestSchedule("sch-upd", "ns1", "wf-upd", "0 * * * *")
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	// 更新：经领域方法改字段
	description := "updated desc"
	if err := sch.Update(schedule.Changes{Description: &description}, time.Now().UTC().Truncate(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		sch.RecordRun("run-upd", time.Now().UTC().Truncate(time.Millisecond))
	}
	if err := store.UpdateSchedule(sch); err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}

	got, err := store.GetSchedule("ns1", "sch-upd")
	if err != nil {
		t.Fatal(err)
	}
	if got.Description != "updated desc" {
		t.Errorf("expected Description='updated desc', got %q", got.Description)
	}
	if got.RunCount != 5 {
		t.Errorf("expected RunCount=5, got %d", got.RunCount)
	}

	// 删除
	if err := store.DeleteSchedule("ns1", "sch-upd"); err != nil {
		t.Fatalf("DeleteSchedule failed: %v", err)
	}

	_, err = store.GetSchedule("ns1", "sch-upd")
	if !errors.Is(err, schedule.ErrNotFound) {
		t.Fatalf("after delete: err = %v, want schedule.ErrNotFound", err)
	}
}

func testUpdateWritesOnlyChangedFields(t *testing.T, store schedule.Repository) {
	// Concurrency: 持有旧快照的写入方只写自己改过的字段，不把别的写入方已提交的字段改回去
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := store.CreateSchedule(newTestSchedule("sch-patch", "default", "wf-patch", "* * * * *")); err != nil {
		t.Fatal(err)
	}
	// 写入方 A 与 B 各自读出同一份快照。
	pauser, err := store.GetSchedule("default", "sch-patch")
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := store.GetSchedule("default", "sch-patch")
	if err != nil {
		t.Fatal(err)
	}
	// 没有经领域方法改动的更新不改变已存的记录。
	idle, err := store.GetSchedule("default", "sch-patch")
	if err != nil {
		t.Fatal(err)
	}
	// A 暂停并提交。
	pauser.Pause(now)
	if err := store.UpdateSchedule(pauser); err != nil {
		t.Fatal(err)
	}
	// B 拿着暂停之前的快照记录一次触发。
	trigger.RecordRun("run-1", now)
	if err := store.UpdateSchedule(trigger); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateSchedule(idle); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetSchedule("default", "sch-patch")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Paused || got.NextRunAt != nil {
		t.Errorf("pause was undone by a writer holding an older snapshot: paused=%v next_run_at=%v", got.Paused, got.NextRunAt)
	}
	if got.LastRunID != "run-1" || got.RunCount != 1 || got.LastRunAt == nil {
		t.Errorf("run record missing: last_run_id=%q run_count=%d last_run_at=%v", got.LastRunID, got.RunCount, got.LastRunAt)
	}
	if got.Description != "test schedule" || got.Input["key"] != "value" {
		t.Errorf("fields nobody changed were lost: %+v", got)
	}
}

func testNamespaceIsNormalized(t *testing.T, store schedule.Repository) {
	// Edge Case: 空 namespace 与 "default" 是同一个 namespace
	sch := newTestSchedule("sch-ns", "", "wf-ns", "* * * * *")
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"", "default"} {
		got, err := store.GetSchedule(namespace, "sch-ns")
		if err != nil {
			t.Fatalf("GetSchedule(%q): %v", namespace, err)
		}
		if got.Namespace != "default" {
			t.Errorf("GetSchedule(%q) namespace = %q, want default", namespace, got.Namespace)
		}
		list, err := store.ListSchedules(namespace)
		if err != nil || len(list) != 1 {
			t.Fatalf("ListSchedules(%q) = %d schedules, err=%v", namespace, len(list), err)
		}
	}
	duplicate := newTestSchedule("sch-ns", "default", "wf-ns", "* * * * *")
	if err := store.CreateSchedule(duplicate); !errors.Is(err, schedule.ErrAlreadyExists) {
		t.Errorf("create under the normalized namespace: err = %v, want schedule.ErrAlreadyExists", err)
	}
	sch.Pause(time.Now().UTC().Truncate(time.Millisecond))
	if err := store.UpdateSchedule(sch); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetSchedule("default", "sch-ns"); err != nil || !got.Paused {
		t.Errorf("update through the empty namespace did not reach the record: %+v err=%v", got, err)
	}
	if err := store.DeleteSchedule("", "sch-ns"); err != nil {
		t.Errorf("DeleteSchedule with the empty namespace: %v", err)
	}
}

func testListOrder(t *testing.T, store schedule.Repository) {
	// Happy Path: ListSchedules 按 (CreatedAt, ID)、ListDueSchedules 按 (NextRunAt, ID) 升序
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	// 创建顺序与期望顺序不同；c、a 的时间相同，由 ID 决定先后。
	for _, item := range []struct {
		id     string
		offset time.Duration
	}{{"sch-c", time.Minute}, {"sch-b", 0}, {"sch-a", time.Minute}, {"sch-d", 2 * time.Minute}} {
		sch := newTestSchedule(item.id, "default", "wf-order", "* * * * *")
		at := base.Add(item.offset)
		sch.CreatedAt, sch.NextRunAt = at, &at
		if err := store.CreateSchedule(sch); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(list []*schedule.Schedule) []string {
		out := make([]string, 0, len(list))
		for _, sch := range list {
			out = append(out, sch.ID)
		}
		return out
	}
	want := []string{"sch-b", "sch-a", "sch-c", "sch-d"}
	list, err := store.ListSchedules("default")
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(list); !slicesEqual(got, want) {
		t.Errorf("ListSchedules order = %v, want %v", got, want)
	}
	due, err := store.ListDueSchedules(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(due); !slicesEqual(got, want) {
		t.Errorf("ListDueSchedules order = %v, want %v", got, want)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testCASSuccess(t *testing.T, store schedule.Repository) {
	// Happy Path: CAS 更新 NextRunAt 成功（oldNextRunAt 匹配）

	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-time.Minute)
	sch := newTestSchedule("sch-cas", "default", "wf-cas", "* * * * *")
	sch.NextRunAt = &old
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	newNext := now.Add(time.Hour)
	sch.NextRunAt = &newNext
	sch.UpdatedAt = now

	ok, err := store.UpdateScheduleNextRunAtCAS(sch, &old)
	if err != nil {
		t.Fatalf("UpdateScheduleNextRunAtCAS failed: %v", err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed when old value matches")
	}

	got, _ := store.GetSchedule("default", "sch-cas")
	if got.NextRunAt == nil || !got.NextRunAt.Equal(newNext) {
		t.Errorf("expected NextRunAt=%v, got %v", newNext, got.NextRunAt)
	}
}

func testCASFail(t *testing.T, store schedule.Repository) {
	// Error Case: CAS 更新失败（oldNextRunAt 不匹配，表示被抢先）

	now := time.Now().UTC().Truncate(time.Millisecond)
	actualVal := now.Add(-time.Minute)
	sch := newTestSchedule("sch-cas-fail", "default", "wf-cas-fail", "* * * * *")
	sch.NextRunAt = &actualVal
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	// 传入错误的旧值（模拟并发已更新）
	wrongOld := now.Add(-2 * time.Minute)
	newNext := now.Add(time.Hour)
	sch.NextRunAt = &newNext

	ok, err := store.UpdateScheduleNextRunAtCAS(sch, &wrongOld)
	if err != nil {
		t.Fatalf("UpdateScheduleNextRunAtCAS returned unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected CAS to fail when old value does not match")
	}
}

func testCASNilOld(t *testing.T, store schedule.Repository) {
	// Edge Case: oldNextRunAt 为 nil 时 WHERE next_run_at IS NULL

	now := time.Now().UTC().Truncate(time.Millisecond)
	sch := newTestSchedule("sch-cas-nil", "default", "wf-cas-nil", "* * * * *")
	sch.NextRunAt = nil // 初始为 nil
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}

	newNext := now.Add(time.Hour)
	sch.NextRunAt = &newNext
	sch.UpdatedAt = now

	ok, err := store.UpdateScheduleNextRunAtCAS(sch, nil)
	if err != nil {
		t.Fatalf("UpdateScheduleNextRunAtCAS with nil old failed: %v", err)
	}
	if !ok {
		t.Fatal("expected CAS to succeed when old is nil and DB has NULL")
	}

	got, _ := store.GetSchedule("default", "sch-cas-nil")
	if got.NextRunAt == nil || !got.NextRunAt.Equal(newNext) {
		t.Errorf("expected NextRunAt=%v, got %v", newNext, got.NextRunAt)
	}
}

func testCreateDuplicate(t *testing.T, store schedule.Repository) {
	// Error Case: 同一 namespace 下重复的 ID 被拒绝，已有记录保持原样
	now := time.Now().UTC().Truncate(time.Millisecond)
	first := &schedule.Schedule{ID: "sch-dup", Namespace: "default", WorkflowType: "wf-1", CronExpression: "* * * * *", CreatedAt: now, UpdatedAt: now}
	if err := store.CreateSchedule(first); err != nil {
		t.Fatal(err)
	}
	second := &schedule.Schedule{ID: "sch-dup", Namespace: "default", WorkflowType: "wf-2", CronExpression: "* * * * *", CreatedAt: now, UpdatedAt: now}
	if err := store.CreateSchedule(second); !errors.Is(err, schedule.ErrAlreadyExists) {
		t.Fatalf("duplicate create: err = %v, want schedule.ErrAlreadyExists", err)
	}
	got, err := store.GetSchedule("default", "sch-dup")
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowType != "wf-1" {
		t.Fatalf("workflow type = %q, want the first record's wf-1", got.WorkflowType)
	}
}

func testUpdateMissing(t *testing.T, store schedule.Repository) {
	// Error Case: 更新不存在的 schedule 返回 ErrNotFound，有改动与没有改动时都一样，也不会建出记录
	sch := newTestSchedule("sch-missing", "ns1", "wf-missing", "0 * * * *")
	if err := store.UpdateSchedule(sch); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("UpdateSchedule without changes on a missing schedule: err = %v, want schedule.ErrNotFound", err)
	}
	sch.RecordRun("run-1", time.Now().UTC().Truncate(time.Millisecond))
	if err := store.UpdateSchedule(sch); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("UpdateSchedule on a missing schedule: err = %v, want schedule.ErrNotFound", err)
	}
	if _, err := store.GetSchedule("ns1", "sch-missing"); !errors.Is(err, schedule.ErrNotFound) {
		t.Errorf("GetSchedule after updating a missing schedule: err = %v, want schedule.ErrNotFound", err)
	}
}

func testReadsAreIsolated(t *testing.T, store schedule.Repository) {
	// Edge Case: 调用方改动写入时与读出后的实体（Input 与时间指针）不影响已存的 schedule
	sch := newTestSchedule("sch-isolated", "ns1", "wf-isolated", "0 * * * *")
	wantNext := *sch.NextRunAt
	if err := store.CreateSchedule(sch); err != nil {
		t.Fatal(err)
	}
	sch.Input["key"] = "changed after create"
	*sch.NextRunAt = wantNext.Add(time.Minute)

	read := func() *schedule.Schedule {
		t.Helper()
		got, err := store.GetSchedule("ns1", "sch-isolated")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := read()
	if first.Input["key"] != "value" || !first.NextRunAt.Equal(wantNext) {
		t.Fatalf("stored schedule follows the caller's entity: input=%v next=%v", first.Input, first.NextRunAt)
	}
	first.Input["key"] = "changed after read"
	*first.NextRunAt = wantNext.Add(2 * time.Minute)
	for _, listed := range mustList(t, store) {
		if listed.ID == "sch-isolated" && listed.Input["key"] == "changed after read" {
			t.Fatal("listed schedule shares the Input of an earlier read")
		}
	}
	if again := read(); again.Input["key"] != "value" || !again.NextRunAt.Equal(wantNext) {
		t.Fatalf("stored schedule follows an earlier read: input=%v next=%v", again.Input, again.NextRunAt)
	}

	// 更新写入的 Input 之后被调用方改动，已存的值保持更新时的内容
	input := map[string]any{"key": "updated"}
	if err := first.Update(schedule.Changes{Input: &input}, time.Now().UTC().Truncate(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateSchedule(first); err != nil {
		t.Fatal(err)
	}
	input["key"] = "changed after update"
	if again := read(); again.Input["key"] != "updated" {
		t.Fatalf("stored schedule follows the updated entity: input=%v", again.Input)
	}
}

func mustList(t *testing.T, store schedule.Repository) []*schedule.Schedule {
	t.Helper()
	listed, err := store.ListSchedules("ns1")
	if err != nil {
		t.Fatal(err)
	}
	return listed
}
