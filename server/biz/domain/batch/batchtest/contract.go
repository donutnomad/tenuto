// Package batchtest 提供 batch.Repository 的契约测试，各实现的测试用同一套断言运行。
package batchtest

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RunRepositoryContract 对 newRepo 返回的实现运行仓储契约；每个子测试拿到一个空仓储。
func RunRepositoryContract(t *testing.T, newRepo func(t *testing.T) batch.Repository) {
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, newRepo(t)) })
	t.Run("Missing", func(t *testing.T) { testMissing(t, newRepo(t)) })
	t.Run("CreateDuplicate", func(t *testing.T) { testCreateDuplicate(t, newRepo(t)) })
	t.Run("UpdatePersistsProgressAndCursor", func(t *testing.T) { testUpdatePersistsProgress(t, newRepo(t)) })
	t.Run("UpdateWritesOnlyChangedFields", func(t *testing.T) { testUpdateWritesOnlyChangedFields(t, newRepo(t)) })
	t.Run("StaleUpdateIsRejected", func(t *testing.T) { testStaleUpdate(t, newRepo(t)) })
	t.Run("ListOrderAndPaging", func(t *testing.T) { testListOrderAndPaging(t, newRepo(t)) })
	t.Run("ListActive", func(t *testing.T) { testListActive(t, newRepo(t)) })
	t.Run("ReadsAreIsolatedFromCallers", func(t *testing.T) { testReadsAreIsolated(t, newRepo(t)) })
}

var base = time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)

func newOperation(t *testing.T, id, namespace string, createdAt time.Time) *batch.Operation {
	t.Helper()
	op, err := batch.New(id, batch.NewParams{
		Namespace: namespace,
		Type:      batch.TypeEnums.Reset,
		ResetType: execution.ResetTypeEnums.LastWorkflowTask,
		Reason:    "contract",
		Filter: execution.RunListFilter{
			WorkflowID:       "wf-1",
			Statuses:         []history.Status{history.StatusFailed},
			SearchAttributes: map[string]string{"team": "payments"},
		},
	}, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func mustCreate(t *testing.T, repo batch.Repository, op *batch.Operation) {
	t.Helper()
	if err := repo.CreateOperation(op); err != nil {
		t.Fatal(err)
	}
}

func mustGet(t *testing.T, repo batch.Repository, namespace, id string) *batch.Operation {
	t.Helper()
	op, err := repo.GetOperation(namespace, id)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func mustUpdate(t *testing.T, repo batch.Repository, op *batch.Operation) {
	t.Helper()
	if err := repo.UpdateOperation(op); err != nil {
		t.Fatal(err)
	}
}

func equalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func testRoundTrip(t *testing.T, repo batch.Repository) {
	// Happy Path: 创建后读回的字段与写入一致，空 namespace 归一为 default
	op := newOperation(t, "op-1", "", base)
	mustCreate(t, repo, op)
	if op.ExportPatch().Version.IsPresent() {
		t.Fatal("CreateOperation must clear pending changes")
	}
	got := mustGet(t, repo, "default", "op-1")
	if got.ID != "op-1" || got.Namespace != "default" || got.Type != batch.TypeEnums.Reset || got.ResetType != execution.ResetTypeEnums.LastWorkflowTask ||
		got.Reason != "contract" || got.Status != batch.StatusEnums.Pending || got.Version != op.Version || !got.CreatedAt.Equal(base) ||
		!got.UpdatedAt.Equal(base) || got.Cursor != nil || got.StartedAt != nil || got.FinishedAt != nil || len(got.Errors) != 0 {
		t.Fatalf("round trip = %+v", got)
	}
	if got.Filter.Namespace != "default" || got.Filter.WorkflowID != "wf-1" || !slices.Equal(got.Filter.Statuses, []history.Status{history.StatusFailed}) ||
		got.Filter.SearchAttributes["team"] != "payments" {
		t.Fatalf("round trip filter = %+v", got.Filter)
	}
	if again := mustGet(t, repo, "", "op-1"); again.ID != "op-1" {
		t.Fatalf("empty namespace must address default: %+v", again)
	}
}

func testMissing(t *testing.T, repo batch.Repository) {
	// Error Case: 读取与更新不存在的批量操作返回 ErrNotFound
	if _, err := repo.GetOperation("default", "missing"); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("GetOperation: err = %v, want ErrNotFound", err)
	}
	op := newOperation(t, "missing", "default", base)
	if err := op.Begin(base); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateOperation(op); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("UpdateOperation: err = %v, want ErrNotFound", err)
	}
}

func testCreateDuplicate(t *testing.T, repo batch.Repository) {
	// Error Case: 同一 namespace 下重复 ID 返回 ErrAlreadyExists；不同 namespace 互不影响
	mustCreate(t, repo, newOperation(t, "op-1", "ns-a", base))
	if err := repo.CreateOperation(newOperation(t, "op-1", "ns-a", base)); !errors.Is(err, batch.ErrAlreadyExists) {
		t.Fatalf("duplicate create: err = %v, want ErrAlreadyExists", err)
	}
	mustCreate(t, repo, newOperation(t, "op-1", "ns-b", base))
}

func testUpdatePersistsProgress(t *testing.T, repo batch.Repository) {
	// Happy Path: 认领、记录两页、结束，每次保存后读回的状态、计数、错误样本与游标都与内存一致
	mustCreate(t, repo, newOperation(t, "op-1", "default", base))
	op := mustGet(t, repo, "default", "op-1")
	started := base.Add(time.Second)
	if err := op.Begin(started); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, repo, op)
	got := mustGet(t, repo, "default", "op-1")
	if got.Status != batch.StatusEnums.Running || !equalTime(got.StartedAt, &started) || got.Version != op.Version {
		t.Fatalf("after Begin = %+v", got)
	}

	cursor := execution.RunCursor{CreatedAt: base.Add(-time.Hour), RunID: "run-0099"}
	if err := got.RecordPage(batch.Page{
		Total: 250, Succeeded: 97, Failed: 2, Skipped: 1, Cursor: cursor,
		Errors: []batch.RunError{{RunID: "run-0007", Message: "boom"}, {RunID: "run-0042", Message: "bang"}},
	}, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, repo, got)
	reread := mustGet(t, repo, "default", "op-1")
	if reread.TotalCount != 250 || reread.SucceededCount != 97 || reread.FailedCount != 2 || reread.SkippedCount != 1 ||
		len(reread.Errors) != 2 || reread.Errors[1] != (batch.RunError{RunID: "run-0042", Message: "bang"}) {
		t.Fatalf("after first page = %+v", reread)
	}
	if reread.Cursor == nil || reread.Cursor.RunID != cursor.RunID || !reread.Cursor.CreatedAt.Equal(cursor.CreatedAt) {
		t.Fatalf("cursor = %+v, want %+v", reread.Cursor, cursor)
	}

	finished := base.Add(3 * time.Second)
	if err := reread.Finish(finished); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, repo, reread)
	done := mustGet(t, repo, "default", "op-1")
	if done.Status != batch.StatusEnums.Completed || done.TotalCount != 100 || !equalTime(done.FinishedAt, &finished) ||
		!done.UpdatedAt.Equal(finished) || done.Version != reread.Version {
		t.Fatalf("after Finish = %+v", done)
	}
}

func testUpdateWritesOnlyChangedFields(t *testing.T, repo batch.Repository) {
	// Integration: 直接改公开字段没有经过领域方法，不属于待保存的变更；没有变更的 Update 不写入
	mustCreate(t, repo, newOperation(t, "op-1", "default", base))
	op := mustGet(t, repo, "default", "op-1")
	op.Reason = "untracked"
	op.SucceededCount = 99
	mustUpdate(t, repo, op)
	if err := op.Begin(base); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, repo, op)
	got := mustGet(t, repo, "default", "op-1")
	if got.Reason != "contract" || got.SucceededCount != 0 || got.Status != batch.StatusEnums.Running {
		t.Fatalf("stored = %+v: only Begin's fields may be written", got)
	}
}

func testStaleUpdate(t *testing.T, repo batch.Repository) {
	// Error Case: 基于过期读取的保存返回 ErrConcurrentUpdate，存储保持先保存的一方
	mustCreate(t, repo, newOperation(t, "op-1", "default", base))
	driver := mustGet(t, repo, "default", "op-1")
	canceller := mustGet(t, repo, "default", "op-1")
	if err := canceller.Cancel(base); err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, repo, canceller)
	if err := driver.Begin(base); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateOperation(driver); !errors.Is(err, batch.ErrConcurrentUpdate) {
		t.Fatalf("stale update: err = %v, want ErrConcurrentUpdate", err)
	}
	if got := mustGet(t, repo, "default", "op-1"); got.Status != batch.StatusEnums.Cancelled {
		t.Fatalf("status = %s, want the cancel that was saved first", got.Status)
	}
}

func testListOrderAndPaging(t *testing.T, repo batch.Repository) {
	// Happy Path: 按 (CreatedAt, ID) 降序分页，Total 不受分页影响，只含指定 namespace
	var want []string
	for i := range 5 {
		// op-3 与 op-4 同一时刻创建，按 ID 决定先后。
		createdAt := base.Add(time.Duration(min(i, 3)) * time.Minute)
		id := fmt.Sprintf("op-%d", i)
		mustCreate(t, repo, newOperation(t, id, "", createdAt))
		want = append([]string{id}, want...)
	}
	mustCreate(t, repo, newOperation(t, "other", "ns-b", base.Add(time.Hour)))

	var got []string
	for offset := 0; ; offset += 2 {
		page, total, err := repo.ListOperations(batch.ListFilter{Namespace: "default", Offset: offset, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if total != 5 {
			t.Fatalf("total = %d, want 5", total)
		}
		if len(page) == 0 {
			break
		}
		for _, op := range page {
			got = append(got, op.ID)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("pages = %v, want %v", got, want)
	}
	all, total, err := repo.ListOperations(batch.ListFilter{})
	if err != nil || total != 5 || len(all) != 5 {
		t.Fatalf("default limit: %d items, total %d, err %v", len(all), total, err)
	}
}

func testListActive(t *testing.T, repo batch.Repository) {
	// Happy Path: 只返回 pending 与 running，跨 namespace 按 (CreatedAt, ID) 升序
	statuses := map[string]func(*batch.Operation) error{
		"pending":   func(*batch.Operation) error { return nil },
		"running":   func(o *batch.Operation) error { return o.Begin(base) },
		"cancelled": func(o *batch.Operation) error { return o.Cancel(base) },
		"completed": func(o *batch.Operation) error {
			if err := o.Begin(base); err != nil {
				return err
			}
			return o.Finish(base)
		},
	}
	for i, name := range []string{"running", "completed", "pending", "cancelled"} {
		op := newOperation(t, "op-"+name, fmt.Sprintf("ns-%d", i), base.Add(time.Duration(i)*time.Minute))
		mustCreate(t, repo, op)
		if err := statuses[name](op); err != nil {
			t.Fatal(err)
		}
		mustUpdate(t, repo, op)
	}
	active, err := repo.ListActiveOperations()
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, op := range active {
		ids = append(ids, op.ID)
	}
	if !slices.Equal(ids, []string{"op-running", "op-pending"}) {
		t.Fatalf("active = %v, want [op-running op-pending]", ids)
	}
}

func testReadsAreIsolated(t *testing.T, repo batch.Repository) {
	// Edge Case: 调用方改动读到的实体不影响仓储里的记录
	mustCreate(t, repo, newOperation(t, "op-1", "default", base))
	got := mustGet(t, repo, "default", "op-1")
	got.Filter.Statuses[0] = history.StatusRunning
	got.Filter.SearchAttributes["team"] = "other"
	again := mustGet(t, repo, "default", "op-1")
	if again.Filter.Statuses[0] != history.StatusFailed || again.Filter.SearchAttributes["team"] != "payments" {
		t.Fatalf("stored filter changed through a read copy: %+v", again.Filter)
	}
}
