package batch_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// 批量操作领域测试
// ============================================
// 测试策略:
// - 状态机：每对 (from, to) 的迁移按表判定，非法迁移返回 ErrInvalidTransition 且不改字段
// - New：类型与 reset_type 校验、namespace 归一、分页参数清零、显式 RunIDs 包含已结束的 Run
// - RecordPage：首页定总数、错误样本有上限、取消后仍可记账；Finish 的 completed/failed 判定；Version 每次保存只加一
// ============================================

var t0 = time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)

func newOperation(t *testing.T, typ batch.Type) *batch.Operation {
	t.Helper()
	op, err := batch.New("op-1", batch.NewParams{Type: typ, ResetType: execution.ResetTypeEnums.FirstWorkflowTask}, t0)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

// opIn 返回处于 status 的批量操作。
func opIn(t *testing.T, status batch.Status) *batch.Operation {
	t.Helper()
	op := newOperation(t, batch.TypeEnums.Terminate)
	steps := map[batch.Status][]func(*batch.Operation) error{
		batch.StatusEnums.Pending:   nil,
		batch.StatusEnums.Running:   {func(o *batch.Operation) error { return o.Begin(t0) }},
		batch.StatusEnums.Completed: {func(o *batch.Operation) error { return o.Begin(t0) }, func(o *batch.Operation) error { return o.Finish(t0) }},
		batch.StatusEnums.Failed: {
			func(o *batch.Operation) error { return o.Begin(t0) },
			func(o *batch.Operation) error { return o.RecordPage(batch.Page{Failed: 1}, t0) },
			func(o *batch.Operation) error { return o.Finish(t0) },
		},
		batch.StatusEnums.Cancelled: {func(o *batch.Operation) error { return o.Cancel(t0) }},
	}
	for _, step := range steps[status] {
		if err := step(op); err != nil {
			t.Fatal(err)
		}
	}
	if op.Status != status {
		t.Fatalf("setup reached %s, want %s", op.Status, status)
	}
	op.ClearPatch()
	return op
}

func TestStatusMachine(t *testing.T) {
	s := batch.StatusEnums
	all := []batch.Status{s.Pending, s.Running, s.Completed, s.Failed, s.Cancelled}
	allowed := map[[2]batch.Status]bool{
		{s.Pending, s.Running}: true, {s.Pending, s.Cancelled}: true,
		{s.Running, s.Completed}: true, {s.Running, s.Failed}: true, {s.Running, s.Cancelled}: true,
	}
	for _, from := range all {
		for _, to := range all {
			if got := batch.CanTransition(from, to); got != allowed[[2]batch.Status{from, to}] {
				t.Errorf("CanTransition(%s, %s) = %v", from, to, got)
			}
		}
		if got, want := batch.IsTerminal(from), from == s.Completed || from == s.Failed || from == s.Cancelled; got != want {
			t.Errorf("IsTerminal(%s) = %v, want %v", from, got, want)
		}
	}
}

func TestTransitionMethods(t *testing.T) {
	s := batch.StatusEnums
	methods := map[string]struct {
		call func(*batch.Operation) error
		to   batch.Status
	}{
		"Begin":  {func(o *batch.Operation) error { return o.Begin(t0) }, s.Running},
		"Finish": {func(o *batch.Operation) error { return o.Finish(t0) }, s.Completed},
		"Cancel": {func(o *batch.Operation) error { return o.Cancel(t0) }, s.Cancelled},
	}
	for name, method := range methods {
		for _, from := range []batch.Status{s.Pending, s.Running, s.Completed, s.Failed, s.Cancelled} {
			t.Run(fmt.Sprintf("%s from %s", name, from), func(t *testing.T) {
				op := opIn(t, from)
				err := method.call(op)
				switch {
				case name == "Cancel" && from == s.Cancelled:
					// 重复取消不变。
					if err != nil || op.Status != s.Cancelled {
						t.Fatalf("err = %v, status = %s", err, op.Status)
					}
				case batch.CanTransition(from, method.to):
					if err != nil || op.Status != method.to {
						t.Fatalf("err = %v, status = %s, want %s", err, op.Status, method.to)
					}
				default:
					if !errors.Is(err, batch.ErrInvalidTransition) || op.Status != from || op.ExportPatch().Status.IsPresent() {
						t.Fatalf("err = %v, status = %s: an illegal transition must fail and change nothing", err, op.Status)
					}
				}
			})
		}
	}
}

func TestNewValidatesAndNormalizes(t *testing.T) {
	cases := []struct {
		name   string
		params batch.NewParams
		ok     bool
	}{
		{"cancel", batch.NewParams{Type: batch.TypeEnums.Cancel}, true},
		{"terminate", batch.NewParams{Type: batch.TypeEnums.Terminate}, true},
		{"reset with type", batch.NewParams{Type: batch.TypeEnums.Reset, ResetType: execution.ResetTypeEnums.LastWorkflowTask}, true},
		{"reset without type", batch.NewParams{Type: batch.TypeEnums.Reset}, false},
		{"unknown type", batch.NewParams{Type: "pause"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := batch.New("op", tc.params, t0)
			if tc.ok != (err == nil) || err != nil && !errors.Is(err, protocol.ErrInvalidRequest) {
				t.Fatalf("err = %v, want ok=%v or an invalid request", err, tc.ok)
			}
		})
	}

	op, err := batch.New("op", batch.NewParams{
		Type:   batch.TypeEnums.Cancel,
		Filter: execution.RunListFilter{RunIDs: []string{"r1"}, PageToken: "token", Limit: 5, OrderDesc: true},
	}, t0)
	if err != nil {
		t.Fatal(err)
	}
	f := op.Filter
	if op.Namespace != "default" || f.Namespace != "default" || f.PageToken != "" || f.Limit != 0 || f.OrderDesc || !f.IncludeTerminal {
		t.Fatalf("operation = %+v, filter = %+v", op, f)
	}
	if op.Status != batch.StatusEnums.Pending || !op.CreatedAt.Equal(t0) || !op.SelectedBefore().Equal(t0.Add(time.Millisecond)) {
		t.Fatalf("operation = %+v", op)
	}
}

func TestRecordPageAndFinish(t *testing.T) {
	op := opIn(t, batch.StatusEnums.Running)
	var many []batch.RunError
	for i := range batch.MaxErrorSamples + 5 {
		many = append(many, batch.RunError{RunID: fmt.Sprintf("r%d", i), Message: "boom"})
	}
	first := execution.RunCursor{CreatedAt: t0, RunID: "r9"}
	if err := op.RecordPage(batch.Page{Total: 40, Succeeded: 3, Failed: 25, Skipped: 2, Errors: many, Cursor: first}, t0); err != nil {
		t.Fatal(err)
	}
	// 后续页的 Total 不覆盖首页的总数。
	second := execution.RunCursor{CreatedAt: t0.Add(time.Second), RunID: "r1"}
	if err := op.RecordPage(batch.Page{Total: 10, Succeeded: 1, Errors: many[:1], Cursor: second}, t0); err != nil {
		t.Fatal(err)
	}
	if op.TotalCount != 40 || op.SucceededCount != 4 || op.FailedCount != 25 || op.SkippedCount != 2 || op.Processed() != 31 {
		t.Fatalf("counters = %+v", op)
	}
	if len(op.Errors) != batch.MaxErrorSamples || *op.Cursor != second {
		t.Fatalf("errors = %d, cursor = %+v", len(op.Errors), op.Cursor)
	}
	// Begin 保存后 Version 是 2；之后两页一起保存只加一。
	if op.Version != 3 || op.ExpectedVersion() != 2 {
		t.Fatalf("two pages in one save: version = %d expected = %d, want 3 and 2", op.Version, op.ExpectedVersion())
	}
	if err := op.Finish(t0); err != nil {
		t.Fatal(err)
	}
	if op.Status != batch.StatusEnums.Completed || op.TotalCount != 31 || op.FinishedAt == nil {
		t.Fatalf("finished = %+v, want completed with total = processed", op)
	}

	// 取消在一页处理期间落库：这一页仍计入。
	cancelled := opIn(t, batch.StatusEnums.Cancelled)
	if err := cancelled.RecordPage(batch.Page{Total: 1, Succeeded: 1, Cursor: first}, t0); err != nil || cancelled.SucceededCount != 1 {
		t.Fatalf("record on cancelled: err = %v, op = %+v", err, cancelled)
	}
	for _, status := range []batch.Status{batch.StatusEnums.Pending, batch.StatusEnums.Completed, batch.StatusEnums.Failed} {
		if err := opIn(t, status).RecordPage(batch.Page{Cursor: first}, t0); !errors.Is(err, batch.ErrInvalidTransition) {
			t.Errorf("RecordPage on %s: err = %v, want ErrInvalidTransition", status, err)
		}
	}
}

func TestFinishStatus(t *testing.T) {
	cases := []struct {
		page batch.Page
		want batch.Status
	}{
		{batch.Page{}, batch.StatusEnums.Completed},
		{batch.Page{Succeeded: 1, Failed: 3}, batch.StatusEnums.Completed},
		{batch.Page{Skipped: 2}, batch.StatusEnums.Completed},
		{batch.Page{Failed: 2, Skipped: 1}, batch.StatusEnums.Failed},
	}
	for _, tc := range cases {
		op := opIn(t, batch.StatusEnums.Running)
		if err := op.RecordPage(tc.page, t0); err != nil {
			t.Fatal(err)
		}
		if err := op.Finish(t0); err != nil || op.Status != tc.want {
			t.Errorf("page %+v: status = %s err = %v, want %s", tc.page, op.Status, err, tc.want)
		}
	}
}
