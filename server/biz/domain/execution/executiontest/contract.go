// Package executiontest 提供 execution.ExecutionManager 的契约测试，各实现的测试用同一套断言运行。
package executiontest

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// weirdAttributeKey 含引号与反斜杠：搜索属性的键原样参与匹配，不拼进 SQL 或 JSON path。
const weirdAttributeKey = `we"ird\\`

// RunListOrderContract 断言 Run 列表按 (created_at, id) 排序，分页不重复也不丢失：
// created_at 相同的 Run 以 id 决定先后，升序与降序互为逆序。每个子测试拿到一个空仓储。
func RunListOrderContract(t *testing.T, newStore func(t *testing.T) execution.ExecutionManager) {
	for name, filter := range map[string]execution.StoreListFilter{
		"Plain": {},
		// 显式写成空串的键匹配空串过滤。
		"Attribute":     {SearchAttributes: map[string]string{weirdAttributeKey: ""}},
		"TwoAttributes": {SearchAttributes: map[string]string{weirdAttributeKey: "", "env": "prod"}},
	} {
		t.Run(name, func(t *testing.T) {
			store := newStore(t)
			want := seedRunsSharingCreatedAt(t, store)
			if got := listCursorPages(t, store, filter, false); !slices.Equal(got, want) {
				t.Fatalf("ascending cursor pages:\n got %v\nwant %v", got, want)
			}
			// CreatedBefore 取第三组的 created_at：只剩前两组。
			bounded := filter
			before := time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
			bounded.CreatedBefore = &before
			if got := listCursorPages(t, store, bounded, false); !slices.Equal(got, want[:16]) {
				t.Fatalf("cursor pages created before %s:\n got %v\nwant %v", before, got, want[:16])
			}
			slices.Reverse(want)
			if got := listCursorPages(t, store, filter, true); !slices.Equal(got, want) {
				t.Fatalf("descending cursor pages:\n got %v\nwant %v", got, want)
			}
		})
	}
}

// listCursorPages 以上一页最后一个 Run 作为 After 游标翻页，直到返回空页。
func listCursorPages(t *testing.T, store execution.Store, filter execution.StoreListFilter, desc bool) []string {
	t.Helper()
	filter.OrderDesc, filter.Limit = desc, 5
	var ids []string
	for {
		page, err := store.List(filter)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Runs) == 0 {
			return ids
		}
		for _, run := range page.Runs {
			ids = append(ids, run.ID)
		}
		cursor := execution.CursorOf(page.Runs[len(page.Runs)-1])
		filter.After = &cursor
	}
}

// seedRunsSharingCreatedAt 写入三组 created_at 各自相同的 Run，插入顺序与 id 顺序不一致，
// 返回按 (created_at, id) 升序排列的 Run ID。
func seedRunsSharingCreatedAt(t *testing.T, store execution.ExecutionManager) []string {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var want []string
	for group := range 3 {
		createdAt := base.Add(time.Duration(group) * time.Minute)
		for _, n := range []int{5, 2, 7, 0, 3, 6, 1, 4} {
			id := fmt.Sprintf("run-g%d-%d", group, n)
			run := execution.NewRun(execution.NewRunOptions{
				ID: id, WorkflowID: "wf-" + id, WorkflowConfigVersion: "v1", CreatedAt: createdAt,
				SearchAttributes: map[string]string{weirdAttributeKey: "", "env": "prod"},
			})
			createRun(t, store, run)
		}
		for n := range 8 {
			want = append(want, fmt.Sprintf("run-g%d-%d", group, n))
		}
	}
	return want
}

// RunListVisibilityContract 断言列表的两条可见性规则：显式给出 RunIDs 时已结束的 Run 也在结果里，
// 结束的 Run 带结束时间（取自终态事件），未结束的没有。
func RunListVisibilityContract(t *testing.T, newStore func(t *testing.T) execution.ExecutionManager) {
	store := newStore(t)
	createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"run-open", "run-closed"} {
		run := execution.NewRun(execution.NewRunOptions{ID: id, WorkflowID: "wf-" + id, WorkflowConfigVersion: "v1", CreatedAt: createdAt})
		createRun(t, store, run)
		started := history.NewWorkflowStartedEvent(nil, id, "", "wf-"+id, "flow", 0)
		started.Timestamp = createdAt
		if _, err := store.Append(id, id, id, started); err != nil {
			t.Fatal(err)
		}
	}
	closedAt := createdAt.Add(time.Minute)
	completed := history.NewWorkflowCompletedEvent(nil)
	completed.Timestamp = closedAt
	closedRun, err := store.Get("run-closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx execution.ExecutionTx) error {
		if _, err := tx.Append("run-closed", "run-closed", "run-closed", completed); err != nil {
			return err
		}
		if err := closedRun.MarkTerminal(history.StatusCompleted, nil); err != nil {
			return err
		}
		return tx.UpdateRun(closedRun)
	}); err != nil {
		t.Fatal(err)
	}

	result, err := execution.ListRuns(store, execution.RunListFilter{RunIDs: []string{"run-open", "run-closed"}})
	if err != nil {
		t.Fatal(err)
	}
	items := map[string]execution.RunListItem{}
	for _, item := range result.Items {
		items[item.RunID] = item
	}
	closed, ok := items["run-closed"]
	if !ok || len(items) != 2 {
		t.Fatalf("explicit run ids must include the closed run: %+v", result.Items)
	}
	if closed.ClosedAt == nil || !closed.ClosedAt.Equal(closedAt) {
		t.Fatalf("closed run closed_at = %v, want %v", closed.ClosedAt, closedAt)
	}
	if items["run-open"].ClosedAt != nil {
		t.Fatalf("open run has closed_at %v", items["run-open"].ClosedAt)
	}
}
