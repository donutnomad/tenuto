package persistence

import (
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// listFilterStore 是 ListRuns 过滤下推用例对两个后端的共同依赖。
type listFilterStore interface {
	execution.VisibilityStore
	RunInTx(fn func(tx execution.ExecutionTx) error) error
	Append(runID, treeID, branchID string, event history.Event) (int64, error)
}

// seedListFilterRuns 写入 7 个按 CreatedAt 递增的 Run：
//
//	r1 tenant=a
//	r2 tenant=b
//	r3 tenant=a  blocked
//	r4 tenant=a
//	r5 tenant=a  TemporalChangeVersion=["fix-1"]
//	r6 tenant=b  blocked
//	r7 tenant=b  TemporalChangeVersion=["other-2","fix-1"]  we"ird=v
func seedListFilterRuns(t *testing.T, store listFilterStore) {
	t.Helper()
	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	seeds := []struct {
		id      string
		attrs   map[string]string
		blocked bool
	}{
		{id: "r1", attrs: map[string]string{"tenant": "a"}},
		{id: "r2", attrs: map[string]string{"tenant": "b"}},
		{id: "r3", attrs: map[string]string{"tenant": "a"}, blocked: true},
		{id: "r4", attrs: map[string]string{"tenant": "a"}},
		{id: "r5", attrs: map[string]string{"tenant": "a", history.ChangeVersionSearchAttribute: history.EncodeChangeVersions([]string{"fix-1"})}},
		{id: "r6", attrs: map[string]string{"tenant": "b"}, blocked: true},
		{id: "r7", attrs: map[string]string{"tenant": "b", `we"ird`: "v", history.ChangeVersionSearchAttribute: history.EncodeChangeVersions([]string{"other-2", "fix-1"})}},
	}
	for i, seed := range seeds {
		if err := store.RunInTx(func(tx execution.ExecutionTx) error {
			return tx.CreateRun(rootBranch(t, &execution.Run{
				ID: seed.id, WorkflowID: "wf-" + seed.id, WorkflowConfigVersion: "v1",
				Status: history.StatusRunning, SearchAttributes: seed.attrs,
				CreatedAt: base.Add(time.Duration(i) * time.Second),
			}))
		}); err != nil {
			t.Fatal(err)
		}
		if seed.blocked {
			failed := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{Cause: "worker_error", Reason: "boom"})
			if _, err := store.Append(seed.id, seed.id, seed.id, failed); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Integration: 搜索属性、blocked、RunIDs 过滤在两个后端都先于分页生效，Total 是过滤后的总数。
func TestListRunsFiltersBeforePagingOnBothBackends(t *testing.T) {
	backends := map[string]func(t *testing.T) listFilterStore{
		"gormstore": func(t *testing.T) listFilterStore { return newTestStore(t).Executions },
		"memory":    func(t *testing.T) listFilterStore { return memory.NewExecutionManager() },
	}
	changeVersion := history.ChangeVersionSearchAttribute
	cases := []struct {
		name      string
		filter    execution.RunListFilter
		wantIDs   []string
		wantTotal int
		// pages 是先按 next_page_token 翻过的页数。
		pages int
	}{
		{"attribute first page", execution.RunListFilter{SearchAttributes: map[string]string{"tenant": "a"}, Limit: 2}, []string{"r1", "r3"}, 4, 0},
		{"attribute second page", execution.RunListFilter{SearchAttributes: map[string]string{"tenant": "a"}, Limit: 2}, []string{"r4", "r5"}, 4, 1},
		{"attribute descending", execution.RunListFilter{SearchAttributes: map[string]string{"tenant": "a"}, Limit: 2, OrderDesc: true}, []string{"r5", "r4"}, 4, 0},
		{"attribute value is case sensitive", execution.RunListFilter{SearchAttributes: map[string]string{"tenant": "A"}}, []string{}, 0, 0},
		{"empty value does not match a missing key", execution.RunListFilter{SearchAttributes: map[string]string{"absent": ""}, Limit: 3}, []string{}, 0, 0},
		{"blocked first page", execution.RunListFilter{BlockedOnly: true, Limit: 1}, []string{"r3"}, 2, 0},
		{"blocked second page", execution.RunListFilter{BlockedOnly: true, Limit: 1}, []string{"r6"}, 2, 1},
		{"blocked and attribute", execution.RunListFilter{BlockedOnly: true, SearchAttributes: map[string]string{"tenant": "b"}}, []string{"r6"}, 1, 0},
		{"change version membership first page", execution.RunListFilter{SearchAttributes: map[string]string{changeVersion: "fix-1"}, Limit: 1}, []string{"r5"}, 2, 0},
		{"change version membership second page", execution.RunListFilter{SearchAttributes: map[string]string{changeVersion: "fix-1"}, Limit: 1}, []string{"r7"}, 2, 1},
		{"change version with attribute", execution.RunListFilter{SearchAttributes: map[string]string{changeVersion: "fix-1", "tenant": "a"}}, []string{"r5"}, 1, 0},
		{"change version miss", execution.RunListFilter{SearchAttributes: map[string]string{changeVersion: "fix"}}, []string{}, 0, 0},
		{"key with a quote", execution.RunListFilter{SearchAttributes: map[string]string{`we"ird`: "v"}}, []string{"r7"}, 1, 0},
		{"run ids", execution.RunListFilter{RunIDs: []string{"r6", "r2", "missing"}}, []string{"r2", "r6"}, 2, 0},
		{"run ids with attribute", execution.RunListFilter{RunIDs: []string{"r1", "r2"}, SearchAttributes: map[string]string{"tenant": "b"}}, []string{"r2"}, 1, 0},
	}
	for backendName, newStore := range backends {
		t.Run(backendName, func(t *testing.T) {
			store := newStore(t)
			seedListFilterRuns(t, store)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					filter := tc.filter
					for range tc.pages {
						page, err := execution.ListRuns(store, filter)
						if err != nil {
							t.Fatal(err)
						}
						filter.PageToken = page.NextPageToken
					}
					result, err := execution.ListRuns(store, filter)
					if err != nil {
						t.Fatal(err)
					}
					gotIDs := make([]string, 0, len(result.Items))
					for _, item := range result.Items {
						gotIDs = append(gotIDs, item.RunID)
					}
					if !slices.Equal(gotIDs, tc.wantIDs) || result.Total != tc.wantTotal {
						t.Fatalf("items = %v total = %d, want %v total = %d", gotIDs, result.Total, tc.wantIDs, tc.wantTotal)
					}
				})
			}
		})
	}
}
