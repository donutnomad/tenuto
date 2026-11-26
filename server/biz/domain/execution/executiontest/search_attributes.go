package executiontest

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// RunListSearchAttributeContract 断言按搜索属性过滤的语义：普通键按值全等，只有显式写成空串的键匹配空串，缺失的键不匹配；
// TemporalChangeVersion 按成员匹配；Run 启动后写入的属性（SearchAttributesUpserted）替换旧值参与过滤；多个属性是「与」。
func RunListSearchAttributeContract(t *testing.T, newStore func(t *testing.T) execution.ExecutionManager) {
	store := newStore(t)
	createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seed := []struct {
		id    string
		attrs map[string]string
	}{
		{"run-a", map[string]string{"env": "prod"}},
		{"run-b", map[string]string{"env": ""}},
		{"run-c", nil},
		{"run-d", map[string]string{"env": "prod", history.ChangeVersionSearchAttribute: history.EncodeChangeVersions([]string{"p1-1", "p2-2"})}},
		{"run-e", map[string]string{"env": "prod"}},
	}
	for i, item := range seed {
		createSearchableRun(t, store, item.id, createdAt.Add(time.Duration(i)*time.Second), item.attrs)
	}
	// run-e 启动后改成 dev：旧值不再匹配。
	if _, err := store.Append("run-e", "run-e", "run-e", history.NewSearchAttributesUpsertedEvent(map[string]any{"env": "dev", "team": "core"})); err != nil {
		t.Fatal(err)
	}

	for name, tc := range map[string]struct {
		filter map[string]string
		want   []string
	}{
		"value":                   {map[string]string{"env": "prod"}, []string{"run-a", "run-d"}},
		"explicit empty value":    {map[string]string{"env": ""}, []string{"run-b"}},
		"upserted value":          {map[string]string{"env": "dev"}, []string{"run-e"}},
		"change version member":   {map[string]string{history.ChangeVersionSearchAttribute: "p2-2"}, []string{"run-d"}},
		"change version and env":  {map[string]string{history.ChangeVersionSearchAttribute: "p1-1", "env": "prod"}, []string{"run-d"}},
		"two attributes":          {map[string]string{"env": "dev", "team": "core"}, []string{"run-e"}},
		"no run has both":         {map[string]string{"env": "prod", "team": "core"}, nil},
		"change version whole":    {map[string]string{history.ChangeVersionSearchAttribute: history.EncodeChangeVersions([]string{"p1-1", "p2-2"})}, nil},
		"unknown attribute value": {map[string]string{"env": "staging"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := execution.ListRuns(store, execution.RunListFilter{SearchAttributes: tc.filter, IncludeTerminal: true})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, item := range result.Items {
				got = append(got, item.RunID)
			}
			if !slices.Equal(got, tc.want) || result.Total != len(tc.want) || result.HasMore {
				t.Fatalf("filter %v: runs %v total %d has_more %v, want %v", tc.filter, got, result.Total, result.HasMore, tc.want)
			}
		})
	}
}

// RunListPageTokenContract 断言 Run 列表按 page token 翻页：每页从上一页的最后一行之后开始，不重复不丢失，
// Total 每页相同；token 只能用于签发它的过滤条件。
func RunListPageTokenContract(t *testing.T, newStore func(t *testing.T) execution.ExecutionManager) {
	store := newStore(t)
	createdAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var want []string
	for i := range 8 {
		id := fmt.Sprintf("run-%d", i)
		attrs := map[string]string{"env": "prod"}
		if i%3 == 0 {
			attrs["env"] = "dev"
		}
		// 两个 Run 共用一个 created_at：同一时刻的 Run 以 ID 决定先后。
		createSearchableRun(t, store, id, createdAt.Add(time.Duration(i/2)*time.Second), attrs)
		if attrs["env"] == "prod" {
			want = append(want, id)
		}
	}
	for _, desc := range []bool{false, true} {
		filter := execution.RunListFilter{SearchAttributes: map[string]string{"env": "prod"}, Limit: 2, OrderDesc: desc}
		var got []string
		for page := 0; ; page++ {
			result, err := execution.ListRuns(store, filter)
			if err != nil {
				t.Fatal(err)
			}
			if result.Total != len(want) {
				t.Fatalf("page %d total = %d, want %d", page, result.Total, len(want))
			}
			for _, item := range result.Items {
				got = append(got, item.RunID)
			}
			if !result.HasMore {
				if result.NextPageToken != "" {
					t.Fatalf("last page has next_page_token %q", result.NextPageToken)
				}
				break
			}
			filter.PageToken = result.NextPageToken
			if page == 0 {
				// 同一 token 换了过滤条件：拒绝。改页大小不影响。
				changed := filter
				changed.SearchAttributes = map[string]string{"env": "dev"}
				if _, err := execution.ListRuns(store, changed); !errors.Is(err, protocol.ErrInvalidRequest) {
					t.Fatalf("token reused with another filter: err = %v, want ErrInvalidRequest", err)
				}
				resized := filter
				resized.Limit = 50
				if _, err := execution.ListRuns(store, resized); err != nil {
					t.Fatalf("token reused with another page size: %v", err)
				}
			}
		}
		expected := slices.Clone(want)
		if desc {
			slices.Reverse(expected)
		}
		if !slices.Equal(got, expected) {
			t.Fatalf("desc=%v pages %v, want %v", desc, got, expected)
		}
	}
}

func createSearchableRun(t *testing.T, store execution.ExecutionManager, id string, createdAt time.Time, attrs map[string]string) {
	t.Helper()
	run := execution.NewRun(execution.NewRunOptions{
		ID: id, WorkflowID: "wf-" + id, WorkflowConfigVersion: "v1", CreatedAt: createdAt, SearchAttributes: attrs,
	})
	createRun(t, store, run)
	started := history.NewWorkflowStartedEvent(nil, id, "", "wf-"+id, "flow", 0)
	started.WorkflowStarted.SearchAttributes = attrs
	started.Timestamp = createdAt
	if _, err := store.Append(id, id, id, started); err != nil {
		t.Fatal(err)
	}
}
