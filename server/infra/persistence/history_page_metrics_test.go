package persistence

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// History 分页的事件数与字节数在首页按快照上界统计，写进分页令牌；后续页沿用令牌里的值，不再对可见范围做
// COUNT / SUM（每页回表读整段 History）。首页之后追加的事件不改变后续页的统计值。
func TestHistoryPageMetricsCarriedInToken(t *testing.T) {
	db := mysqltest.NewDB(t)
	var aggregates atomic.Int64
	countAggregates := func(tx *gorm.DB) {
		if sql := strings.ToUpper(tx.Statement.SQL.String()); strings.Contains(sql, "COUNT(") || strings.Contains(sql, "SUM(") {
			aggregates.Add(1)
		}
	}
	for name, err := range map[string]error{
		"query": db.Callback().Query().After("gorm:query").Register("history-page:query", countAggregates),
		"row":   db.Callback().Row().After("gorm:row").Register("history-page:row", countAggregates),
		"raw":   db.Callback().Raw().After("gorm:raw").Register("history-page:raw", countAggregates),
	} {
		if err != nil {
			t.Fatalf("register %s callback: %v", name, err)
		}
	}
	store, err := New(db, Config{HistoryPageTokenKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	const runID = "run-history-metrics"
	createTestRun(t, store, runID)
	appendSignal := func(name string) {
		t.Helper()
		if _, err := store.Executions.Append(runID, runID, runID, history.Event{Timestamp: time.Unix(1, 0), WorkflowSignaled: &history.WorkflowSignaledEvent{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		appendSignal(name)
	}

	first, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: runID, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.HistoryEventCount != 5 || first.HistorySizeBytes <= 0 || first.NextPageToken == "" {
		t.Fatalf("first page: count=%d size=%d token=%q", first.HistoryEventCount, first.HistorySizeBytes, first.NextPageToken)
	}
	if aggregates.Load() == 0 {
		t.Fatal("first page must compute the snapshot metrics")
	}
	appendSignal("after-snapshot")

	aggregates.Store(0)
	token, events := first.NextPageToken, len(first.Events)
	for token != "" {
		page, err := store.Executions.GetEventPage(execution.HistoryPageRequest{RunID: runID, PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		if page.HistoryEventCount != first.HistoryEventCount || page.HistorySizeBytes != first.HistorySizeBytes {
			t.Fatalf("later page metrics = (%d, %d), want the first page's (%d, %d)",
				page.HistoryEventCount, page.HistorySizeBytes, first.HistoryEventCount, first.HistorySizeBytes)
		}
		token, events = page.NextPageToken, events+len(page.Events)
	}
	if events != 5 {
		t.Fatalf("paged %d events, want the 5 in the snapshot", events)
	}
	if got := aggregates.Load(); got != 0 {
		t.Fatalf("later pages ran %d COUNT/SUM statements", got)
	}
}
