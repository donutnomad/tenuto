package engine_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

type historyReadCountingManager struct {
	*memory.ExecutionManager
	reads atomic.Int64
}

func (m *historyReadCountingManager) GetEvents(runID string) ([]history.Event, error) {
	m.reads.Add(1)
	return m.ExecutionManager.GetEvents(runID)
}

func TestListRunsProjectionDoesNotReadEventHistory(t *testing.T) {
	mgr := &historyReadCountingManager{ExecutionManager: memory.NewExecutionManager()}
	seedRuns(t, mgr.ExecutionManager, 50)
	runtime := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64)})

	result, err := runtime.View().ListRuns(execution.RunListFilter{
		Namespace:       protocol.DefaultNamespace,
		IncludeTerminal: true,
		Limit:           50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 50 {
		t.Fatalf("ListRuns returned %d items, want 50", len(result.Items))
	}
	if reads := mgr.reads.Load(); reads != 0 {
		t.Fatalf("ListRuns issued %d full History reads; execution projection must serve visibility", reads)
	}
}

func TestHistoryPaginationAllocationGrowthIsLinear(t *testing.T) {
	measure := func(eventCount int) float64 {
		mgr := memory.NewExecutionManager()
		runID := fmt.Sprintf("history-allocation-%d", eventCount)
		createRun(t, mgr, runID)
		seedHistory(t, mgr, runID, eventCount)
		return testing.AllocsPerRun(3, func() {
			request := execution.HistoryPageRequest{RunID: runID, PageSize: execution.DefaultHistoryPageSize, Direction: execution.HistoryDirectionAscending}
			seen := 0
			for {
				page, err := mgr.GetEventPage(request)
				if err != nil {
					panic(err)
				}
				seen += len(page.Events)
				if page.NextPageToken == "" {
					break
				}
				request.PageToken = page.NextPageToken
			}
			if seen != eventCount {
				panic(fmt.Sprintf("history iterator returned %d events, want %d", seen, eventCount))
			}
		})
	}
	small := measure(1_000)
	large := measure(10_000)
	if large > small*15 {
		t.Fatalf("pagination allocations grew superlinearly: 1k=%.0f 10k=%.0f", small, large)
	}
}

func BenchmarkHistoryPagination(b *testing.B) {
	for _, eventCount := range []int{5_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("events_%d", eventCount), func(b *testing.B) {
			mgr := memory.NewExecutionManager()
			runID := "history-benchmark"
			createRun(b, mgr, runID)
			seedHistory(b, mgr, runID, eventCount)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				request := execution.HistoryPageRequest{
					RunID:     runID,
					PageSize:  execution.DefaultHistoryPageSize,
					Direction: execution.HistoryDirectionAscending,
				}
				seen := 0
				for {
					page, err := mgr.GetEventPage(request)
					if err != nil {
						b.Fatal(err)
					}
					seen += len(page.Events)
					if page.NextPageToken == "" {
						break
					}
					request.PageToken = page.NextPageToken
				}
				if seen != eventCount {
					b.Fatalf("iterator returned %d events, want %d", seen, eventCount)
				}
			}
		})
	}
}

func BenchmarkListRunsProjection(b *testing.B) {
	for _, runCount := range []int{50, 500} {
		b.Run(fmt.Sprintf("runs_%d", runCount), func(b *testing.B) {
			mgr := memory.NewExecutionManager()
			seedRuns(b, mgr, runCount)
			runtime := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(runCount + 1), ActivityRouter: memory.NewActivityQueueRouter(1)})
			filter := execution.RunListFilter{
				Namespace:       protocol.DefaultNamespace,
				IncludeTerminal: true,
				Limit:           runCount,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := runtime.View().ListRuns(filter)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Items) != runCount {
					b.Fatalf("ListRuns returned %d items, want %d", len(result.Items), runCount)
				}
			}
		})
	}
}

type testOrBenchmark interface {
	Helper()
	Fatal(args ...any)
}

func createRun(tb testOrBenchmark, mgr *memory.ExecutionManager, runID string) {
	tb.Helper()
	err := mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(engine.WithRootBranch(&execution.Run{
			ID:           runID,
			Namespace:    protocol.DefaultNamespace,
			WorkflowID:   "history-scaling",
			WorkflowType: "history-scaling",
			Status:       history.StatusRunning,
			CreatedAt:    time.Unix(1, 0),
		}))
	})
	if err != nil {
		tb.Fatal(err)
	}
}

func seedHistory(tb testOrBenchmark, mgr *memory.ExecutionManager, runID string, count int) {
	tb.Helper()
	for i := range count {
		_, err := mgr.Append(runID, "tree", "branch", history.Event{
			Timestamp: time.Unix(int64(i+2), 0),
			WorkflowSignaled: &history.WorkflowSignaledEvent{
				Name:  "benchmark",
				Input: map[string]any{"sequence": int64(i)},
			},
		})
		if err != nil {
			tb.Fatal(err)
		}
	}
}

func seedRuns(tb testOrBenchmark, mgr *memory.ExecutionManager, count int) {
	tb.Helper()
	for i := range count {
		runID := fmt.Sprintf("run-%06d", i)
		createRun(tb, mgr, runID)
		seedHistory(tb, mgr, runID, 1)
	}
}
