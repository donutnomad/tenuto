package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// History 写到普通事件的上限（硬上限减去关闭预留）之后，非关闭路径（Signal、Update、WorkflowTaskScheduled）的写入
// 超限：Run 用关闭预留以 WorkflowFailed(HistoryLimitExceeded) 关闭，失败信息写明指标、预计值与上限，不停在 running。
func TestHistoryLimitClosesRunOnNonCloseWrites(t *testing.T) {
	const hardEvents, reservedEvents = 12, 2
	for _, tc := range []struct {
		name string
		// closedTask 为 true 时先关闭第一个 Workflow Task，并给后续写入留一个事件的余量。
		closedTask bool
		write      func(ctx context.Context, server *engine.Server, runID string) error
	}{
		{"signal", false, func(ctx context.Context, server *engine.Server, runID string) error {
			_, err := server.SignalWorkflow(ctx, "", "", runID, "tick", "", nil)
			return err
		}},
		{"update", false, func(ctx context.Context, server *engine.Server, runID string) error {
			_, err := server.UpdateWorkflow(ctx, "", "", runID, "add", "u-1", protocol.UpdateWaitStageEnums.Accepted, nil)
			return err
		}},
		// 没有 open 的 Workflow Task：Signal 恰好写满普通事件的上限，随后的 WorkflowTaskScheduled 超限。
		{"workflow_task_scheduled", true, func(ctx context.Context, server *engine.Server, runID string) error {
			_, err := server.SignalWorkflow(ctx, "", "", runID, "tick", "", nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mgr, err := memory.NewExecutionManagerWithHistoryLimits(history.HistoryLimits{
				WarningEvents: 4, WarningBytes: 1 << 20,
				HardEvents: hardEvents, HardBytes: 2 << 20,
				ReservedCloseEvents: reservedEvents, ReservedCloseBytes: 1024,
			})
			if err != nil {
				t.Fatal(err)
			}
			server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: mgr}, WorkflowQueue: memory.NewTaskQueue(8), ActivityRouter: memory.NewActivityQueueRouter(8)})
			t.Cleanup(server.Stop)
			ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{Type: "history-limit-" + tc.name, Program: programtest.Noop()})
			if err != nil {
				t.Fatal(err)
			}
			runID, err := startedRunID(server.StartWorkflow(ctx, "", ref.WorkflowType, ref.WorkflowType, "", nil))
			if err != nil {
				t.Fatal(err)
			}
			fillTo := hardEvents - reservedEvents
			if tc.closedTask {
				if err := server.HandleWorkflowTaskCompleted(server.WorkflowTaskContext(ctx, runID, 2), runID, 2, nil, "worker"); err != nil {
					t.Fatal(err)
				}
				fillTo--
			}
			// 用 Signal 填到 fillTo 个事件。
			for {
				events, err := mgr.GetEvents(runID)
				if err != nil {
					t.Fatal(err)
				}
				if len(events) >= fillTo {
					break
				}
				if _, err := mgr.Append(runID, runID, runID, history.NewWorkflowSignaledEvent("fill", nil)); err != nil {
					t.Fatal(err)
				}
			}

			err = tc.write(ctx, server, runID)
			if !tc.closedTask {
				var limitErr *history.HistoryLimitExceededError
				if !errors.As(err, &limitErr) {
					t.Fatalf("write err = %v, want HistoryLimitExceededError", err)
				}
			}
			status, err := server.GetRunStatus(runID)
			if err != nil || status != history.StatusFailed {
				t.Fatalf("status = %s, err = %v, want failed", status, err)
			}
			events, err := mgr.GetEvents(runID)
			if err != nil {
				t.Fatal(err)
			}
			last := events[len(events)-1]
			if last.WorkflowFailed == nil || last.WorkflowFailed.Failure == nil || last.WorkflowFailed.Failure.Type != "HistoryLimitExceeded" ||
				!strings.Contains(last.WorkflowFailed.Failure.Message, "history_event_count projected=11 limit=10") {
				t.Fatalf("last event = %+v, want WorkflowFailed(HistoryLimitExceeded) naming the metric", last)
			}
			if len(events) > hardEvents {
				t.Fatalf("history has %d events, over the hard limit %d", len(events), hardEvents)
			}
		})
	}
}
