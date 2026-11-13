package interpreter

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const executionSessionSequentialActivities = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var first map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &first); err != nil {
		return err
	}
	var second map[string]any
	return workflow.ExecuteActivity(ctx, activities.B, first).Get(ctx, &second)
}
`

func TestExecutionSessionRetainsParkedWorkflowAcrossActivations(t *testing.T) {
	nodes := compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go")
	session := NewExecutionSession(context.Background(), nodes, 7)
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Fatalf("close session: %v", err)
		}
	})

	restored, err := session.Restore(context.Background(), newPagedSessionHistoryIterator(1, []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{"request": "one"}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(3, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
	}))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.AppliedThroughEventID != 3 {
		t.Fatalf("restored cursor = %d, want 3", restored.AppliedThroughEventID)
	}

	first, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "activation-1",
		ExecutionGeneration: 7,
		Events: []history.Event{
			executionSessionEvent(4, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"first": "done"}})),
			executionSessionEvent(5, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 5,
	})
	if err != nil {
		t.Fatalf("activate first delta: %v", err)
	}
	assertSingleSessionCommand(t, first.Commands, history.CmdScheduleActivity, "#a2")
	if first.AppliedThroughEventID != 3 || session.AppliedThroughEventID() != 3 {
		t.Fatalf("cursor advanced before commit: result=%d session=%d", first.AppliedThroughEventID, session.AppliedThroughEventID())
	}
	if _, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "overlap", ExecutionGeneration: 7}); !errors.Is(err, executor.ErrExecutionSessionPendingCommit) {
		t.Fatalf("overlapping activation error = %v, want %v", err, executor.ErrExecutionSessionPendingCommit)
	}

	err = session.Commit(history.WorkflowTaskCompletionAck{
		Accepted:                true,
		CommittedThroughEventID: 7,
		CommittedEvents: []history.Event{
			executionSessionEvent(6, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
			executionSessionEvent(7, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"})),
		},
		ActivationID:        first.ActivationID,
		CommandDigest:       first.CommandDigest,
		ExecutionGeneration: 7,
	})
	if err != nil {
		t.Fatalf("commit first activation: %v", err)
	}
	if got := session.AppliedThroughEventID(); got != 7 {
		t.Fatalf("committed cursor = %d, want 7", got)
	}

	second, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "activation-2",
		ExecutionGeneration: 7,
		Events: []history.Event{
			executionSessionEvent(8, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 7, Output: map[string]any{"second": "done"}})),
			executionSessionEvent(9, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 9,
	})
	if err != nil {
		t.Fatalf("activate second delta: %v", err)
	}
	assertSingleSessionCommand(t, second.Commands, history.CmdCompleteWorkflow, "")
}

func TestExecutionSessionRetainsActivityFailurePath(t *testing.T) {
	session := NewExecutionSession(context.Background(), compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go"), 1)
	t.Cleanup(func() { _ = session.Close() })

	_, err := session.Restore(context.Background(), NewSliceHistoryIterator([]history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(3, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
	}))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	first, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "plain-1",
		ExecutionGeneration: 1,
		Events: []history.Event{
			executionSessionEvent(4, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 3})),
			executionSessionEvent(5, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 5,
	})
	if err != nil {
		t.Fatalf("activate first delta: %v", err)
	}
	assertSingleSessionCommand(t, first.Commands, history.CmdScheduleActivity, "#a2")
	if err := session.Commit(history.WorkflowTaskCompletionAck{
		Accepted: true, CommittedThroughEventID: 7,
		CommittedEvents: []history.Event{
			executionSessionEvent(6, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
			executionSessionEvent(7, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"})),
		},
		ActivationID: first.ActivationID, CommandDigest: first.CommandDigest, ExecutionGeneration: 1,
	}); err != nil {
		t.Fatalf("commit first activation: %v", err)
	}

	second, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "plain-2",
		ExecutionGeneration: 1,
		Events: []history.Event{
			executionSessionEvent(8, history.NewActivityFailedEvent(history.ActivityFailedEvent{ScheduledEventID: 7, Error: "step-b failed", Failure: &protocol.Failure{Message: "step-b failed"}})),
			executionSessionEvent(9, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 9,
	})
	if err != nil {
		t.Fatalf("activate failure delta: %v", err)
	}
	assertSingleSessionCommand(t, second.Commands, history.CmdFailWorkflow, "")
	if second.Commands[0].Error != "step-b failed" {
		t.Fatalf("failure = %q, want step-b failed", second.Commands[0].Error)
	}
}

func TestExecutionSessionEmptyRestoreDefersMainUntilFirstActivation(t *testing.T) {
	session := NewExecutionSession(context.Background(), compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go"), 3)
	t.Cleanup(func() { _ = session.Close() })

	restored, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil))
	if err != nil {
		t.Fatalf("restore empty history: %v", err)
	}
	if restored.AppliedThroughEventID != 0 {
		t.Fatalf("empty restore cursor = %d, want 0", restored.AppliedThroughEventID)
	}
	result, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "first-activation",
		ExecutionGeneration: 3,
		Events: []history.Event{
			executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{"request": "first"}, "", "", "", "", 0)),
			executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 2,
	})
	if err != nil {
		t.Fatalf("activate first history: %v", err)
	}
	assertSingleSessionCommand(t, result.Commands, history.CmdScheduleActivity, "#a1")
}

func TestExecutionSessionInvalidAckClosesSession(t *testing.T) {
	session := NewExecutionSession(context.Background(), compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go"), 4)
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	result, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "activation-fenced",
		ExecutionGeneration: 4,
		Events: []history.Event{
			executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{}, "", "", "", "", 0)),
			executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 2,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	err = session.Commit(history.WorkflowTaskCompletionAck{
		Accepted: true, CommittedThroughEventID: 4,
		CommittedEvents: []history.Event{
			executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
			executionSessionEvent(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
		},
		ActivationID: result.ActivationID, CommandDigest: "sha256:stale", ExecutionGeneration: 4,
	})
	if err == nil {
		t.Fatal("commit with stale digest succeeded")
	}
	if _, err := session.Activate(context.Background(), history.WorkflowActivation{}); !errors.Is(err, executor.ErrExecutionSessionClosed) {
		t.Fatalf("activation after ambiguous commit error = %v, want closed session", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

func executionSessionEvent(id int64, event history.Event) history.Event {
	event.ID = id
	event.Timestamp = time.Unix(id, 0).UTC()
	return event
}

func assertSingleSessionCommand(t *testing.T, commands []history.Command, commandType history.CommandType, stepID string) {
	t.Helper()
	if len(commands) != 1 {
		t.Fatalf("commands = %+v, want one %s command", commands, commandType)
	}
	if commands[0].Type != commandType || commands[0].ActivityID != stepID {
		t.Fatalf("command = %+v, want type=%s step=%q", commands[0], commandType, stepID)
	}
}

type errorHistoryIterator struct {
	err error
}

type pagedSessionHistoryIterator struct {
	events   []history.Event
	pageSize int
}

func newPagedSessionHistoryIterator(pageSize int, events []history.Event) executor.HistoryIterator {
	return &pagedSessionHistoryIterator{events: append([]history.Event(nil), events...), pageSize: pageSize}
}

func (i *pagedSessionHistoryIterator) Next(context.Context) ([]history.Event, error) {
	if len(i.events) == 0 {
		return nil, io.EOF
	}
	size := min(i.pageSize, len(i.events))
	page := append([]history.Event(nil), i.events[:size]...)
	i.events = i.events[size:]
	return page, nil
}

func (i errorHistoryIterator) Next(context.Context) ([]history.Event, error) {
	if i.err == nil {
		return nil, io.EOF
	}
	return nil, i.err
}

func restoredTestSession(t *testing.T) executor.ExecutionSession {
	t.Helper()
	nodes := compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go")
	session := NewExecutionSession(context.Background(), nodes, 7)
	t.Cleanup(func() { _ = session.Close() })
	_, err := session.Restore(context.Background(), NewSliceHistoryIterator([]history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{"request": "one"}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(3, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
	}))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	return session
}

// Error Case：activation 增量与 session 游标/generation 对不上 → ErrExecutionSessionInvalid。
func TestExecutionSessionMarksActivationValidationErrorsInvalid(t *testing.T) {
	started := func(id int64) history.Event {
		return executionSessionEvent(id, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0))
	}
	cases := map[string]history.WorkflowActivation{
		"generation mismatch": {ActivationID: "a", ExecutionGeneration: 8, Events: []history.Event{started(4)}, ThroughEventID: 4},
		"empty delta":         {ActivationID: "a", ExecutionGeneration: 7},
		"gap in delta":        {ActivationID: "a", ExecutionGeneration: 7, Events: []history.Event{started(6)}, ThroughEventID: 6},
		"tail is not WorkflowTaskStarted": {ActivationID: "a", ExecutionGeneration: 7, ThroughEventID: 4, Events: []history.Event{
			executionSessionEvent(4, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 3})),
		}},
	}
	for name, activation := range cases {
		t.Run(name, func(t *testing.T) {
			session := restoredTestSession(t)
			if _, err := session.Activate(context.Background(), activation); !errors.Is(err, executor.ErrExecutionSessionInvalid) {
				t.Fatalf("activate error = %v, want %v", err, executor.ErrExecutionSessionInvalid)
			}
			if _, err := session.Activate(context.Background(), activation); !errors.Is(err, executor.ErrExecutionSessionClosed) || !errors.Is(err, executor.ErrExecutionSessionInvalid) {
				t.Fatalf("closed session error = %v", err)
			}
		})
	}
}

// Error Case：history event ID 不连续 → ErrExecutionSessionInvalid。
func TestExecutionSessionMarksHistoryGapInvalid(t *testing.T) {
	session := NewExecutionSession(context.Background(), nil, 1)
	t.Cleanup(func() { _ = session.Close() })
	_, err := session.Restore(context.Background(), NewSliceHistoryIterator([]history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)),
		executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
	}))
	if !errors.Is(err, executor.ErrExecutionSessionInvalid) {
		t.Fatalf("restore error = %v, want %v", err, executor.ErrExecutionSessionInvalid)
	}
}

// Error Case: 已提交事件的规范化大小只在需要本地度量时计算。无法度量的事件不影响 Commit，
// 在下一次没有 Server 统计的 activation 上报错并关闭 session。
func TestExecutionSessionUnmeasurableCommittedEventFailsNextLocalMeasurement(t *testing.T) {
	session := restoredTestSession(t)
	result, err := session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "activation-1",
		ExecutionGeneration: 7,
		Events: []history.Event{
			executionSessionEvent(4, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"first": "done"}})),
			executionSessionEvent(5, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		},
		ThroughEventID: 5,
	})
	require.NoError(t, err)
	err = session.Commit(history.WorkflowTaskCompletionAck{
		Accepted:                true,
		CommittedThroughEventID: 7,
		CommittedEvents: []history.Event{
			executionSessionEvent(6, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
			// chan 无法序列化，度量 history 大小时报错。
			executionSessionEvent(7, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B", Input: map[string]any{"bad": make(chan int)}})),
		},
		ActivationID:        result.ActivationID,
		CommandDigest:       result.CommandDigest,
		ExecutionGeneration: 7,
	})
	require.NoError(t, err)

	_, err = session.Activate(context.Background(), history.WorkflowActivation{
		ActivationID:        "activation-2",
		ExecutionGeneration: 7,
		Events:              []history.Event{executionSessionEvent(8, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0))},
		ThroughEventID:      8,
	})
	require.ErrorContains(t, err, "measure workflow history event 7")
	_, err = session.Activate(context.Background(), history.WorkflowActivation{ActivationID: "next", ExecutionGeneration: 7})
	assert.ErrorIs(t, err, executor.ErrExecutionSessionClosed)
}

// Integration: 一次性 Execute 与 session（Restore + Activate）共用同一驱动——对同一段历史的每个
// Workflow Task 边界，两条路径必须产出相同的命令；Restore 对未提交尾部的历史报 session 无效。
func TestExecutionSessionMatchesExecuteAtEveryWorkflowTask(t *testing.T) {
	nodes := compileWLangSource(t, []byte(executionSessionSequentialActivities), "session-flow", "W", "session.go")
	events := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(map[string]any{"request": "one"}, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A"})),
		executionSessionEvent(5, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"first": "done"}})),
		executionSessionEvent(6, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		executionSessionEvent(7, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(8, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "activities.B"})),
		executionSessionEvent(9, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 8, Output: map[string]any{"second": "done"}})),
		executionSessionEvent(10, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
	}
	// 每个用例：committed 是已提交前缀的长度，其后到 started 为止是本次 activation。
	cases := []struct {
		name               string
		committed, started int
		wantType           history.CommandType
	}{
		{name: "first task", committed: 0, started: 2, wantType: history.CmdScheduleActivity},
		{name: "second task", committed: 4, started: 6, wantType: history.CmdScheduleActivity},
		{name: "final task", committed: 8, started: 10, wantType: history.CmdCompleteWorkflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full, err := NewWorkflowExecutor(nodes, events[:tc.started]).Execute()
			require.NoError(t, err)

			session := NewExecutionSession(context.Background(), nodes, 1)
			t.Cleanup(func() { _ = session.Close() })
			_, err = session.Restore(context.Background(), NewSliceHistoryIterator(events[:tc.committed]))
			require.NoError(t, err)
			activated, err := session.Activate(context.Background(), history.WorkflowActivation{
				ActivationID:        "activation",
				ExecutionGeneration: 1,
				Events:              events[tc.committed:tc.started],
				ThroughEventID:      int64(tc.started),
			})
			require.NoError(t, err)

			require.Len(t, full, 1)
			assert.Equal(t, tc.wantType, full[0].Type)
			fullDigest, err := history.DigestCommands(full)
			require.NoError(t, err)
			assert.Equal(t, fullDigest, activated.CommandDigest)
		})
	}

	t.Run("Error Case: restore rejects an uncommitted tail", func(t *testing.T) {
		session := NewExecutionSession(context.Background(), nodes, 1)
		t.Cleanup(func() { _ = session.Close() })
		_, err := session.Restore(context.Background(), NewSliceHistoryIterator(events[:5]))
		assert.ErrorIs(t, err, executor.ErrExecutionSessionInvalid)
	})
}

// lane 内的 Activity 后接 main 上的顺序 Activity：在每个 Workflow Task 上，
// 一次性 Execute 与会话 Restore+Activate 产出相同命令。
func TestExecutionSessionMatchesExecuteForLanes(t *testing.T) {
	nodes := compileWLangSource(t, []byte(`package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx workflow.Context) {
		var a map[string]any
		_ = workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &a)
	})
	wg.Wait(ctx)
	var c map[string]any
	return workflow.ExecuteActivity(ctx, activities.C, input).Get(ctx, &c)
}
`), "session-lanes", "W", "session_lanes.go")
	events := []history.Event{
		executionSessionEvent(1, history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)),
		executionSessionEvent(2, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		executionSessionEvent(3, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(4, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "/0#a1", ActivityType: "activities.A"})),
		executionSessionEvent(5, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"a": "done"}})),
		executionSessionEvent(6, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
		executionSessionEvent(7, history.NewWorkflowTaskCompletedEvent(0, "worker-a")),
		executionSessionEvent(8, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.C"})),
		executionSessionEvent(9, history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ScheduledEventID: 8, Output: map[string]any{"c": "done"}})),
		executionSessionEvent(10, history.NewWorkflowTaskStartedEvent(0, "worker-a", "build-a", 0)),
	}
	cases := []struct {
		name               string
		committed, started int
		wantType           history.CommandType
		wantStepID         string
	}{
		{name: "lane activity", committed: 0, started: 2, wantType: history.CmdScheduleActivity, wantStepID: "/0#a1"},
		{name: "node after the lane", committed: 4, started: 6, wantType: history.CmdScheduleActivity, wantStepID: "#a1"},
		{name: "final task", committed: 8, started: 10, wantType: history.CmdCompleteWorkflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full, err := NewWorkflowExecutor(nodes, events[:tc.started]).Execute()
			require.NoError(t, err)

			session := NewExecutionSession(context.Background(), nodes, 1)
			t.Cleanup(func() { _ = session.Close() })
			_, err = session.Restore(context.Background(), NewSliceHistoryIterator(events[:tc.committed]))
			require.NoError(t, err)
			activated, err := session.Activate(context.Background(), history.WorkflowActivation{
				ActivationID:        "activation",
				ExecutionGeneration: 1,
				Events:              events[tc.committed:tc.started],
				ThroughEventID:      int64(tc.started),
			})
			require.NoError(t, err)

			require.Len(t, full, 1)
			assert.Equal(t, tc.wantType, full[0].Type)
			assert.Equal(t, tc.wantStepID, full[0].ActivityID)
			fullDigest, err := history.DigestCommands(full)
			require.NoError(t, err)
			assert.Equal(t, fullDigest, activated.CommandDigest)
		})
	}
}
