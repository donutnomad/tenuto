package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type incrementalTestSession struct {
	restoreErr  error
	activateErr error
	closed      bool
	activations int
	through     int64
	commands    []history.Command
}

func (s *incrementalTestSession) Restore(context.Context, HistoryIterator) (RestoreResult, error) {
	return RestoreResult{AppliedThroughEventID: s.through}, s.restoreErr
}

func (s *incrementalTestSession) Activate(_ context.Context, activation history.WorkflowActivation) (history.ActivationResult, error) {
	s.activations++
	return history.ActivationResult{ActivationID: activation.ActivationID, Commands: s.commands}, s.activateErr
}

func (s *incrementalTestSession) Commit(ack history.WorkflowTaskCompletionAck) error {
	s.through = ack.CommittedThroughEventID
	return nil
}

func (s *incrementalTestSession) AppliedThroughEventID() int64 { return s.through }
func (s *incrementalTestSession) EstimatedSize() int64         { return 1 }
func (s *incrementalTestSession) Close() error {
	s.closed = true
	return nil
}

func incrementalTestOptions(t *testing.T, session *incrementalTestSession) IncrementalTaskOptions {
	t.Helper()
	cache, err := NewExecutionCache(ExecutionCacheConfig{MaxEntries: 2, MaxEstimatedBytes: 1024, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return IncrementalTaskOptions{
		Cache: cache,
		Key: ExecutionCacheKey{
			Namespace: "default", RunID: "run-1", WorkflowType: "flow",
			ArtifactFingerprint: "artifact-1", ExecutorABIVersion: "v1",
		},
		NewSession: func(context.Context, *program.WorkflowConfig, int64) ExecutionSession { return session },
		NewHistory: func([]history.Event) HistoryIterator { return nil },
		ReadEvents: func(context.Context, string) ([]history.Event, error) {
			return []history.Event{{ID: 1}}, nil
		},
		GetActivation: func(context.Context, protocol.Task, int64, int64, string) (history.WorkflowActivation, error) {
			return history.WorkflowActivation{ActivationID: "activation-1"}, nil
		},
		Complete: func(_ context.Context, _ protocol.Task, result history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
			return history.WorkflowTaskCompletionAck{Accepted: true, ActivationID: result.ActivationID, CommittedThroughEventID: 1}, nil
		},
		ActivationID: "activation-1",
	}
}

func TestRunIncrementalTaskReusesCommittedSession(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	reads, activations := 0, 0
	opts.ReadEvents = func(context.Context, string) ([]history.Event, error) {
		reads++
		return []history.Event{{ID: 1}}, nil
	}
	opts.GetActivation = func(_ context.Context, _ protocol.Task, after, generation int64, _ string) (history.WorkflowActivation, error) {
		activations++
		if after != 1 || generation != 1 {
			t.Fatalf("warm activation after=%d generation=%d", after, generation)
		}
		return history.WorkflowActivation{ActivationID: "activation-1"}, nil
	}
	task := protocol.Task{RunID: "run-1", ExecutionGeneration: 1}
	for range 2 {
		if result := RunIncrementalTask(context.Background(), context.Background(), task, opts); result.Err != nil {
			t.Fatalf("incremental task: %+v", result)
		}
	}
	if reads != 1 || activations != 1 || session.activations != 2 || session.closed {
		t.Fatalf("reads=%d activations=%d session=%+v", reads, activations, session)
	}
}

func TestRunIncrementalTaskClosesFailedColdSession(t *testing.T) {
	session := &incrementalTestSession{restoreErr: errors.New("restore failed")}
	opts := incrementalTestOptions(t, session)
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != IncrementalTaskExecutionFailure || !errors.Is(result.Err, session.restoreErr) || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
	if got := opts.Cache.Snapshot().Entries; got != 0 {
		t.Fatalf("cached failed session: %d entries", got)
	}
}

func TestRunIncrementalTaskDropsSessionWhenCompletionNotAccepted(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	opts.Complete = func(context.Context, protocol.Task, history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
		return history.WorkflowTaskCompletionAck{Accepted: false}, nil
	}
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != 0 || result.Err != nil || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
	if got := opts.Cache.Snapshot().Entries; got != 0 {
		t.Fatalf("cached unaccepted session: %d entries", got)
	}
}

func TestRunIncrementalTaskClosesSessionWhenCompletionFails(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	completionErr := errors.New("completion failed")
	opts.Complete = func(context.Context, protocol.Task, history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
		return history.WorkflowTaskCompletionAck{}, completionErr
	}
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != IncrementalTaskCompletionFailure || !errors.Is(result.Err, completionErr) || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
}

// Edge Case：Server 已提交本次 Workflow Task 之后 session 进不了缓存（例如 Worker 正在停止、缓存已关闭）
// → 丢弃 session，任务照常算完成；已提交的任务不能再被当作失败重投。
func TestRunIncrementalTaskDropsSessionWhenAdmitFailsAfterCommit(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	complete := opts.Complete
	opts.Complete = func(ctx context.Context, task protocol.Task, result history.ActivationResult) (history.WorkflowTaskCompletionAck, error) {
		_ = opts.Cache.Close()
		return complete(ctx, task, result)
	}
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != 0 || result.Err != nil || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
}

// Error Case（cache hit）：缓存 session 拒绝 activation 增量 → 逐出 Entry，返回 session 失败；
// 下一次投递走 cold restore。
func TestRunIncrementalTaskEvictsInvalidCachedSession(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	reads := 0
	opts.ReadEvents = func(context.Context, string) ([]history.Event, error) {
		reads++
		return []history.Event{{ID: 1}}, nil
	}
	task := protocol.Task{RunID: "run-1"}
	if result := RunIncrementalTask(context.Background(), context.Background(), task, opts); result.Err != nil {
		t.Fatalf("warm up: %+v", result)
	}

	session.activateErr = fmt.Errorf("%w: delta event 9 does not follow 1", ErrExecutionSessionInvalid)
	result := RunIncrementalTask(context.Background(), context.Background(), task, opts)
	if result.Failure != IncrementalTaskSessionFailure || !errors.Is(result.Err, ErrExecutionSessionInvalid) {
		t.Fatalf("result=%+v", result)
	}
	if got := opts.Cache.Snapshot().Entries; got != 0 || !session.closed {
		t.Fatalf("invalid session stayed cached: entries=%d closed=%v", got, session.closed)
	}

	session.activateErr = nil
	if result := RunIncrementalTask(context.Background(), context.Background(), task, opts); result.Err != nil {
		t.Fatalf("redelivery: %+v", result)
	}
	if reads != 2 {
		t.Fatalf("redelivery must cold restore from history: reads=%d", reads)
	}
}

// Error Case（cache miss）：cold restore 遇到形状错误的 history → session 失败，不进入缓存。
func TestRunIncrementalTaskReportsInvalidHistoryOnColdRestore(t *testing.T) {
	session := &incrementalTestSession{restoreErr: fmt.Errorf("%w: event ID 3 does not follow 1", ErrExecutionSessionInvalid)}
	opts := incrementalTestOptions(t, session)
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != IncrementalTaskSessionFailure || !errors.Is(result.Err, ErrExecutionSessionInvalid) || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
	if got := opts.Cache.Snapshot().Entries; got != 0 {
		t.Fatalf("cached failed session: %d entries", got)
	}
}

// Edge Case（cache miss）：命令事件与它的 WorkflowTaskCompleted 之间隔着外部事件 → 不建 session，
// 交给按 SplitBatches 归批的 full-history 路径。
func TestRunIncrementalTaskLeavesInterleavedCommandHistoryToFullHistoryPath(t *testing.T) {
	session := &incrementalTestSession{}
	opts := incrementalTestOptions(t, session)
	sessions := 0
	opts.NewSession = func(context.Context, *program.WorkflowConfig, int64) ExecutionSession {
		sessions++
		return session
	}
	opts.ReadEvents = func(context.Context, string) ([]history.Event, error) {
		return []history.Event{
			{ID: 1, WorkflowStarted: &history.WorkflowStartedEvent{}},
			{ID: 2, WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}},
			{ID: 3, ActivityCompleted: &history.ActivityCompletedEvent{}},
			{ID: 4, ActivityScheduled: &history.ActivityScheduledEvent{}},
		}, nil
	}
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != IncrementalTaskNotSelected || !errors.Is(result.Err, ErrIncrementalNotSelected) {
		t.Fatalf("result=%+v", result)
	}
	if sessions != 0 || opts.Cache.Snapshot().Entries != 0 {
		t.Fatalf("sessions=%d cache entries=%d", sessions, opts.Cache.Snapshot().Entries)
	}
}

// Error Case：workflow 执行错误保持 ExecutionFailure，由上报层 Fail/Block。
func TestRunIncrementalTaskKeepsWorkflowErrorAsExecutionFailure(t *testing.T) {
	session := &incrementalTestSession{activateErr: errors.New("workflow coroutine panic: boom")}
	opts := incrementalTestOptions(t, session)
	result := RunIncrementalTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, opts)
	if result.Failure != IncrementalTaskExecutionFailure || !session.closed {
		t.Fatalf("result=%+v closed=%v", result, session.closed)
	}
}
