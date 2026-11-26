package interpreter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	executor2 "github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type executionSessionState uint8

const (
	executionSessionNew executionSessionState = iota
	executionSessionIdle
	executionSessionActivating
	executionSessionPendingCommit
	executionSessionClosed
)

// workflowExecutionSession 让一个 WorkflowExecutor（连同它 park 住的协程栈）跨多个 Workflow Task 存活，
// 每个 Workflow Task 只喂增量事件。
//
// 状态机：
//
//	New ──Restore──▶ Idle ──Activate──▶ PendingCommit ──Commit──▶ Idle
//	                                                     └─ack.Terminal─▶ Closed
//
// Activating 是 Restore/Activate 持锁执行期间的瞬时状态。任何一步失败都进入 Closed（failLocked），
// 调用方丢弃会话并从完整 History 重建；Closed 是终态。
//
// 一个 Workflow Task 的顺序：
//
//  1. Activate(events)：events 紧接 appliedThrough，executor 以 live 方式推进并产出命令；
//     appliedThrough 保持原值，会话记住本次的 ActivationID、命令摘要与 ThroughEventID。
//  2. 调用方把命令提交给 Server（Complete）。
//  3. Commit(ack)：ack 必须对应第 1 步的 Activation 与命令摘要，ack.CommittedEvents 是 ID 连续的
//     WorkflowTaskCompleted 与命令事件，排在 ThroughEventID 之后。会话把它们灌入事实表。
//     它们紧接 ThroughEventID 时 appliedThrough 推进到 ack.CommittedThroughEventID；中间隔着 Workflow Task
//     进行期间落库的事实时，appliedThrough 停在 ThroughEventID，已灌入的提交事件记为 committedAhead，
//     下一次 Activate 的增量从这些事实开始，跳过 committedAhead。
//
// 不变量：Idle 状态下，executor 的状态等于「用 SplitBatches 重放 appliedThrough 之前的已提交决策批，
// 再加上 committedAhead 所属的那一批」得到的状态；下一次 Activate 的事件（去掉 committedAhead）
// 正是 SplitBatches 给出的下一批。Restore 用已提交的 History 建立这个不变量，所以它要求 History 以已提交的
// Workflow Task 结尾。
type workflowExecutionSession struct {
	mu                  sync.Mutex
	state               executionSessionState
	executor            *WorkflowExecutor
	contextGate         *activationContextGate
	lifetimeCancel      context.CancelFunc
	executionGeneration int64
	appliedThrough      int64
	// committedAhead 是已灌入、但排在 appliedThrough 之后未看到的事实后面的提交事件（WorkflowTaskCompleted
	// 与命令事件）的 ID 区间；没有时为零值。
	committedAhead    eventRange
	pendingActivation string
	pendingDigest     string
	pendingThrough    int64
	closeOnce         sync.Once
	closeErr          error
}

func NewExecutionSession(lifetimeCtx context.Context, workflow *program.WorkflowConfig, executionGeneration int64) executor2.ExecutionSession {
	if lifetimeCtx == nil {
		lifetimeCtx = context.Background()
	}
	lifetimeCtx, lifetimeCancel := context.WithCancel(lifetimeCtx)
	gate := newActivationContextGate(lifetimeCtx)
	executor := NewWorkflowExecutorWithContext(gate, workflow, nil)
	return &workflowExecutionSession{
		state:               executionSessionNew,
		executor:            executor,
		contextGate:         gate,
		lifetimeCancel:      lifetimeCancel,
		executionGeneration: executionGeneration,
	}
}

func (s *workflowExecutionSession) Restore(ctx context.Context, history executor2.HistoryIterator) (result executor2.RestoreResult, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == executionSessionClosed {
		return executor2.RestoreResult{}, executor2.ErrExecutionSessionClosed
	}
	if s.state != executionSessionNew {
		return executor2.RestoreResult{}, executor2.ErrExecutionSessionAlreadyRestored
	}
	if history == nil {
		return executor2.RestoreResult{}, s.failLocked(invalidSession(errors.New("restore history iterator is nil")))
	}
	if err := s.contextGate.Begin(ctx); err != nil {
		return executor2.RestoreResult{}, s.failLocked(err)
	}
	defer s.contextGate.End()

	s.state = executionSessionActivating
	events, err := readRestoreHistory(ctx, history)
	if err != nil {
		return executor2.RestoreResult{}, s.failLocked(err)
	}
	batches := executor2.SplitBatches(events)
	committed := len(batches) - 1
	appliedThrough, committedAhead, err := restoreCursor(events, batches[committed])
	if err != nil {
		return executor2.RestoreResult{}, s.failLocked(invalidSession(err))
	}
	s.executor.startScheduler()
	if err := s.executor.restore(batches[:committed]); err != nil {
		return executor2.RestoreResult{}, s.failLocked(err)
	}
	s.executor.isReplay = false
	s.executor.commands = nil
	s.executor.replayCommands = nil
	s.appliedThrough = appliedThrough
	s.committedAhead = committedAhead
	s.state = executionSessionIdle
	return executor2.RestoreResult{AppliedThroughEventID: s.appliedThrough, EstimatedSize: s.estimatedSizeLocked()}, nil
}

// restoreCursor 返回 Restore 之后的游标。History 以已提交的 Workflow Task 结尾时，未归入已提交批的只能是
// 最后一个 Workflow Task 没有看到的事实（carried，ID 小于最后一个 WorkflowTaskCompleted）：游标停在它们之前，
// 最后一个 WorkflowTaskCompleted 到 History 末尾的提交事件记为 committedAhead。
func restoreCursor(events, carried []history.Event) (int64, eventRange, error) {
	if len(events) == 0 {
		return 0, eventRange{}, nil
	}
	last := events[len(events)-1].ID
	if len(carried) == 0 {
		return last, eventRange{}, nil
	}
	completed := slices.IndexFunc(events, func(event history.Event) bool {
		return event.WorkflowTaskCompleted != nil && event.ID > carried[len(carried)-1].ID
	})
	if completed < 0 {
		return 0, eventRange{}, errors.New("restore requires history through a committed WorkflowTaskCompleted tail")
	}
	if carried[len(carried)-1].ID != events[completed].ID-1 {
		return 0, eventRange{}, fmt.Errorf("restore: unseen events end at %d, not before WorkflowTaskCompleted %d", carried[len(carried)-1].ID, events[completed].ID)
	}
	return carried[0].ID - 1, eventRange{from: events[completed].ID, through: last}, nil
}

// readRestoreHistory 读完迭代器并要求 Event ID 从 1 起连续。
func readRestoreHistory(ctx context.Context, pages executor2.HistoryIterator) ([]history.Event, error) {
	var events []history.Event
	for {
		page, err := pages.Next(ctx)
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return nil, fmt.Errorf("restore execution session: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := history.ValidateEventSequence(int64(len(events)), page); err != nil {
			return nil, invalidSession(fmt.Errorf("restore: %w", err))
		}
		events = append(events, page...)
	}
}

func (s *workflowExecutionSession) Activate(ctx context.Context, activation history.WorkflowActivation) (result history.ActivationResult, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case executionSessionClosed:
		return history.ActivationResult{}, executor2.ErrExecutionSessionClosed
	case executionSessionNew:
		return history.ActivationResult{}, executor2.ErrExecutionSessionNotRestored
	case executionSessionPendingCommit:
		return history.ActivationResult{}, executor2.ErrExecutionSessionPendingCommit
	case executionSessionActivating:
		return history.ActivationResult{}, invalidSession(errors.New("already activating"))
	}
	if err := validateActivation(s.executionGeneration, s.appliedThrough, activation); err != nil {
		return history.ActivationResult{}, s.failLocked(invalidSession(err))
	}

	live, err := s.committedAhead.skip(activation.Events)
	if err != nil {
		return history.ActivationResult{}, s.failLocked(invalidSession(err))
	}
	if !endsWithStartedWorkflowTask(live) {
		return history.ActivationResult{}, s.failLocked(invalidSession(errors.New("workflow activation delta must end with a started workflow task")))
	}
	if err := s.contextGate.Begin(ctx); err != nil {
		return history.ActivationResult{}, s.failLocked(err)
	}
	defer s.contextGate.End()

	s.state = executionSessionActivating
	s.committedAhead = eventRange{}
	s.executor.commands = nil
	s.executor.evalErr = nil
	s.executor.execErr = nil
	if err := s.executor.activate(live); err != nil {
		return history.ActivationResult{}, s.failLocked(err)
	}
	if err := s.contextGate.Err(); err != nil {
		return history.ActivationResult{}, s.failLocked(err)
	}

	commands := cloneCommands(s.executor.commands)
	digest, err := history.DigestCommands(commands)
	if err != nil {
		return history.ActivationResult{}, s.failLocked(err)
	}
	s.pendingActivation = activation.ActivationID
	s.pendingDigest = digest
	s.pendingThrough = activation.ThroughEventID
	s.state = executionSessionPendingCommit
	return history.ActivationResult{
		ActivationID:            activation.ActivationID,
		ExecutionGeneration:     s.executionGeneration,
		Commands:                cloneCommands(commands),
		CommandDigest:           digest,
		AppliedThroughEventID:   s.appliedThrough,
		ActivatedThroughEventID: activation.ThroughEventID,
	}, nil
}

func (s *workflowExecutionSession) Commit(ack history.WorkflowTaskCompletionAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == executionSessionClosed {
		return executor2.ErrExecutionSessionClosed
	}
	if s.state != executionSessionPendingCommit {
		return executor2.ErrExecutionSessionNotPending
	}
	if err := s.validateAckLocked(ack); err != nil {
		return s.failLocked(err)
	}
	s.executor.applyBatch(ack.CommittedEvents)
	if err := s.executor.executionError(); err != nil {
		return s.failLocked(err)
	}
	if first := ack.CommittedEvents[0].ID; first == s.pendingThrough+1 {
		s.appliedThrough = ack.CommittedThroughEventID
	} else {
		// Workflow Task 进行期间落库的事实（pendingThrough, first）没有被这次决策看到，留给下一次 Activate。
		s.appliedThrough = s.pendingThrough
		s.committedAhead = eventRange{from: first, through: ack.CommittedThroughEventID}
	}
	s.pendingActivation = ""
	s.pendingDigest = ""
	s.pendingThrough = 0
	s.executor.commands = nil
	if ack.Terminal {
		return s.closeLocked()
	}
	s.state = executionSessionIdle
	return nil
}

func (s *workflowExecutionSession) AppliedThroughEventID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appliedThrough
}

func (s *workflowExecutionSession) EstimatedSize() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.estimatedSizeLocked()
}

func (s *workflowExecutionSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeLocked()
}

func (s *workflowExecutionSession) validateAckLocked(ack history.WorkflowTaskCompletionAck) error {
	if !ack.Accepted {
		return executor2.ErrWorkflowTaskCompletionRejected
	}
	if ack.ActivationID != s.pendingActivation {
		return fmt.Errorf("workflow task completion activation %q does not match pending activation %q", ack.ActivationID, s.pendingActivation)
	}
	if ack.ExecutionGeneration != s.executionGeneration {
		return fmt.Errorf("workflow task completion generation %d does not match session generation %d", ack.ExecutionGeneration, s.executionGeneration)
	}
	if ack.CommandDigest != s.pendingDigest {
		return fmt.Errorf("workflow task completion command digest %q does not match pending digest %q", ack.CommandDigest, s.pendingDigest)
	}
	if len(ack.CommittedEvents) == 0 || ack.CommittedEvents[0].WorkflowTaskCompleted == nil {
		return errors.New("workflow task completion authoritative tail must begin with WorkflowTaskCompleted")
	}
	first := ack.CommittedEvents[0]
	if first.ID <= s.pendingThrough {
		return fmt.Errorf("workflow task completion authoritative tail starts at %d, not after activated event %d", first.ID, s.pendingThrough)
	}
	if err := history.ValidateEventSequence(first.ID-1, ack.CommittedEvents); err != nil {
		return fmt.Errorf("workflow task completion authoritative tail: %w", err)
	}
	// SeenThroughEventID 为 0 表示 Worker 看到了 WorkflowTaskCompleted 之前的全部事件，只在两者之间没有间隙时成立。
	seen := first.WorkflowTaskCompleted.SeenThroughEventID
	if seen == 0 && first.ID != s.pendingThrough+1 || seen != 0 && seen != s.pendingThrough {
		return fmt.Errorf("workflow task completion saw through event %d, activation was through %d", seen, s.pendingThrough)
	}
	terminal := false
	for i, event := range ack.CommittedEvents[1:] {
		if !isWorkflowTaskCommandResultEvent(event) {
			return fmt.Errorf("workflow task completion authoritative tail event %d at index %d is not a command result", event.ID, i+1)
		}
		terminal = terminal || isTerminalWorkflowEvent(event)
	}
	if terminal != ack.Terminal {
		return fmt.Errorf("workflow task completion terminal flag %t does not match authoritative tail terminal state %t", ack.Terminal, terminal)
	}
	last := ack.CommittedEvents[len(ack.CommittedEvents)-1].ID
	if ack.CommittedThroughEventID != last {
		return fmt.Errorf("workflow task completion through event %d does not match authoritative tail %d", ack.CommittedThroughEventID, last)
	}
	return nil
}

func invalidSession(err error) error {
	return fmt.Errorf("%w: %w", executor2.ErrExecutionSessionInvalid, err)
}

func (s *workflowExecutionSession) failLocked(err error) error {
	closeErr := s.closeLocked()
	return errors.Join(err, closeErr)
}

func (s *workflowExecutionSession) closeLocked() error {
	s.closeOnce.Do(func() {
		s.state = executionSessionClosed
		s.lifetimeCancel()
		if s.executor.sched != nil {
			s.executor.sched.Reclaim()
		}
	})
	return s.closeErr
}

func (s *workflowExecutionSession) estimatedSizeLocked() int64 {
	const (
		baseBytes          = int64(2048)
		mapEntryBytes      = int64(96)
		parkedRoutineBytes = int64(16 << 10)
	)
	e := s.executor
	entries := len(e.completed) + len(e.failed) + len(e.unclaimedActivities) + len(e.awaitingActivities) +
		len(e.signals) + len(e.updateAccepted) +
		len(e.updateCompleted) + len(e.timersScheduled) + len(e.timersFired) + len(e.markers) + len(e.changeVersions) +
		len(e.childStarted) + len(e.childCompleted) + len(e.childFailures) + len(e.waitGroupN) + len(e.selectors)
	routines := 0
	if e.sched != nil {
		routines = e.sched.RoutineCount()
	}
	return baseBytes + int64(entries)*mapEntryBytes + int64(routines)*parkedRoutineBytes
}

type sliceHistoryIterator struct {
	events []history.Event
	done   bool
}

func NewSliceHistoryIterator(events []history.Event) executor2.HistoryIterator {
	return &sliceHistoryIterator{events: append([]history.Event(nil), events...)}
}

func (i *sliceHistoryIterator) Next(context.Context) ([]history.Event, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true
	return append([]history.Event(nil), i.events...), nil
}

func validateActivation(generation, appliedThrough int64, activation history.WorkflowActivation) error {
	if activation.ActivationID == "" {
		return errors.New("workflow activation ID is empty")
	}
	if activation.ExecutionGeneration != generation {
		return fmt.Errorf("workflow activation generation %d does not match session generation %d", activation.ExecutionGeneration, generation)
	}
	if len(activation.Events) == 0 {
		return errors.New("workflow activation delta is empty")
	}
	if err := history.ValidateEventSequence(appliedThrough, activation.Events); err != nil {
		return fmt.Errorf("workflow activation delta: %w", err)
	}
	last := activation.Events[len(activation.Events)-1].ID
	if activation.ThroughEventID != last {
		return fmt.Errorf("workflow activation through event %d does not match delta tail %d", activation.ThroughEventID, last)
	}
	return nil
}

// eventRange 是一段闭区间的 Event ID；零值表示空。
type eventRange struct {
	from, through int64
}

// skip 从增量里去掉区间内已灌入的事件。区间非空时增量必须完整覆盖它，且区间以 WorkflowTaskCompleted 开头。
func (r eventRange) skip(events []history.Event) ([]history.Event, error) {
	if r.from == 0 {
		return events, nil
	}
	if len(events) == 0 || events[len(events)-1].ID < r.through {
		return nil, fmt.Errorf("workflow activation delta does not cover committed events %d-%d", r.from, r.through)
	}
	live := make([]history.Event, 0, len(events))
	for _, event := range events {
		if event.ID < r.from || event.ID > r.through {
			live = append(live, event)
			continue
		}
		if event.ID == r.from && event.WorkflowTaskCompleted == nil {
			return nil, fmt.Errorf("workflow activation delta event %d is not the committed WorkflowTaskCompleted", event.ID)
		}
	}
	return live, nil
}

// endsWithStartedWorkflowTask 报告增量里最后一个 Workflow Task 生命周期事件是否为 WorkflowTaskStarted。
// Started 之后、Worker 取增量之前落库的事实跟在它后面，属于本次激活，与全量路径读到的历史一致。
func endsWithStartedWorkflowTask(events []history.Event) bool {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		switch {
		case event.WorkflowTaskStarted != nil:
			return true
		case event.WorkflowTaskScheduled != nil, event.WorkflowTaskCompleted != nil,
			event.WorkflowTaskFailed != nil, event.WorkflowTaskTimedOut != nil:
			return false
		}
	}
	return false
}

func isWorkflowTaskCommandResultEvent(event history.Event) bool {
	return executor2.IsCommandEvent(event) || isTerminalWorkflowEvent(event)
}

func isTerminalWorkflowEvent(event history.Event) bool {
	return event.WorkflowCompleted != nil || event.WorkflowFailed != nil || event.WorkflowCancelled != nil ||
		event.WorkflowContinuedAsNew != nil
}

func cloneCommands(commands []history.Command) []history.Command {
	cloned := make([]history.Command, len(commands))
	for i, command := range commands {
		cloned[i] = command
		cloned[i].Input = cloneSessionMap(command.Input)
		cloned[i].Output = cloneSessionMap(command.Output)
		if command.RetryPolicy != nil {
			policy := *command.RetryPolicy
			policy.NonRetryableErrors = append([]string(nil), command.RetryPolicy.NonRetryableErrors...)
			policy.NonRetryableErrorTypes = append([]string(nil), command.RetryPolicy.NonRetryableErrorTypes...)
			cloned[i].RetryPolicy = &policy
		}
		if command.UpdateOutcome != nil {
			outcome := *command.UpdateOutcome
			if outcome.Success != nil {
				payload := *outcome.Success
				payload.Data = append([]byte(nil), payload.Data...)
				outcome.Success = &payload
			}
			if outcome.Failure != nil {
				failure := *outcome.Failure
				failure.Details = append(protocol.Payloads(nil), failure.Details...)
				for j := range failure.Details {
					failure.Details[j].Data = append([]byte(nil), failure.Details[j].Data...)
				}
				outcome.Failure = &failure
			}
			cloned[i].UpdateOutcome = &outcome
		}
	}
	return cloned
}

func cloneSessionMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = cloneSessionValue(item)
	}
	return cloned
}

func cloneSessionValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneSessionMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for i, item := range typed {
			cloned[i] = cloneSessionValue(item)
		}
		return cloned
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}

type activationContextGate struct {
	mu       sync.RWMutex
	lifetime context.Context
	active   context.Context
	cancel   context.CancelFunc
	stop     func() bool
}

func newActivationContextGate(lifetime context.Context) *activationContextGate {
	return &activationContextGate{lifetime: lifetime}
}

func (g *activationContextGate) Begin(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := g.lifetime.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active != nil {
		return errors.New("activation context is already installed")
	}
	active, cancel := context.WithCancel(ctx)
	g.active = active
	g.cancel = cancel
	g.stop = context.AfterFunc(g.lifetime, cancel)
	return nil
}

func (g *activationContextGate) End() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stop != nil {
		g.stop()
	}
	if g.cancel != nil {
		g.cancel()
	}
	g.active = nil
	g.cancel = nil
	g.stop = nil
}

func (g *activationContextGate) Deadline() (deadline time.Time, ok bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.active != nil {
		return g.active.Deadline()
	}
	return g.lifetime.Deadline()
}

func (g *activationContextGate) Done() <-chan struct{} {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.active != nil {
		return g.active.Done()
	}
	return g.lifetime.Done()
}

func (g *activationContextGate) Err() error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err := g.lifetime.Err(); err != nil {
		return err
	}
	if g.active != nil {
		return g.active.Err()
	}
	return nil
}

func (g *activationContextGate) Value(key any) any {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.active != nil {
		if value := g.active.Value(key); value != nil {
			return value
		}
	}
	return g.lifetime.Value(key)
}
