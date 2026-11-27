package executor

import (
	"context"
	"errors"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type IncrementalTaskFailure uint8

const (
	IncrementalTaskReadFailure IncrementalTaskFailure = iota + 1
	IncrementalTaskExecutionFailure
	IncrementalTaskCompletionFailure
	IncrementalTaskCacheFailure
	// IncrementalTaskSessionFailure：session/activation/history 校验失败（ErrExecutionSessionInvalid）。
	IncrementalTaskSessionFailure
	// IncrementalTaskNotSelected：cache miss 后读到的配置或 history 不走增量路径，由 full-history 路径执行。
	IncrementalTaskNotSelected
)

// ErrIncrementalNotSelected 由 LoadWorkflow 返回，表示该配置不走增量路径。
var ErrIncrementalNotSelected = errors.New("workflow config is not selected for incremental execution")

type IncrementalTaskResult struct {
	Failure IncrementalTaskFailure
	Err     error
}

type IncrementalTaskOptions struct {
	Cache        *ExecutionCache
	Key          ExecutionCacheKey
	Capabilities []string
	// Workflow 是要执行的配置。为 nil 时由 LoadWorkflow 在 cache miss 上读取；
	// cache hit 复用 session，不需要配置。
	Workflow      *program.WorkflowConfig
	LoadWorkflow  func(context.Context) (*program.WorkflowConfig, error)
	NewSession    func(context.Context, *program.WorkflowConfig, int64) ExecutionSession
	NewHistory    func([]history.Event) HistoryIterator
	ReadEvents    func(context.Context, string) ([]history.Event, error)
	GetActivation func(context.Context, protocol.Task, int64, int64, string) (history.WorkflowActivation, error)
	Complete      func(context.Context, protocol.Task, history.ActivationResult) (history.WorkflowTaskCompletionAck, error)
	ActivationID  string
}

// RunIncrementalTask 在 sticky execution cache 上执行一次 Workflow Task：取得 session、
// 执行 activation、把结果提交给 Server，再推进 session 游标。失败上报与队列确认由调用方负责。
//
// 主流程：
//
//	Acquire(key) ── hit ──▶ GetActivation(session 游标之后的增量)
//	      └────── miss ─▶ 读 definition 与完整历史 → NewSession → Restore(已提交前缀)
//	                      → 未提交尾部作为本次 activation
//	Activate ─▶ Complete(Server 落库) ─▶ Commit(ack 里的权威事件) ─▶ Admit / 更新 cache 元数据
//
// 不变式：
//   - cache 里的 session 游标总是停在已提交的 Workflow Task 边界；Activate 之后、Commit 之前的
//     session 领先于 history，只存在于本函数的一次调用内。
//   - 任何一步失败，或 Server 未采纳这批命令，session 都被关闭并逐出；下一次 Workflow Task
//     从 Event History cold restore。
//   - generation 或 worker build 与 entry 元数据不一致时，entry 被逐出并按 miss 处理。
//   - Run 进入终态后 session 关闭并逐出。
func RunIncrementalTask(ctx, runtimeCtx context.Context, task protocol.Task, opts IncrementalTaskOptions) IncrementalTaskResult {
	if opts.Cache == nil || opts.NewSession == nil || opts.NewHistory == nil || opts.ReadEvents == nil || opts.GetActivation == nil || opts.Complete == nil {
		return IncrementalTaskResult{Failure: IncrementalTaskCacheFailure, Err: errors.New("incomplete incremental task options")}
	}
	generation := task.ExecutionGeneration

	var session ExecutionSession
	entry, err := opts.Cache.Acquire(opts.Key)
	miss := errors.Is(err, ErrExecutionCacheEntryMissing)
	if err != nil && !miss {
		return IncrementalTaskResult{Failure: IncrementalTaskCacheFailure, Err: err}
	}
	defer func() {
		if entry != nil {
			entry.Release()
		}
	}()
	// A lease must be released before invalidation can close its session.
	evict := func(reason protocol.ExecutionCacheEvictionReason) {
		if entry != nil {
			entry.Release()
			entry = nil
		}
		_ = opts.Cache.Invalidate(opts.Key, reason)
	}
	if entry != nil {
		metadata := entry.Metadata()
		if metadata.ExecutionGeneration != generation || metadata.WorkerBuildID != task.WorkerBuildID {
			evict(protocol.ExecutionCacheEvictionGeneration)
			miss = true
		} else {
			var ok bool
			session, ok = entry.Session().(ExecutionSession)
			if !ok {
				evict(protocol.ExecutionCacheEvictionInvalid)
				return IncrementalTaskResult{Failure: IncrementalTaskCacheFailure, Err: errors.New("execution cache entry has incompatible session type")}
			}
		}
	}

	var activation history.WorkflowActivation
	if miss {
		workflow := opts.Workflow
		if workflow == nil && opts.LoadWorkflow != nil {
			var loadErr error
			workflow, loadErr = opts.LoadWorkflow(ctx)
			if errors.Is(loadErr, ErrIncrementalNotSelected) {
				return IncrementalTaskResult{Failure: IncrementalTaskNotSelected, Err: loadErr}
			}
			if loadErr != nil {
				return IncrementalTaskResult{Failure: IncrementalTaskReadFailure, Err: loadErr}
			}
		}
		events, readErr := opts.ReadEvents(ctx, task.RunID)
		if readErr != nil {
			return IncrementalTaskResult{Failure: IncrementalTaskReadFailure, Err: readErr}
		}
		restoreEvents, activationEvents, live := SplitColdRestoreHistory(events)
		// 归批规则以 SplitBatches 为准。命令事件与它的 WorkflowTaskCompleted 之间隔着外部事件时，
		// 连续切分得到的 activation 比最后一批多出这些命令事件；这种 history 交给 full-history 路径。
		if batches := SplitBatches(events); len(batches[len(batches)-1]) != len(live) {
			return IncrementalTaskResult{Failure: IncrementalTaskNotSelected, Err: ErrIncrementalNotSelected}
		}
		session = opts.NewSession(context.Background(), workflow, generation)
		if _, restoreErr := session.Restore(ctx, opts.NewHistory(restoreEvents)); restoreErr != nil {
			_ = session.Close()
			return IncrementalTaskResult{Failure: sessionFailureKind(restoreErr), Err: restoreErr}
		}
		activation = history.WorkflowActivation{
			ActivationID:        opts.ActivationID,
			ExecutionGeneration: generation,
			Events:              activationEvents,
		}
		if len(activationEvents) > 0 {
			activation.ThroughEventID = activationEvents[len(activationEvents)-1].ID
		}
	} else {
		activation, err = opts.GetActivation(ctx, task, session.AppliedThroughEventID(), generation, opts.ActivationID)
		if err != nil {
			evict(protocol.ExecutionCacheEvictionInvalid)
			return IncrementalTaskResult{Failure: IncrementalTaskReadFailure, Err: err}
		}
	}

	result, err := session.Activate(ctx, activation)
	if err != nil {
		if entry != nil {
			evict(protocol.ExecutionCacheEvictionInvalid)
		} else {
			_ = session.Close()
		}
		return IncrementalTaskResult{Failure: sessionFailureKind(err), Err: err}
	}
	err = protocol.CheckWorkflowTaskCommandsSize(result.Commands)
	var ack history.WorkflowTaskCompletionAck
	if err == nil {
		ack, err = opts.Complete(runtimeCtx, task, result)
	}
	if err != nil {
		_ = session.Close()
		evict(protocol.ExecutionCacheEvictionInvalid)
		return IncrementalTaskResult{Failure: IncrementalTaskCompletionFailure, Err: err}
	}
	if !ack.Accepted {
		// 服务端已关闭本次 WFT 且未采纳这批命令：确认 lease，丢弃已领先于 history 的 session。
		_ = session.Close()
		evict(protocol.ExecutionCacheEvictionInvalid)
		return IncrementalTaskResult{}
	}
	if err := session.Commit(ack); err != nil {
		evict(protocol.ExecutionCacheEvictionInvalid)
		_ = session.Close()
		return IncrementalTaskResult{Failure: IncrementalTaskCacheFailure, Err: err}
	}
	if ack.Terminal {
		evict(protocol.ExecutionCacheEvictionInvalid)
		return IncrementalTaskResult{}
	}
	metadata := ExecutionCacheEntryMetadata{
		ExecutionGeneration: generation,
		StickyGeneration:    ack.StickyGeneration,
		WorkerBuildID:       task.WorkerBuildID,
		Capabilities:        append([]string(nil), opts.Capabilities...),
	}
	if entry != nil {
		entry.UpdateMetadata(metadata)
	} else if err := opts.Cache.Admit(opts.Key, session, metadata); err != nil {
		// Server 已提交本次 Workflow Task：进不了缓存只意味着下一次走 cold restore，任务本身已完成。
		_ = session.Close()
	}
	return IncrementalTaskResult{}
}

// sessionFailureKind 区分 session 校验失败与 workflow 执行失败：前者重试，后者进入 Fail/Block。
func sessionFailureKind(err error) IncrementalTaskFailure {
	if errors.Is(err, ErrExecutionSessionInvalid) {
		return IncrementalTaskSessionFailure
	}
	return IncrementalTaskExecutionFailure
}

// SplitColdRestoreHistory 把完整历史切成 cold restore 的两段输入：最后一个已完成 Workflow Task
// 及紧随其后的命令事件为止是已提交前缀（交给 Restore），activation 是 Restore 之后游标后面的全部事件。
// 两段各自保持 Event ID 连续，这是 Restore / Activate 的输入要求。
//
// 最后一个 Workflow Task 进行期间落库的事实排在它的 WorkflowTaskCompleted 之前、ID 大于 SeenThroughEventID：
// 它们属于下一批，Restore 的游标停在它们之前，activation 从它们开始，包含 Restore 已灌入的 WorkflowTaskCompleted
// 与命令事件，session 激活时跳过这一段。live 是去掉这一段之后 session 实际执行的事件；
// 命令事件紧跟各自的 WorkflowTaskCompleted 时，它与 SplitBatches 的最后一批一致，RunIncrementalTask 只在两者一致时使用这个切分。
func SplitColdRestoreHistory(events []history.Event) (restore, activation, live []history.Event) {
	lastCompleted := -1
	for index, event := range events {
		if event.WorkflowTaskCompleted != nil {
			lastCompleted = index
		}
	}
	if lastCompleted < 0 {
		all := append([]history.Event(nil), events...)
		return nil, all, all
	}
	boundary := lastCompleted + 1
	for boundary < len(events) && IsCommandEvent(events[boundary]) {
		boundary++
	}
	start := lastCompleted
	if seen := events[lastCompleted].WorkflowTaskCompleted.SeenThroughEventID; seen > 0 {
		for start > 0 && events[start-1].ID > seen {
			start--
		}
	}
	restore = append([]history.Event(nil), events[:boundary]...)
	if start == lastCompleted {
		activation = append([]history.Event(nil), events[boundary:]...)
		return restore, activation, activation
	}
	activation = append([]history.Event(nil), events[start:]...)
	live = append(append([]history.Event(nil), events[start:lastCompleted]...), events[boundary:]...)
	return restore, activation, live
}
