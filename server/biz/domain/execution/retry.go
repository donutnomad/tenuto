package execution

import (
	"math"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// NonRetryableFailure 判定一次失败是否被显式策略拦下。三条策略互相独立，任一命中即不重试：
//
//  1. Failure.NonRetryable 显式声明；
//  2. Failure.Type 与 errorTypes 中某项完全相等（大小写敏感，非子串、非前缀）；
//  3. Failure.Message 包含 messagePatterns 中某项（既有子串语义）。
//
// 只看顶层 Failure：fmt.Errorf 包装后的顶层 type 是 wrapError，两条列表策略都不命中，
// 内层 flag 也不会被提升，与 Temporal 的顶层判定一致。
func NonRetryableFailure(failure protocol.Failure, errorTypes, messagePatterns []string) bool {
	if failure.NonRetryable {
		return true
	}
	for _, errorType := range errorTypes {
		if failure.Type == errorType {
			return true
		}
	}
	for _, pattern := range messagePatterns {
		if strings.Contains(failure.Message, pattern) {
			return true
		}
	}
	return false
}

// DecideWorkflowRetry applies the Run's retry policy against a fixed clock.
func DecideWorkflowRetry(run *Run, policy *protocol.RetryPolicy, failure protocol.Failure, now time.Time) (bool, time.Duration, string) {
	if policy == nil {
		return false, 0, "RetryPolicyNotSet"
	}
	if NonRetryableFailure(failure, policy.NonRetryableErrorTypes, policy.NonRetryableErrors) {
		return false, 0, "NonRetryableFailure"
	}
	attempt := run.Attempt
	if policy.MaxAttempts > 0 && attempt+1 > policy.MaxAttempts {
		return false, 0, "MaximumAttemptsReached"
	}
	backoff := WorkflowRetryBackoff(policy, attempt)
	if run.DeadlineAt != nil && now.Add(backoff).After(*run.DeadlineAt) {
		return false, 0, "Timeout"
	}
	return true, backoff, "InProgress"
}

// Activity 的缺省重试策略与 Temporal 相同：InitialInterval 1s、BackoffCoefficient 2.0、
// MaxInterval 为 100 倍 InitialInterval、MaxAttempts 0（不限次数）、没有不可重试的错误类型。
// Workflow 级重试的缺省间隔沿用同一组值；Workflow 只在配置了重试策略时重试。
const (
	defaultRetryInitialInterval    = time.Second
	defaultRetryBackoffCoefficient = 2.0
	defaultRetryMaxIntervalFactor  = 100
)

// EffectiveActivityRetryPolicy 返回 Activity 实际生效的重试策略：没有策略时取缺省策略，部分设置的
// 策略按缺省值补齐未设置（零值）的字段。调度 Activity 时写入 ActivityScheduled 事件与 ActivityState
// 的就是它；Server 对 Activity 重试的一切判断都读它。
func EffectiveActivityRetryPolicy(policy *history.RetryPolicy) *history.RetryPolicy {
	var effective history.RetryPolicy
	if policy != nil {
		effective = *policy
		effective.NonRetryableErrors = append([]string(nil), policy.NonRetryableErrors...)
		effective.NonRetryableErrorTypes = append([]string(nil), policy.NonRetryableErrorTypes...)
	}
	if effective.InitialIntervalMs <= 0 {
		effective.InitialIntervalMs = defaultRetryInitialInterval.Milliseconds()
	}
	if effective.BackoffCoefficient == 0 {
		effective.BackoffCoefficient = defaultRetryBackoffCoefficient
	}
	if effective.MaxIntervalMs <= 0 {
		effective.MaxIntervalMs = defaultRetryMaxIntervalFactor * effective.InitialIntervalMs
	}
	return &effective
}

// activityRetryPolicy 返回 command 调度的 Activity 的生效重试策略。
func activityRetryPolicy(command history.Command) *history.RetryPolicy {
	return EffectiveActivityRetryPolicy(history.ToRetryPolicy(command.RetryPolicy))
}

// WorkflowRetryBackoff uses the current completed attempt, starting at one.
func WorkflowRetryBackoff(policy *protocol.RetryPolicy, currentAttempt int) time.Duration {
	initial := retryDurationOr(policy.InitialInterval, defaultRetryInitialInterval)
	maxInterval := retryDurationOr(policy.MaxInterval, defaultRetryMaxIntervalFactor*initial)
	return backoffInterval(initial, policy.BackoffCoefficient, maxInterval, currentAttempt-1)
}

// activityRetryDelay 返回第 nextAttempt 次尝试开始前的等待，nextAttempt 是失败后已递增的尝试序号：
// 首次重试（nextAttempt=2）等 InitialInterval，之后每次乘 BackoffCoefficient，封顶 MaxInterval。
func activityRetryDelay(policy *history.RetryPolicy, nextAttempt int) time.Duration {
	effective := EffectiveActivityRetryPolicy(policy)
	return backoffInterval(
		time.Duration(effective.InitialIntervalMs)*time.Millisecond,
		effective.BackoffCoefficient,
		time.Duration(effective.MaxIntervalMs)*time.Millisecond,
		nextAttempt-2,
	)
}

// backoffInterval 计算 initial × coefficient^retries，封顶 maxInterval。比较在浮点域完成：
// 指数溢出得到的 +Inf 直接落到上限，不会先被截成负的 Duration。
func backoffInterval(initial time.Duration, coefficient float64, maxInterval time.Duration, retries int) time.Duration {
	if coefficient < 1 {
		coefficient = 2
	}
	interval := float64(initial) * math.Pow(coefficient, float64(max(retries, 0)))
	if interval > 0 && interval <= float64(maxInterval) {
		return time.Duration(interval)
	}
	return maxInterval
}

func retryDurationOr(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return fallback
	}
	return duration
}

// NewRetryRun carries the pinned definition and execution deadline into the
// next attempt while starting a fresh History branch. startAttrs are the old
// Run's search attributes at start: values written while it ran, including the
// GetVersion decisions, belong to the old attempt.
func NewRetryRun(old *Run, id string, now time.Time, startAttrs map[string]string) *Run {
	next := NewRun(NewRunOptions{
		ID: id, Namespace: old.Namespace, WorkflowID: old.WorkflowID,
		WorkflowType: old.WorkflowType, WorkflowConfigVersion: old.WorkflowConfigVersion,
		FirstRunID: old.FirstRunID, ContinuedRunID: old.ID, Input: old.Input,
		SearchAttributes: cloneStringMap(startAttrs), DeadlineAt: old.DeadlineAt,
		Attempt: old.Attempt + 1, Initiator: InitiatorEnums.Retry,
		// 子工作流的重试 run 沿用父关联：最后一次尝试的终态才传播给父 run。
		ParentRunID: old.ParentRunID, ParentChildID: old.ParentChildID,
		ParentScheduledEventID: old.ParentScheduledEventID,
		CreatedAt:              now,
	})
	inheritRunUpgradeState(next, old)
	return next
}

// NewRetryBackoffTimer 返回推迟重试 Run 首个 Workflow Task 的持久化 timer，在 now 之后 backoff 到期。
// RetryBackoff timer 没有 TimerScheduled 事件，ScheduledEventID 只作复合主键：重试 Run 只有这一个 RetryBackoff
// timer，取 now 的纳秒时间戳。
func NewRetryBackoffTimer(retryRunID string, backoff time.Duration, now time.Time, traceContext map[string]string) *TimerState {
	return &TimerState{
		RunID: retryRunID, ScheduledEventID: now.UnixNano(),
		Kind: TimerKindEnums.RetryBackoff, Deadline: now.Add(backoff),
		Status: TimerStatusEnums.Scheduled, TraceContext: traceContext,
		CreatedAt: now, UpdatedAt: now,
	}
}

type RetryRunCommit struct {
	OldRunID     string
	OldTreeID    string
	OldBranchID  string
	NewRun       *Run
	StartedEvent history.Event
	TimerState   *TimerState
	// Failure 是本次失败的完整载荷；Event/Error 与它保持一致。
	Failure                protocol.Failure
	TimeoutType            string
	RetryState             string
	FailureAlreadyAppended bool
}

// CommitRetryRun atomically closes the old Run, moves the current pointer,
// starts the next Run, and persists its backoff timer. The old Run is read
// with GetRunForUpdate in this transaction; the next Run takes its effective
// policy from that read.
func CommitRetryRun(tx ExecutionTx, request RetryRunCommit) (bool, error) {
	if !request.FailureAlreadyAppended {
		failed, err := history.NewWorkflowFailedFailureEvent(request.Failure, request.TimeoutType)
		if err != nil {
			return false, err
		}
		failed.WorkflowFailed.NewExecutionRunID = request.NewRun.ID
		failed.WorkflowFailed.RetryState = request.RetryState
		_, inserted, err := tx.AppendIdempotent(request.OldRunID, request.OldTreeID, request.OldBranchID, failed)
		if err != nil || !inserted {
			return false, err
		}
	}
	failedRun, err := tx.GetRunForUpdate(request.OldRunID)
	if err != nil {
		return false, err
	}
	request.NewRun.inheritPolicy(failedRun)
	if err := closeRun(tx, failedRun, history.StatusFailed, nil); err != nil {
		return false, err
	}
	if err := CommitStartedRun(tx, request.NewRun, request.OldRunID, request.NewRun.ID, request.NewRun.ID, request.StartedEvent); err != nil {
		return false, err
	}
	if err := tx.UpsertTimerState(request.TimerState); err != nil {
		return false, err
	}
	return true, nil
}
