package execution

import (
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// NormalizeEventFailures 校验并 canonical 化事件自带的失败载荷。所有写历史的边界共用这一份
// 实现：gorm 落库与 memory 后端因此接受同一份载荷范围（既有 payload 数量与字节上限）并产出
// 同一份 canonical detail 编码。
//
// 语义约束：
//   - 失败类事件（ActivityAttemptFailed / ActivityFailed / ActivityTimedOut / WorkflowFailed /
//     ChildWorkflowFailed）必须携带 Failure，缺失是协议错误，不以字符串回退补造；
//   - WorkflowUpdateCompleted 必须携带 Outcome，且 Success/Failure 恰好一个；
//   - 失败事件的 Error（ActivityTimedOut 是 Reason）统一改写为 Failure.Message：
//     它是投影列，字符串消费方必须与结构化载荷读到同一个文本。
//
// 事件是值传递，但它携带的指针（*ActivityFailedEvent、*UpdateOutcome ...）与调用方共享。
// 因此这里对每个承载失败的 struct 先做浅拷贝再写回，调用方对象在任何情况下都不会被改动；
// 校验失败返回错误，调用方在写状态/事件之前原样返回。
func NormalizeEventFailures(event history.Event) (history.Event, error) {
	normalize := func(failure **protocol.Failure) error {
		normalized, err := (*failure).Normalize()
		if err != nil {
			return err
		}
		*failure = &normalized
		return nil
	}
	requireFailure := func(failure **protocol.Failure, kind string) error {
		if *failure == nil {
			return protocol.InvalidRequestf("%s requires a failure payload", kind)
		}
		return normalize(failure)
	}
	wrap := func(err error) (history.Event, error) {
		return history.Event{}, protocol.InvalidRequestf("normalize %s failure payload: %w", event.EventType(), err)
	}

	if failed := event.ActivityAttemptFailed; failed != nil {
		cloned := *failed
		if err := requireFailure(&cloned.Failure, "activity attempt failed"); err != nil {
			return wrap(err)
		}
		cloned.Error = cloned.Failure.Message
		event.ActivityAttemptFailed = &cloned
	}
	if failed := event.ActivityFailed; failed != nil {
		cloned := *failed
		if err := requireFailure(&cloned.Failure, "activity failed"); err != nil {
			return wrap(err)
		}
		cloned.Error = cloned.Failure.Message
		event.ActivityFailed = &cloned
	}
	if timedOut := event.ActivityTimedOut; timedOut != nil {
		cloned := *timedOut
		if err := requireFailure(&cloned.Failure, "activity timed out"); err != nil {
			return wrap(err)
		}
		event.ActivityTimedOut = &cloned
	}
	if failed := event.WorkflowFailed; failed != nil {
		cloned := *failed
		if err := requireFailure(&cloned.Failure, "workflow failed"); err != nil {
			return wrap(err)
		}
		cloned.Error = cloned.Failure.Message
		event.WorkflowFailed = &cloned
	}
	if failed := event.ChildWorkflowFailed; failed != nil {
		cloned := *failed
		if err := requireFailure(&cloned.Failure, "child workflow failed"); err != nil {
			return wrap(err)
		}
		cloned.Error = cloned.Failure.Message
		event.ChildWorkflowFailed = &cloned
	}
	if accepted := event.WorkflowUpdateAccepted; accepted != nil && accepted.Args != nil {
		cloned := *accepted
		args, err := protocol.NormalizePayloads(*cloned.Args)
		if err != nil {
			return wrap(err)
		}
		cloned.Args = &args
		event.WorkflowUpdateAccepted = &cloned
	}
	if completed := event.WorkflowUpdateCompleted; completed != nil {
		cloned := *completed
		if cloned.Outcome == nil {
			return wrap(protocol.InvalidRequestf("workflow update completed requires an outcome"))
		}
		outcome := *cloned.Outcome
		if err := outcome.Validate(); err != nil {
			return wrap(err)
		}
		normalizedOutcome, err := protocol.NormalizeUpdateOutcome(outcome)
		if err != nil {
			return wrap(err)
		}
		cloned.Outcome = &normalizedOutcome
		event.WorkflowUpdateCompleted = &cloned
	}
	return event, nil
}
