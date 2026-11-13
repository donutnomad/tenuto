package history

import "fmt"

// DedupeKey 从事件内容推导其"内容唯一"去重维度，是这类 first-wins 幂等的唯一定义点。
//
// ActivityScheduled 与 ActivityCancelRequested 没有去重维度：ActivityID 在前一个 Activity 关闭后可以再用，
// 同一 ID 可以有多个调度与取消请求。它们是 Workflow Task 的命令事件，只在 WorkflowTaskCompleted 本次
// 插入时追加，重复提交在关闭事件上去重。
func DedupeKey(event Event) (string, bool) {
	typ := event.EventType()
	switch {
	case typ == EventActivityStarted && event.ActivityStarted != nil:
		return fmt.Sprintf("activity_started:%d:%d:%d", event.ActivityStarted.ScheduledEventID, event.ActivityStarted.Stamp, NormalizeActivityAttempt(event.ActivityStarted.Attempt)), true
	case IsActivityAttemptEventType(typ):
		attempt, scheduledEventID, stamp := activityAttemptDedupeFields(event)
		return fmt.Sprintf("activity_attempt:%s:%d:%d:%d", typ, scheduledEventID, stamp, NormalizeActivityAttempt(attempt)), true
	case IsActivityCloseEventType(typ):
		return fmt.Sprintf("activity_closed:%d", activityCloseScheduledEventID(event)), true
	case typ == EventWorkflowTaskStarted && event.WorkflowTaskStarted != nil:
		return fmt.Sprintf("workflow_task_started:%d", event.WorkflowTaskStarted.ScheduledEventID), true
	case IsWorkflowTerminalEventType(typ):
		return "workflow_terminal", true
	case typ == EventTimerScheduled && event.TimerScheduled != nil:
		return "timer_scheduled:" + event.TimerScheduled.TimerID, true
	case typ == EventTimerCancelled && event.TimerCancelled != nil:
		return "timer_cancelled:" + event.TimerCancelled.TimerID, true
	case typ == EventChildWorkflowCancelRequested && event.ChildWorkflowCancelRequested != nil:
		return "child_workflow_cancel_requested:" + event.ChildWorkflowCancelRequested.ChildID, true
	case typ == EventMarkerRecorded && event.MarkerRecorded != nil:
		return fmt.Sprintf("marker_recorded:%s:%s", event.MarkerRecorded.MarkerName, event.MarkerRecorded.MarkerID), true
	case typ == EventWorkflowUpdateAccepted && event.WorkflowUpdateAccepted != nil && event.WorkflowUpdateAccepted.UpdateID != "":
		// updateID 幂等的存储层兜底：同一 updateID 的 accepted 只落一条。挡住"lease 前快照后、
		// Append 前"的并发窗口（快照检查漏网时，DedupeKey first-wins 保证历史恰好一条 accepted）。
		return "workflow_update_accepted:" + event.WorkflowUpdateAccepted.UpdateID, true
	case typ == EventWorkflowUpdateCompleted && event.WorkflowUpdateCompleted != nil && event.WorkflowUpdateCompleted.UpdateID != "":
		return "workflow_update_completed:" + event.WorkflowUpdateCompleted.UpdateID, true
	case typ == EventChildWorkflowStarted && event.ChildWorkflowStarted != nil:
		return "child_workflow_started:" + event.ChildWorkflowStarted.ChildID, true
	case typ == EventWorkflowSignaled && event.WorkflowSignaled != nil && event.WorkflowSignaled.RequestID != "":
		return "workflow_signaled:" + event.WorkflowSignaled.RequestID, true
	default:
		return "", false
	}
}

func activityAttemptDedupeFields(event Event) (attempt int, scheduledEventID int64, stamp int64) {
	if e := event.ActivityAttemptFailed; e != nil {
		return e.Attempt, e.ScheduledEventID, e.Stamp
	}
	if e := event.ActivityTimedOut; e != nil {
		return e.Attempt, e.ScheduledEventID, e.Stamp
	}
	return 0, 0, 0
}

func activityCloseScheduledEventID(event Event) int64 {
	if e := event.ActivityCompleted; e != nil {
		return e.ScheduledEventID
	}
	if e := event.ActivityFailed; e != nil {
		return e.ScheduledEventID
	}
	if e := event.ActivityCancelled; e != nil {
		return e.ScheduledEventID
	}
	return 0
}

// NormalizeActivityAttempt 把未赋值的尝试序号归一成第 1 次。
func NormalizeActivityAttempt(attempt int) int {
	if attempt < 1 {
		return 1
	}
	return attempt
}
