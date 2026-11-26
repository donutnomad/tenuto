package interpreter

import (
	"cmp"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

// applyBatch 把一批事件灌入事实表。它只更新事实，协程由调用方推进。
// 每遇到一个 WorkflowTaskStarted 就固定一次 GetInfo 快照（见 snapshotActivationInfo）。
// workflow.Now 的取值不在这里更新：它属于决策批，由 runDecisionBatch 设定（见 batchTime）。
func (e *WorkflowExecutor) applyBatch(events []history.Event) {
	for _, event := range events {
		e.unmeasuredEvents = append(e.unmeasuredEvents, event)
		if started := event.WorkflowTaskStarted; started != nil {
			e.snapshotActivationInfo(started)
		}
	}
	for _, event := range events {
		if event.WorkflowCompleted != nil || event.WorkflowFailed != nil ||
			event.WorkflowCancelled != nil || event.WorkflowTerminated != nil {
			e.terminated = true
		}
		if event.WorkflowCancellationRequested != nil {
			e.cancellationRequested = true
		}
		if ev := event.WorkflowStarted; ev != nil {
			e.input = ev.Input
		} else if ev := event.WorkflowSignaled; ev != nil {
			e.signals[ev.Name] = append(e.signals[ev.Name], ev.Input)
		} else if ev := event.WorkflowUpdateAccepted; ev != nil {
			e.updateAccepted[ev.Name] = append(e.updateAccepted[ev.Name], history.WorkflowUpdateRecord{
				UpdateID:        ev.UpdateID,
				Name:            ev.Name,
				Args:            ev.Args,
				AcceptedEventID: event.ID,
			})
		} else if ev := event.WorkflowUpdateCompleted; ev != nil {
			if ev.UpdateID != "" {
				// 幂等表只记「该 update 已完成」这一事实；结果内容以 Outcome 为准。
				e.updateCompleted[ev.UpdateID] = nil
			}
		} else if ev := event.TimerScheduled; ev != nil {
			e.timersScheduled[ev.TimerID] = event
		} else if ev := event.TimerFired; ev != nil {
			e.timersFired[ev.TimerID] = true
			delete(e.timersScheduled, ev.TimerID)
		} else if ev := event.MarkerRecorded; ev != nil {
			e.markers[markerKey(ev.MarkerName, ev.MarkerID)] = ev.Value
			if ev.MarkerName == executor.VersionMarkerName {
				e.changeVersions[ev.MarkerID] = int64FromMap(ev.Value, "version")
			}
		} else if ev := event.ChildWorkflowStarted; ev != nil {
			e.childStarted[ev.ChildID] = event
		} else if ev := event.ChildWorkflowCompleted; ev != nil {
			childType := ""
			if started := e.childStarted[ev.ChildID].ChildWorkflowStarted; started != nil {
				childType = started.Name
			}
			e.childCompleted[ev.ChildID] = e.normalizeResult(program.PayloadChildResult, childType, ev.Output)
			delete(e.childStarted, ev.ChildID)
			delete(e.childFailures, ev.ChildID)
		} else if ev := event.ChildWorkflowFailed; ev != nil {
			if ev.Failure == nil {
				e.evalErr = fmt.Errorf("ChildWorkflowFailed event %d has no failure payload", event.ID)
				return
			}
			// 子终态失败的权威载荷是 Failure（与 ActivityFailed 同形）；Error 只是它的 message 投影。
			e.childFailures[ev.ChildID] = ev.Failure
			delete(e.childStarted, ev.ChildID)
		} else if ev := event.ChildWorkflowCancelled; ev != nil {
			e.childFailures[ev.ChildID] = canceledFailure(nil)
			delete(e.childStarted, ev.ChildID)
		} else if ev := event.ChildWorkflowTerminated; ev != nil {
			reason := ev.Reason
			if reason == "" {
				reason = "terminated"
			}
			e.childFailures[ev.ChildID] = &protocol.Failure{Message: reason}
			delete(e.childStarted, ev.ChildID)
		} else if ev := event.ActivityScheduled; ev != nil {
			e.bindActivityScheduled(ev.ActivityID, event.ID)
			e.activityTypes[event.ID] = ev.ActivityType
		} else if ev := event.ActivityCompleted; ev != nil {
			e.completed[ev.ScheduledEventID] = e.normalizeResult(program.PayloadActivityResult, cmp.Or(ev.ActivityType, e.activityTypes[ev.ScheduledEventID]), ev.Output)
			delete(e.failed, ev.ScheduledEventID)
		} else if ev := event.ActivityCancelled; ev != nil {
			// 取消确认是 Activity 的终态结果，与失败走同一张事实表：Future 返回取消错误。
			e.failed[ev.ScheduledEventID] = canceledFailure(ev.Details)
		} else if ev := event.ActivityFailed; ev != nil {
			if ev.Failure == nil {
				// 失败必须由 Failure 表达：缺载荷的历史不是「untyped 失败」，而是协议不完整。
				// 明确报错而不是在后续取值处空指针 panic。
				e.evalErr = fmt.Errorf("ActivityFailed event %d has no failure payload", event.ID)
				return
			}
			e.failed[ev.ScheduledEventID] = ev.Failure
		}
	}
}

// snapshotActivationInfo 固定 workflow.GetInfo 在一次 Workflow Task 里的取值：该任务的
// WorkflowTaskStarted 写入时的历史规模。重放这个批时，批里排在它后面的 WorkflowTaskCompleted
// 与命令事件不进入快照，所以重放读到的值与 live 相同。
// Server 在事件里带了统计就直接采用，之前累计的本地度量随之作废；否则用本地度量。
func (e *WorkflowExecutor) snapshotActivationInfo(started *history.WorkflowTaskStartedEvent) {
	if started.HistoryEventCount > 0 {
		e.historyMetrics = history.HistoryMetrics{EventCount: started.HistoryEventCount, SizeBytes: started.HistorySizeBytes}
		e.unmeasuredEvents = nil
		e.setActivationInfo(e.historyMetrics, started.ContinueAsNewReasons)
		return
	}
	e.captureActivationInfo()
}

// captureActivationInfo 用本地度量生成 GetInfo 快照。事件的规范化大小在这里才计算：
// 历史带 Server 统计时没有事件需要度量。
func (e *WorkflowExecutor) captureActivationInfo() {
	for _, event := range e.unmeasuredEvents {
		size, err := history.CanonicalEventSize(event)
		if err != nil {
			e.evalErr = fmt.Errorf("measure workflow history event %d: %w", event.ID, err)
			return
		}
		e.historyMetrics.EventCount++
		e.historyMetrics.SizeBytes += size
	}
	e.unmeasuredEvents = nil
	reasons, err := history.ContinueAsNewReasonsForMetrics(e.historyMetrics, history.DefaultHistoryLimits())
	if err != nil {
		e.evalErr = err
		return
	}
	e.setActivationInfo(e.historyMetrics, reasons)
}

func (e *WorkflowExecutor) setActivationInfo(metrics history.HistoryMetrics, reasons []protocol.ContinueAsNewReason) {
	e.activationInfo = workflow.Info{
		CurrentHistoryLength: metrics.EventCount,
		CurrentHistorySize:   metrics.SizeBytes,
		ContinueAsNewReasons: append([]workflow.ContinueAsNewReason(nil), reasons...),
	}
}

// batchTime 返回一个决策批里 workflow.Now 的取值：批内最后一个 WorkflowTaskStarted 的时间戳，与 Temporal
// 取 WorkflowTaskStarted 时间一致。live 执行时批尾就是这个事件；同一个批提交之后多出的 WorkflowTaskCompleted
// 与命令事件（时间是提交时刻）排在它后面，重放时不影响取值，所以 live 与重放读到同一个值。
// 没有 WorkflowTaskStarted 的批（裸历史）取批内最后一个带时间戳的事件。ok 为 false 时批内没有时间戳，沿用原值。
func batchTime(batch []history.Event) (time.Time, bool) {
	var last time.Time
	for i := len(batch) - 1; i >= 0; i-- {
		event := batch[i]
		if event.Timestamp.IsZero() {
			continue
		}
		if event.WorkflowTaskStarted != nil {
			return event.Timestamp, true
		}
		if last.IsZero() {
			last = event.Timestamp
		}
	}
	return last, !last.IsZero()
}

func (e *WorkflowExecutor) marker(name, id string) (map[string]any, bool) {
	value, ok := e.markers[markerKey(name, id)]
	return value, ok
}

// recordMarker 产出 CmdRecordMarker 并立即写入 e.markers，同一 Workflow Task 内后续读取
// 直接命中。
func (e *WorkflowExecutor) recordMarker(name, id string, input map[string]any) {
	e.markers[markerKey(name, id)] = input
	e.emitCommand(history.Command{
		Type:       history.CmdRecordMarker,
		MarkerName: name,
		MarkerID:   id,
		Input:      input,
	})
}

// shadowMarker 在 replay 批读回已有 marker 时补一条 RecordMarker 命令，与历史里的
// MarkerRecorded 事件配对。同一个 marker 只补一次。
func (e *WorkflowExecutor) shadowMarker(name, id string, value map[string]any) {
	key := markerKey(name, id)
	if e.shadowedMarkers[key] {
		return
	}
	e.shadowedMarkers[key] = true
	e.emitShadow(history.Command{
		Type:       history.CmdRecordMarker,
		MarkerName: name,
		MarkerID:   id,
		Input:      value,
	})
}

// canceledFailure 是 Activity 确认取消与子 Workflow 以 Cancelled 结束时交给 Workflow 代码的失败：
// 还原成取消错误，IsCanceledError 为 true。
//
// ActivityCancelRequested、TimerCancelled、ChildWorkflowCancelRequested 是命令事件，不进事实表：
// 取消命令由代码执行到取消点时产出，重放时同样产出并与这些事件比对。
func canceledFailure(details protocol.Payloads) *protocol.Failure {
	return &protocol.Failure{Message: "canceled", Type: apperror.CanceledType, NonRetryable: true, Details: details}
}

func markerKey(name, id string) string {
	return name + ":" + id
}
