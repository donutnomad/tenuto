package execution

import (
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type WorkflowTargetRef struct {
	Namespace    string
	WorkflowType string
	Version      string
}

// WorkflowCommandResolver lets the Run layer request routing and immutable
// definition refs without depending on the server composition package.
type WorkflowCommandResolver interface {
	ResolveActivityCommand(runID string, command history.Command) (history.Command, error)
	ChildWorkflowRef(runID, workflowType string) (WorkflowTargetRef, error)
	ContinuedWorkflowRef(runID, workflowType string) (WorkflowTargetRef, error)
	CheckWorkflowInput(ref WorkflowTargetRef, input map[string]any) error
}

// ErrBadScheduleActivityAttributes 表示 Workflow 发出的调度 Activity 命令缺少必填属性或属性超长。
var ErrBadScheduleActivityAttributes = errors.New("bad schedule activity attributes")

// WorkflowTaskFailedCauseBadScheduleActivityAttributes 是调度 Activity 命令被拒绝时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseBadScheduleActivityAttributes = "bad_schedule_activity_attributes"

// ErrBadStartTimerAttributes 表示 Workflow 发出的 Timer 命令属性不合法（TimerID 超长）。
var ErrBadStartTimerAttributes = errors.New("bad start timer attributes")

// WorkflowTaskFailedCauseBadStartTimerAttributes 是 Timer 命令被拒绝时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseBadStartTimerAttributes = "bad_start_timer_attributes"

// ErrBadStartChildWorkflowAttributes 表示 Workflow 发出的启动子 Workflow 命令属性不合法（子 Workflow ID 或 Workflow Type 超长）。
var ErrBadStartChildWorkflowAttributes = errors.New("bad start child workflow attributes")

// WorkflowTaskFailedCauseBadStartChildWorkflowAttributes 是启动子 Workflow 命令被拒绝时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseBadStartChildWorkflowAttributes = "bad_start_child_workflow_attributes"

// ErrBadRecordMarkerAttributes 表示 Workflow 发出的 RecordMarker 命令属性不合法（marker 名或 ID 超长）。
var ErrBadRecordMarkerAttributes = errors.New("bad record marker attributes")

// WorkflowTaskFailedCauseBadRecordMarkerAttributes 是 RecordMarker 命令被拒绝时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseBadRecordMarkerAttributes = "bad_record_marker_attributes"

// BadCommandAttributesCause 返回命令校验错误（ValidateWorkflowCommand 的结果）对应的 WorkflowTaskFailed cause。
// 这类错误由 Workflow 代码决定，重试同一批命令得到同样的结果：Workflow Task 记为失败，Run 显示 Blocked。
func BadCommandAttributesCause(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrBadScheduleActivityAttributes):
		return WorkflowTaskFailedCauseBadScheduleActivityAttributes, true
	case errors.Is(err, ErrBadStartTimerAttributes):
		return WorkflowTaskFailedCauseBadStartTimerAttributes, true
	case errors.Is(err, ErrBadStartChildWorkflowAttributes):
		return WorkflowTaskFailedCauseBadStartChildWorkflowAttributes, true
	case errors.Is(err, ErrBadRecordMarkerAttributes):
		return WorkflowTaskFailedCauseBadRecordMarkerAttributes, true
	}
	return "", false
}

// ErrScheduleActivityDuplicateID 表示调度 Activity 命令的 ActivityID 与本 Run 里仍未关闭的 Activity 重复。
var ErrScheduleActivityDuplicateID = errors.New("schedule activity duplicate id")

// WorkflowTaskFailedCauseScheduleActivityDuplicateID 是 ActivityID 重复时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseScheduleActivityDuplicateID = "schedule_activity_duplicate_id"

// CheckCommandPayloadSize 校验命令携带的载荷（Activity、子 Workflow 与 Continue-As-New 的输入，Marker 数据，
// Workflow 结果，Update 结果）不超过 limit 字节；limit <= 0 表示不检查。超限时返回包装 protocol.ErrPayloadTooLarge
// 的错误。
func CheckCommandPayloadSize(command history.Command, limit int) error {
	what, payload := "", any(nil)
	switch command.Type {
	case history.CmdScheduleActivity:
		what, payload = fmt.Sprintf("input of activity %q (type %s)", command.ActivityID, command.ActivityType), command.Input
	case history.CmdStartChildWorkflow:
		what, payload = fmt.Sprintf("input of child workflow %q (type %s)", command.ChildID, command.WorkflowType), command.Input
	case history.CmdContinueAsNew:
		what, payload = "continue-as-new input", command.Input
	case history.CmdRecordMarker:
		what, payload = fmt.Sprintf("data of marker %q", command.MarkerName), command.Input
	case history.CmdCompleteWorkflow:
		what, payload = "workflow result", command.Output
	case history.CmdCompleteWorkflowUpdate:
		if command.UpdateOutcome == nil {
			return nil
		}
		what = fmt.Sprintf("outcome of update %q", command.UpdateID)
		if success := command.UpdateOutcome.Success; success != nil {
			return protocol.CheckPayloadBytes(what, len(success.Data), limit)
		}
		payload = command.UpdateOutcome.Failure
	default:
		return nil
	}
	return protocol.CheckPayloadSize(what, payload, limit)
}

// ValidateWorkflowCommand 校验 runID 的 Workflow Task 提交的一条命令。标识不超过 protocol 的长度上限（子 Workflow 的
// Workflow ID 是 <runID>/<ChildID>，整体受 MaxWorkflowIDLength 限制）；调度 Activity 命令另见 ValidateScheduleActivityCommand。
func ValidateWorkflowCommand(runID string, command history.Command) error {
	switch command.Type {
	case history.CmdScheduleActivity:
		return ValidateScheduleActivityCommand(command)
	case history.CmdScheduleTimer:
		if err := protocol.CheckLength("TimerID", command.TimerID, protocol.MaxTimerIDLength); err != nil {
			return fmt.Errorf("%w: %w", ErrBadStartTimerAttributes, err)
		}
	case history.CmdStartChildWorkflow:
		if err := errors.Join(
			protocol.CheckLength("child workflow id", ChildWorkflowID(runID, command.ChildID), protocol.MaxWorkflowIDLength),
			protocol.CheckLength("child workflow type", command.WorkflowType, protocol.MaxWorkflowTypeLength),
		); err != nil {
			return fmt.Errorf("%w: %w", ErrBadStartChildWorkflowAttributes, err)
		}
	case history.CmdRecordMarker:
		if err := errors.Join(
			protocol.CheckLength("marker name", command.MarkerName, protocol.MaxMarkerNameLength),
			protocol.CheckLength("marker id", command.MarkerID, protocol.MaxMarkerIDLength),
		); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRecordMarkerAttributes, err)
		}
	}
	return nil
}

// ValidateScheduleActivityCommand 校验调度 Activity 命令：ActivityID 非空，ActivityID、ActivityType 与 TaskQueue
// 不超过 protocol 的长度上限；StartToCloseTimeout 与 ScheduleToCloseTimeout 至少设置一个。attempt 开始后它的存活
// 只由超时判定，没有超时的 attempt 在 Worker 消失后无法重试。
func ValidateScheduleActivityCommand(command history.Command) error {
	if command.ActivityID == "" {
		return fmt.Errorf("%w: activity of type %s has an empty ActivityID", ErrBadScheduleActivityAttributes, command.ActivityType)
	}
	if err := errors.Join(
		protocol.CheckLength("ActivityID", command.ActivityID, protocol.MaxActivityIDLength),
		protocol.CheckLength("ActivityType", command.ActivityType, protocol.MaxActivityTypeLength),
		protocol.CheckLength("TaskQueue", command.TaskQueue, protocol.MaxTaskQueueLength),
	); err != nil {
		return fmt.Errorf("%w: activity %.64q: %w", ErrBadScheduleActivityAttributes, command.ActivityID, err)
	}
	if command.StartToCloseTimeout <= 0 && command.ScheduleToCloseTimeout <= 0 {
		return fmt.Errorf("%w: activity %q (type %s) sets neither StartToCloseTimeout nor ScheduleToCloseTimeout",
			ErrBadScheduleActivityAttributes, command.ActivityID, command.ActivityType)
	}
	return nil
}

type PreparedWorkflowTask struct {
	Commands        []history.Command
	CommandEvents   []history.Event
	ChildTargets    map[int]ChildWorkflowTarget
	ChildRefs       map[int]WorkflowTargetRef
	ContinuedRunIDs map[int]string
	ContinuedRefs   map[int]WorkflowTargetRef
}

// PrepareWorkflowTask resolves all external references before RunInTx begins.
//
// 单个命令的载荷超过 payloadLimit 字节，或全部命令超过 protocol.MaxWorkflowTaskCommandsSize 时返回包装
// protocol.ErrPayloadTooLarge 的错误。
func PrepareWorkflowTask(runID string, commands []history.Command, traceContext map[string]string, payloadLimit int, resolver WorkflowCommandResolver) (PreparedWorkflowTask, error) {
	prepared := PreparedWorkflowTask{
		Commands:        make([]history.Command, len(commands)),
		CommandEvents:   make([]history.Event, 0, len(commands)),
		ChildTargets:    make(map[int]ChildWorkflowTarget),
		ChildRefs:       make(map[int]WorkflowTargetRef),
		ContinuedRunIDs: make(map[int]string),
		ContinuedRefs:   make(map[int]WorkflowTargetRef),
	}
	if err := protocol.CheckWorkflowTaskCommandsSize(commands); err != nil {
		return PreparedWorkflowTask{}, err
	}
	for i, command := range commands {
		if err := CheckCommandPayloadSize(command, payloadLimit); err != nil {
			return PreparedWorkflowTask{}, err
		}
		if err := ValidateWorkflowCommand(runID, command); err != nil {
			return PreparedWorkflowTask{}, err
		}
		if command.Type == history.CmdScheduleActivity {
			resolved, err := resolver.ResolveActivityCommand(runID, command)
			if err != nil {
				return PreparedWorkflowTask{}, err
			}
			command = resolved
		}
		if command.Type == history.CmdCompleteWorkflowUpdate && command.UpdateOutcome != nil {
			normalized, err := protocol.NormalizeUpdateOutcome(*command.UpdateOutcome)
			if err != nil {
				return PreparedWorkflowTask{}, fmt.Errorf("complete workflow update %q: %w", command.UpdateID, err)
			}
			command.UpdateOutcome = &normalized
		}
		prepared.Commands[i] = command
		switch command.Type {
		case history.CmdScheduleActivity:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewActivityScheduledEvent(history.ActivityScheduledEvent{
				ActivityID: command.ActivityID, ActivityType: command.ActivityType,
				TaskQueue:                command.TaskQueue,
				Input:                    command.Input,
				ScheduleToStartTimeoutMs: command.ScheduleToStartTimeout.Milliseconds(),
				StartToCloseTimeoutMs:    command.StartToCloseTimeout.Milliseconds(),
				ScheduleToCloseTimeoutMs: command.ScheduleToCloseTimeout.Milliseconds(),
				HeartbeatTimeoutMs:       command.HeartbeatTimeout.Milliseconds(),
				RetryPolicy:              activityRetryPolicy(command),
				TraceContext:             cloneStringMap(traceContext),
			}))
		case history.CmdRequestCancelActivity:
			// 调度事件 ID 由提交时按 open waits 补全（resolveCancelTargets）。
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewActivityCancelRequestedEvent(command.ActivityID, 0))
		case history.CmdCancelTimer:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewTimerCancelledEvent(command.TimerID, 0))
		case history.CmdRequestCancelChildWorkflow:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewChildWorkflowCancelRequestedEvent(command.ChildID, "", 0))
		case history.CmdScheduleTimer:
			event := history.NewTimerScheduledEvent(command.TimerID, map[string]any{"delay_ns": int64(command.Delay)}, command.Delay.Milliseconds())
			event.TimerScheduled.TraceContext = cloneStringMap(traceContext)
			prepared.CommandEvents = append(prepared.CommandEvents, event)
		case history.CmdRecordMarker:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewMarkerRecordedEvent(command.MarkerName, command.MarkerID, command.Input))
		case history.CmdUpsertSearchAttributes:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewSearchAttributesUpsertedEvent(command.Input))
		case history.CmdStartChildWorkflow:
			ref, err := resolver.ChildWorkflowRef(runID, command.WorkflowType)
			if err != nil {
				return PreparedWorkflowTask{}, err
			}
			if err := resolver.CheckWorkflowInput(ref, command.Input); err != nil {
				return PreparedWorkflowTask{}, fmt.Errorf("child workflow %q: %w", command.WorkflowType, err)
			}
			policy, err := protocol.NormalizeParentClosePolicy(command.ParentClosePolicy)
			if err != nil {
				return PreparedWorkflowTask{}, fmt.Errorf("child workflow %q: %w", command.WorkflowType, err)
			}
			childRunID := NewID()
			prepared.ChildTargets[i] = ChildWorkflowTarget{RunID: childRunID, WorkflowType: ref.WorkflowType, ConfigVersion: ref.Version, ParentClosePolicy: policy}
			prepared.ChildRefs[i] = ref
			event := history.NewChildWorkflowStartedEvent(ref.WorkflowType, command.ChildID, childRunID, ref.Version, command.Input)
			event.ChildWorkflowStarted.ParentClosePolicy = policy
			prepared.CommandEvents = append(prepared.CommandEvents, event)
		case history.CmdContinueAsNew:
			ref, err := resolver.ContinuedWorkflowRef(runID, command.WorkflowType)
			if err != nil {
				return PreparedWorkflowTask{}, err
			}
			if err := resolver.CheckWorkflowInput(ref, command.Input); err != nil {
				return PreparedWorkflowTask{}, fmt.Errorf("continue-as-new %q: %w", ref.WorkflowType, err)
			}
			continuedRunID := NewID()
			prepared.ContinuedRunIDs[i] = continuedRunID
			prepared.ContinuedRefs[i] = ref
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewWorkflowContinuedAsNewEvent(ref.WorkflowType, continuedRunID, ref.Version, command.Input))
		case history.CmdCompleteWorkflowUpdate:
			if command.UpdateOutcome == nil {
				return PreparedWorkflowTask{}, protocol.InvalidRequestf("complete workflow update %q: UpdateOutcome is required", command.UpdateID)
			}
			prepared.CommandEvents = append(prepared.CommandEvents,
				history.NewWorkflowUpdateCompletedOutcomeEvent(command.Name, command.UpdateID, *command.UpdateOutcome))
		case history.CmdCompleteWorkflow:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewWorkflowCompletedEvent(command.Output))
		case history.CmdCancelWorkflow:
			prepared.CommandEvents = append(prepared.CommandEvents, history.NewWorkflowCancelledEvent(command.Details))
		case history.CmdFailWorkflow:
			// The engine appends the terminal WorkflowFailed event in the same transaction (AppendWorkflowFailed).
		}
	}
	return prepared, nil
}
