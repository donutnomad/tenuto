package execution

import (
	"errors"
	"fmt"
	"unicode/utf8"

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

// ErrBadScheduleActivityAttributes 表示 Workflow 发出的调度 Activity 命令缺少必填属性。
var ErrBadScheduleActivityAttributes = errors.New("bad schedule activity attributes")

// WorkflowTaskFailedCauseBadScheduleActivityAttributes 是调度 Activity 命令被拒绝时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseBadScheduleActivityAttributes = "bad_schedule_activity_attributes"

// ErrScheduleActivityDuplicateID 表示调度 Activity 命令的 ActivityID 与本 Run 里仍未关闭的 Activity 重复。
var ErrScheduleActivityDuplicateID = errors.New("schedule activity duplicate id")

// WorkflowTaskFailedCauseScheduleActivityDuplicateID 是 ActivityID 重复时 WorkflowTaskFailed 的 cause。
const WorkflowTaskFailedCauseScheduleActivityDuplicateID = "schedule_activity_duplicate_id"

// MaxActivityIDLength 是 ActivityID 的最大字符数，与 tenuto_activity_states.activity_id 的列宽一致。
const MaxActivityIDLength = 191

// ValidateScheduleActivityCommand 校验调度 Activity 命令：ActivityID 非空且不超过 MaxActivityIDLength 个字符；
// StartToCloseTimeout 与 ScheduleToCloseTimeout 至少设置一个。attempt 开始后它的存活只由超时判定，
// 没有超时的 attempt 在 Worker 消失后无法重试。
func ValidateScheduleActivityCommand(command history.Command) error {
	if command.ActivityID == "" {
		return fmt.Errorf("%w: activity of type %s has an empty ActivityID", ErrBadScheduleActivityAttributes, command.ActivityType)
	}
	if n := utf8.RuneCountInString(command.ActivityID); n > MaxActivityIDLength {
		return fmt.Errorf("%w: ActivityID of activity type %s has %d characters, the limit is %d",
			ErrBadScheduleActivityAttributes, command.ActivityType, n, MaxActivityIDLength)
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
func PrepareWorkflowTask(runID string, commands []history.Command, traceContext map[string]string, resolver WorkflowCommandResolver) (PreparedWorkflowTask, error) {
	prepared := PreparedWorkflowTask{
		Commands:        make([]history.Command, len(commands)),
		CommandEvents:   make([]history.Event, 0, len(commands)),
		ChildTargets:    make(map[int]ChildWorkflowTarget),
		ChildRefs:       make(map[int]WorkflowTargetRef),
		ContinuedRunIDs: make(map[int]string),
		ContinuedRefs:   make(map[int]WorkflowTargetRef),
	}
	for i, command := range commands {
		if command.Type == history.CmdScheduleActivity {
			if err := ValidateScheduleActivityCommand(command); err != nil {
				return PreparedWorkflowTask{}, err
			}
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
