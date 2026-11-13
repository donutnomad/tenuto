package execution

import (
	"fmt"
	"strings"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// RecordedPayloadPositions 返回 History 里已有、之后的 Workflow Task 还会由 Program 解码的载荷位置：Workflow 输入、
// 收到的 Signal、完成的 Activity 结果、还没有关闭的 Activity 的输入（之后由 Worker 按 Run 当前 Program 的契约执行）、
// 完成的子 Workflow 结果、接受的 Update 参数、SideEffect / MutableSideEffect 的值，以及失败的 Activity / 子 Workflow
// 带的应用错误 detail（Program 用 workflow.ApplicationErrorDetail 读取）。
func RecordedPayloadPositions(events []history.Event) map[program.PayloadPosition]bool {
	positions := map[program.PayloadPosition]bool{}
	scheduled := map[int64]string{}
	childTypes := map[string]string{}
	closeActivity := func(scheduledEventID int64) { delete(scheduled, scheduledEventID) }
	for _, event := range events {
		switch {
		case event.WorkflowStarted != nil:
			positions[program.PayloadPosition{Kind: program.PayloadWorkflowInput}] = true
		case event.WorkflowSignaled != nil:
			positions[program.PayloadPosition{Kind: program.PayloadSignal, Name: event.WorkflowSignaled.Name}] = true
		case event.ActivityScheduled != nil:
			scheduled[event.ID] = event.ActivityScheduled.ActivityType
		case event.ActivityCompleted != nil:
			activityType := event.ActivityCompleted.ActivityType
			if activityType == "" {
				activityType = scheduled[event.ActivityCompleted.ScheduledEventID]
			}
			positions[program.PayloadPosition{Kind: program.PayloadActivityResult, Name: activityType}] = true
			closeActivity(event.ActivityCompleted.ScheduledEventID)
		case event.ActivityFailed != nil:
			if event.ActivityFailed.Failure != nil {
				positions[program.PayloadPosition{Kind: program.PayloadErrorDetail}] = true
			}
			closeActivity(event.ActivityFailed.ScheduledEventID)
		case event.ActivityTimedOut != nil:
			if event.ActivityTimedOut.Failure != nil {
				positions[program.PayloadPosition{Kind: program.PayloadErrorDetail}] = true
			}
			closeActivity(event.ActivityTimedOut.ScheduledEventID)
		case event.ActivityCancelled != nil:
			closeActivity(event.ActivityCancelled.ScheduledEventID)
		case event.ChildWorkflowStarted != nil:
			childTypes[event.ChildWorkflowStarted.ChildID] = event.ChildWorkflowStarted.Name
		case event.ChildWorkflowFailed != nil:
			if event.ChildWorkflowFailed.Failure != nil {
				positions[program.PayloadPosition{Kind: program.PayloadErrorDetail}] = true
			}
		case event.ChildWorkflowCompleted != nil:
			positions[program.PayloadPosition{Kind: program.PayloadChildResult, Name: childTypes[event.ChildWorkflowCompleted.ChildID]}] = true
		case event.WorkflowUpdateAccepted != nil:
			if args := event.WorkflowUpdateAccepted.Args; args != nil {
				for index := range *args {
					positions[program.PayloadPosition{Kind: program.PayloadUpdateArgument, Name: event.WorkflowUpdateAccepted.Name, Index: index}] = true
				}
			}
		case event.MarkerRecorded != nil:
			switch marker := event.MarkerRecorded; marker.MarkerName {
			case "SideEffect":
				positions[program.PayloadPosition{Kind: program.PayloadSideEffect}] = true
			case "MutableSideEffect":
				// MarkerID 是 <id>_<调用序号>。
				if cut := strings.LastIndexByte(marker.MarkerID, '_'); cut > 0 {
					positions[program.PayloadPosition{Kind: program.PayloadMutableSideEffect, Name: marker.MarkerID[:cut]}] = true
				}
			}
		}
	}
	for _, activityType := range scheduled {
		positions[program.PayloadPosition{Kind: program.PayloadActivityInput, Name: activityType}] = true
	}
	return positions
}

// PayloadConflictError 是升级到 candidate 时 History 里已有载荷可能读不回的错误；incompatibilities 是
// program.ComparePayloadTypes 的结果。确定位置的不兼容只算 History 里出现过的位置；未确定的解码处（见
// program.UnresolvedPayloadSite）在 History 里有同种载荷时算（名字已知时还要同名，种类未知时总算）。一项都不算时
// 返回 nil。
func PayloadConflictError(candidate string, incompatibilities []program.PayloadIncompatibility, events []history.Event) error {
	if len(incompatibilities) == 0 {
		return nil
	}
	recorded := RecordedPayloadPositions(events)
	var unreadable, unproven []string
	for _, incompatibility := range incompatibilities {
		switch {
		case incompatibility.Unresolved == nil:
			if recorded[incompatibility.Position] {
				unreadable = append(unreadable, incompatibility.String())
			}
		case recordsKind(recorded, incompatibility.Position):
			unproven = append(unproven, incompatibility.String())
		}
	}
	var reasons []string
	if len(unreadable) > 0 {
		reasons = append(reasons, "payloads recorded in history cannot be read by the candidate program: "+strings.Join(unreadable, "; "))
	}
	if len(unproven) > 0 {
		reasons = append(reasons, "the payload check cannot prove that payloads recorded in history stay readable: "+strings.Join(unproven, "; "))
	}
	if len(reasons) == 0 {
		return nil
	}
	return fmt.Errorf("auto upgrade to %s refused: %s; "+
		"set policy pinned to finish on the current program, or publish a version that keeps these fields and set policy auto_upgrade to resume",
		candidate, strings.Join(reasons, "; "))
}

// recordsKind 报告 recorded 里是否有 position 这类载荷：Kind 为空时任何载荷都算，Name 非空时只算同名的。
func recordsKind(recorded map[program.PayloadPosition]bool, position program.PayloadPosition) bool {
	for candidate := range recorded {
		if position.Kind == "" || (candidate.Kind == position.Kind && (position.Name == "" || candidate.Name == position.Name)) {
			return true
		}
	}
	return false
}
