package execution

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type commandResolverStub struct {
	checked []WorkflowTargetRef
}

func (s *commandResolverStub) ResolveActivityCommand(_ string, command history.Command) (history.Command, error) {
	command.TaskQueue = "payments"
	return command, nil
}

func (s *commandResolverStub) ChildWorkflowRef(_, workflowType string) (WorkflowTargetRef, error) {
	return WorkflowTargetRef{Namespace: "tenant", WorkflowType: workflowType, Version: "child-v2"}, nil
}

func (s *commandResolverStub) ContinuedWorkflowRef(_, workflowType string) (WorkflowTargetRef, error) {
	return WorkflowTargetRef{Namespace: "tenant", WorkflowType: workflowType, Version: "next-v3"}, nil
}

func (s *commandResolverStub) CheckWorkflowInput(ref WorkflowTargetRef, _ map[string]any) error {
	s.checked = append(s.checked, ref)
	return nil
}

func TestPrepareWorkflowTaskPinsRoutesAndTargetVersions(t *testing.T) {
	resolver := &commandResolverStub{}
	commands := []history.Command{
		{Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "charge", StartToCloseTimeout: time.Second},
		{Type: history.CmdFailWorkflow, Error: "failed"},
		{Type: history.CmdStartChildWorkflow, ChildID: "child-1", WorkflowType: "child"},
		{Type: history.CmdContinueAsNew, WorkflowType: "next"},
	}
	prepared, err := PrepareWorkflowTask("run-1", commands, map[string]string{"trace": "value"}, 0, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Commands) != 4 || len(prepared.CommandEvents) != 3 {
		t.Fatalf("prepared task = %+v", prepared)
	}
	activity := prepared.CommandEvents[0].ActivityScheduled
	if activity == nil || activity.TaskQueue != "payments" || activity.TraceContext["trace"] != "value" {
		t.Fatalf("activity route = %+v", activity)
	}
	child := prepared.ChildTargets[2]
	if child.RunID == "" || child.ConfigVersion != "child-v2" || prepared.CommandEvents[1].ChildWorkflowStarted == nil {
		t.Fatalf("child target = %+v", child)
	}
	if prepared.ContinuedRunIDs[3] == "" || prepared.ContinuedRefs[3].Version != "next-v3" || prepared.CommandEvents[2].WorkflowContinuedAsNew == nil {
		t.Fatalf("continued target = %+v", prepared.ContinuedRefs[3])
	}
	if len(resolver.checked) != 2 || resolver.checked[0].Version != "child-v2" || resolver.checked[1].Version != "next-v3" {
		t.Fatalf("checked input refs = %+v", resolver.checked)
	}
	if commands[0].TaskQueue != "" {
		t.Fatal("preparation mutated executor command")
	}
}

func TestPrepareWorkflowTaskValidatesScheduleActivityAttributes(t *testing.T) {
	for name, command := range map[string]history.Command{
		"no timeout":             {Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "charge"},
		"only schedule-to-start": {Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "charge", ScheduleToStartTimeout: time.Second, HeartbeatTimeout: time.Second},
		"empty activity id":      {Type: history.CmdScheduleActivity, ActivityType: "charge", StartToCloseTimeout: time.Second},
		"activity id too long":   {Type: history.CmdScheduleActivity, ActivityID: strings.Repeat("界", protocol.MaxActivityIDLength+1), ActivityType: "charge", StartToCloseTimeout: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := PrepareWorkflowTask("run-1", []history.Command{command}, nil, 0, &commandResolverStub{})
			if !errors.Is(err, ErrBadScheduleActivityAttributes) {
				t.Fatalf("err = %v, want ErrBadScheduleActivityAttributes", err)
			}
		})
	}
	for name, command := range map[string]history.Command{
		"start-to-close":      {Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "charge", StartToCloseTimeout: time.Second},
		"schedule-to-close":   {Type: history.CmdScheduleActivity, ActivityID: "charge", ActivityType: "charge", ScheduleToCloseTimeout: time.Minute},
		"longest activity id": {Type: history.CmdScheduleActivity, ActivityID: strings.Repeat("界", protocol.MaxActivityIDLength), ActivityType: "charge", StartToCloseTimeout: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PrepareWorkflowTask("run-1", []history.Command{command}, nil, 0, &commandResolverStub{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// 超过列宽的命令标识按命令类型得到各自的 WorkflowTaskFailed cause；恰好到上限的标识通过。
func TestValidateWorkflowCommandChecksIdentifierLengths(t *testing.T) {
	runID := "01a2185a-0000-7000-8000-000000000000"
	childLimit := protocol.MaxWorkflowIDLength - len(runID) - 1
	for name, tc := range map[string]struct {
		command history.Command
		cause   string
	}{
		"timer id":            {history.Command{Type: history.CmdScheduleTimer, TimerID: strings.Repeat("界", protocol.MaxTimerIDLength+1)}, WorkflowTaskFailedCauseBadStartTimerAttributes},
		"child id":            {history.Command{Type: history.CmdStartChildWorkflow, ChildID: strings.Repeat("c", childLimit+1), WorkflowType: "flow"}, WorkflowTaskFailedCauseBadStartChildWorkflowAttributes},
		"child workflow type": {history.Command{Type: history.CmdStartChildWorkflow, ChildID: "c", WorkflowType: strings.Repeat("t", protocol.MaxWorkflowTypeLength+1)}, WorkflowTaskFailedCauseBadStartChildWorkflowAttributes},
		"activity type":       {history.Command{Type: history.CmdScheduleActivity, ActivityID: "a", ActivityType: strings.Repeat("t", protocol.MaxActivityTypeLength+1), StartToCloseTimeout: time.Second}, WorkflowTaskFailedCauseBadScheduleActivityAttributes},
		"task queue":          {history.Command{Type: history.CmdScheduleActivity, ActivityID: "a", ActivityType: "t", TaskQueue: strings.Repeat("q", protocol.MaxTaskQueueLength+1), StartToCloseTimeout: time.Second}, WorkflowTaskFailedCauseBadScheduleActivityAttributes},
		"marker name":         {history.Command{Type: history.CmdRecordMarker, MarkerName: strings.Repeat("n", protocol.MaxMarkerNameLength+1), MarkerID: "1"}, WorkflowTaskFailedCauseBadRecordMarkerAttributes},
		"marker id":           {history.Command{Type: history.CmdRecordMarker, MarkerName: "SideEffect", MarkerID: strings.Repeat("界", protocol.MaxMarkerIDLength+1)}, WorkflowTaskFailedCauseBadRecordMarkerAttributes},
	} {
		t.Run(name, func(t *testing.T) {
			cause, ok := BadCommandAttributesCause(ValidateWorkflowCommand(runID, tc.command))
			if !ok || cause != tc.cause {
				t.Fatalf("cause = %q, %v, want %q", cause, ok, tc.cause)
			}
		})
	}
	for name, command := range map[string]history.Command{
		"longest timer id":   {Type: history.CmdScheduleTimer, TimerID: strings.Repeat("界", protocol.MaxTimerIDLength)},
		"longest child id":   {Type: history.CmdStartChildWorkflow, ChildID: strings.Repeat("c", childLimit), WorkflowType: "flow"},
		"longest task queue": {Type: history.CmdScheduleActivity, ActivityID: "a", ActivityType: "t", TaskQueue: strings.Repeat("q", protocol.MaxTaskQueueLength), StartToCloseTimeout: time.Second},
		"longest marker":     {Type: history.CmdRecordMarker, MarkerName: strings.Repeat("n", protocol.MaxMarkerNameLength), MarkerID: strings.Repeat("界", protocol.MaxMarkerIDLength)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateWorkflowCommand(runID, command); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ActivityScheduled 记录生效的重试策略：没有策略的命令得到缺省策略，部分设置的策略补齐缺省值。
func TestPrepareWorkflowTaskRecordsEffectiveRetryPolicy(t *testing.T) {
	for name, tc := range map[string]struct {
		policy *protocol.ParsedRetryPolicy
		want   history.RetryPolicy
	}{
		"no policy": {policy: nil, want: history.RetryPolicy{InitialIntervalMs: 1000, BackoffCoefficient: 2, MaxIntervalMs: 100_000}},
		"partial":   {policy: &protocol.ParsedRetryPolicy{MaxAttempts: 4, InitialInterval: 3 * time.Second}, want: history.RetryPolicy{MaxAttempts: 4, InitialIntervalMs: 3000, BackoffCoefficient: 2, MaxIntervalMs: 300_000}},
	} {
		t.Run(name, func(t *testing.T) {
			command := history.Command{Type: history.CmdScheduleActivity, ActivityID: "#a1", ActivityType: "charge", StartToCloseTimeout: time.Minute, RetryPolicy: tc.policy}
			prepared, err := PrepareWorkflowTask("run-1", []history.Command{command}, nil, 0, &commandResolverStub{})
			if err != nil {
				t.Fatal(err)
			}
			got := prepared.CommandEvents[0].ActivityScheduled.RetryPolicy
			if got == nil || got.MaxAttempts != tc.want.MaxAttempts || got.InitialIntervalMs != tc.want.InitialIntervalMs ||
				got.BackoffCoefficient != tc.want.BackoffCoefficient || got.MaxIntervalMs != tc.want.MaxIntervalMs {
				t.Fatalf("ActivityScheduled retry policy = %+v, want %+v", got, tc.want)
			}
		})
	}
}
