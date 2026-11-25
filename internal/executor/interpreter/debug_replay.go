package interpreter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	wlruntime "github.com/donutnomad/wlang/runtime"

	"github.com/donutnomad/tenuto/program"
)

// ReplayWLangForDebug runs a program against event history through the same
// executor and workflow host API used by workers. The replay is read-only and
// reports every wlang statement through onStep.
func ReplayWLangForDebug(
	ctx context.Context,
	programJSON []byte,
	events []history.Event,
	onStep wlruntime.StepHook,
	effectHooks ...executor.DebugEffectHook,
) (executor.DebugReplayResult, error) {
	var artifact program.WLangProgram
	if err := json.Unmarshal(programJSON, &artifact); err != nil {
		return executor.DebugReplayResult{}, fmt.Errorf("parse debug program: %w", err)
	}
	if err := artifact.ValidateArtifact(); err != nil {
		return executor.DebugReplayResult{}, fmt.Errorf("parse debug program: %w", err)
	}
	if err := artifact.ValidateCapabilities(program.CurrentWLangCapabilities()); err != nil {
		return executor.DebugReplayResult{}, fmt.Errorf("parse debug program: %w", err)
	}

	activities := make(map[string]program.ActivityRef)
	collectDebugSymbols(artifact.Program, activities)
	workflow := &program.WorkflowConfig{Type: "debug-replay", Program: artifact, Activities: activities}
	replay := NewWorkflowExecutorWithContext(ctx, workflow, events)
	replay.stepHook = onStep
	if len(effectHooks) > 0 && effectHooks[0] != nil {
		replay.debugEffectHook = enrichDebugEffectHook(events, effectHooks[0])
	}
	replay.debugWaitingActivities = make(map[uint64]executor.DebugWaitingActivity)
	replay.debugActivities = []*executor.DebugActivity{}
	_, _, err := replay.replayReadOnly(true)
	debugActivities := make([]executor.DebugActivity, 0, len(replay.debugActivities))
	for _, activity := range replay.debugActivities {
		debugActivities = append(debugActivities, *activity)
	}
	return executor.DebugReplayResult{
		Blocked:           replay.blocked || len(replay.debugWaitingActivities) > 0,
		WaitingActivities: replay.debugWaitingActivities,
		Activities:        debugActivities,
		Info:              replay.activationInfo,
	}, err
}

// enrichDebugEffectHook 给子 Workflow 的调试效果补上历史里的子 Run 与调度事件；Activity 的效果由调用自带调度事件 ID。
func enrichDebugEffectHook(events []history.Event, hook executor.DebugEffectHook) executor.DebugEffectHook {
	childStarted := make(map[string]history.ChildWorkflowStartedEvent)
	childStartedEventID := make(map[string]int64)
	for _, event := range events {
		if started := event.ChildWorkflowStarted; started != nil {
			childStarted[started.ChildID] = *started
			childStartedEventID[started.ChildID] = event.ID
		}
	}
	return func(effect executor.DebugEffect) {
		if effect.Kind == "child-workflow" {
			if started, ok := childStarted[effect.ChildID]; ok {
				effect.ChildRunID = started.ChildRunID
				if effect.ChildWorkflowID == "" {
					effect.ChildWorkflowID = started.Name
				}
				effect.ScheduledEventID = childStartedEventID[effect.ChildID]
			}
		}
		hook(effect)
	}
}

func collectDebugSymbols(v any, out map[string]program.ActivityRef) {
	switch value := v.(type) {
	case []any:
		for _, item := range value {
			collectDebugSymbols(item, out)
		}
	case map[string]any:
		if symbol, ok := value["symbol"].(string); ok && symbol != "" {
			out[symbol] = program.ActivityRef{Type: symbol}
		}
		for _, item := range value {
			collectDebugSymbols(item, out)
		}
	}
}
