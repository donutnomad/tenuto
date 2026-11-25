package main

// replay_trace.go implements the Trace Debugger's full-replay pass: it runs the
// program once over the entire event log and records every OnStep as a
// traceStep, grouping steps into lanes keyed by wlang's RoutineID. Running runs
// (with unresolved activities) surface a pending frontier per lane instead of
// being silently truncated.

import (
	"context"
	"strconv"
	"sync"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/wlang/runtime"
)

// ── Trace data structures (json tags snake_case, aligned with replayState) ──

type traceStep struct {
	Seq     int            `json:"seq"`
	LaneID  string         `json:"lane_id"`
	LaneSeq int            `json:"lane_seq"`
	Line    int            `json:"line"`
	Path    string         `json:"path"`
	Vars    map[string]any `json:"vars"`
	Frames  []traceFrame   `json:"frames"`
	Effects []traceEffect  `json:"effects,omitempty"`
}

type traceEffect struct {
	Kind              string `json:"kind"`
	Phase             string `json:"phase"`
	InvocationID      string `json:"invocation_id"`
	ActivityID        string `json:"activity_id"`
	Status            string `json:"status"`
	ActivityType      string `json:"activity_type,omitempty"`
	ChildID           string `json:"child_id,omitempty"`
	ChildRunID        string `json:"child_run_id,omitempty"`
	ChildWorkflowType string `json:"child_workflow_type,omitempty"`
	ScheduledEventID  int64  `json:"scheduled_event_id,omitempty"`
	CallSeq           *int   `json:"call_seq,omitempty"`
	WaitSeq           *int   `json:"wait_seq,omitempty"`
	ResumeSeq         *int   `json:"resume_seq,omitempty"`
}

type traceEffectRecord struct {
	StepIndex int
	Phase     string
	Seq       int
}

type traceEffectInvocation struct {
	Base      traceEffect
	CallSeq   *int
	WaitSeq   *int
	ResumeSeq *int
	Records   []traceEffectRecord
	Seen      map[string]bool
}

type traceFrame struct {
	Name string         `json:"name"`
	Line int            `json:"line"`
	Path string         `json:"path"`
	Vars map[string]any `json:"vars"`
}

type waitingActivity struct {
	ActivityID string `json:"activity_id"`
	Type       string `json:"type"`
	Kind       string `json:"kind"` // activity/timer/signal/update/child
}

type laneInfo struct {
	LaneID          string           `json:"lane_id"`
	LaneKind        string           `json:"lane_kind"` // main/routine
	EndKind         string           `json:"end_kind"`  // completed/pending-frontier/error
	PendingFrontier bool             `json:"pending_frontier"`
	WaitingActivity *waitingActivity `json:"waiting_activity,omitempty"`
	ErrorMsg        string           `json:"error_msg,omitempty"`
	StartLine       int              `json:"start_line"`
	EndLine         int              `json:"end_line"`
	StepCount       int              `json:"step_count"`
}

type traceResult struct {
	Pseudocode string          `json:"pseudocode"`
	Lanes      []laneInfo      `json:"lanes"`
	Steps      []traceStep     `json:"steps"`
	Activities []activityState `json:"activities"`
	RunKind    string          `json:"run_kind"` // completed/running
	TotalSteps int             `json:"total_steps"`
	Error      string          `json:"error,omitempty"`
}

// computeLaneID derives a lane key from the routine instance. RoutineID is
// globally unique per launch (including foreach fan-out), so it alone
// distinguishes concurrent instances — no path/scope-root heuristics needed.
func computeLaneID(info runtime.StepInfo) string {
	if info.RoutineID == 0 {
		return "main"
	}
	return "routine-" + strconv.FormatUint(info.RoutineID, 10)
}

func laneKind(laneID string) string {
	if laneID == "main" {
		return "main"
	}
	return "routine"
}

// buildTrace compiles and replays the program once over the full event log,
// recording every step grouped into lanes. It is the wasm entry point for the
// trace debugger (exposed to JS as traceReplay).
func buildTrace(programJSON, eventsJSON []byte) (traceResult, error) {
	h, err := newReplayHandle(programJSON, eventsJSON)
	if err != nil {
		return traceResult{}, err
	}
	return h.buildTrace()
}

func (h *replayHandle) buildTrace() (traceResult, error) {
	// Full replay: make every event visible so activities/timers/signals resolve
	// as far as the log allows.
	env := buildReplayEnv(h.events)
	tr := newTraceRun()

	var mu sync.Mutex
	var steps []traceStep
	seq := 0
	laneOrder := map[string]int{} // laneID -> display order (first-seen)
	nextLaneOrder := 0
	laneAcc := map[string]*laneInfo{}
	// laneStepSeq assigns a per-lane sequence number.
	laneStepSeq := map[string]int{}
	lastStepByLane := map[string]int{}
	effectInvocations := map[string]*traceEffectInvocation{}

	onStep := func(info runtime.StepInfo) error {
		node := info.Node
		path := node.Path()
		line := h.pathToLine(path)
		laneID := computeLaneID(info)

		frames := make([]traceFrame, 0, len(info.Frames))
		for _, frame := range info.Frames {
			frames = append(frames, traceFrame{
				Name: frame.Name,
				Line: h.pathToLine(frame.Path),
				Path: frame.Path,
				Vars: snapshotScope(frame.Scope),
			})
		}
		vars := snapshotScope(info.Scope)
		if len(frames) > 0 {
			vars = frames[0].Vars
		}

		mu.Lock()
		if _, ok := laneOrder[laneID]; !ok {
			laneOrder[laneID] = nextLaneOrder
			nextLaneOrder++
			laneAcc[laneID] = &laneInfo{
				LaneID:    laneID,
				LaneKind:  laneKind(laneID),
				StartLine: line,
				EndLine:   line,
			}
		}
		lseq := laneStepSeq[laneID]
		laneStepSeq[laneID] = lseq + 1

		steps = append(steps, traceStep{
			Seq:     seq,
			LaneID:  laneID,
			LaneSeq: lseq,
			Line:    line,
			Path:    path,
			Vars:    vars,
			Frames:  frames,
		})
		lastStepByLane[laneID] = len(steps) - 1
		acc := laneAcc[laneID]
		acc.EndLine = line
		acc.StepCount++
		seq++
		mu.Unlock()
		return nil
	}

	onEffect := func(effect executor.DebugEffect) {
		laneID := "main"
		if effect.RoutineID != 0 {
			laneID = "routine-" + strconv.FormatUint(effect.RoutineID, 10)
		}
		mu.Lock()
		defer mu.Unlock()
		stepIndex, ok := lastStepByLane[laneID]
		if !ok || stepIndex < 0 || stepIndex >= len(steps) {
			return
		}
		invocationID := debugInvocationID(effect)
		invocation := effectInvocations[invocationID]
		if invocation == nil {
			invocation = &traceEffectInvocation{
				Base: traceEffect{
					Kind:              effect.Kind,
					InvocationID:      invocationID,
					ActivityID:        effect.ActivityID,
					ActivityType:      effect.Type,
					ChildID:           effect.ChildID,
					ChildRunID:        effect.ChildRunID,
					ChildWorkflowType: effect.ChildWorkflowType,
					ScheduledEventID:  effect.ScheduledEventID,
				},
				Seen: make(map[string]bool),
			}
			effectInvocations[invocationID] = invocation
		}
		seq := steps[stepIndex].Seq
		key := effect.Phase + ":" + strconv.Itoa(seq)
		if invocation.Seen[key] {
			return
		}
		invocation.Seen[key] = true
		seqCopy := seq
		switch effect.Phase {
		case "scheduled":
			if invocation.CallSeq == nil {
				invocation.CallSeq = &seqCopy
			}
		case "awaited":
			invocation.WaitSeq = &seqCopy
		case "resolved":
			invocation.ResumeSeq = &seqCopy
		}
		invocation.Records = append(invocation.Records, traceEffectRecord{
			StepIndex: stepIndex,
			Phase:     effect.Phase,
			Seq:       seq,
		})
	}

	debugResult, runErr := interpreter.ReplayWLangForDebug(
		context.Background(),
		h.programJSON,
		h.engineEvents,
		onStep,
		onEffect,
	)
	for routineID, waiting := range debugResult.WaitingActivities {
		laneID := "main"
		if routineID != 0 {
			laneID = "routine-" + strconv.FormatUint(routineID, 10)
		}
		tr.recordFrontierForLane(laneID, waitingActivity{
			ActivityID: waiting.ActivityID,
			Type:       waiting.Type,
			Kind:       "activity",
		})
	}

	mu.Lock()
	defer mu.Unlock()
	attachTraceEffects(steps, effectInvocations, h.engineEvents)

	// Assemble lanes in first-seen order.
	lanes := assembleLanes(laneOrder, laneAcc, tr, runErr, env)

	runKind := "completed"
	for i := range lanes {
		if lanes[i].EndKind == "pending-frontier" {
			runKind = "running"
			break
		}
	}

	errMsg := env.errMsg
	if runErr != nil && errMsg == "" {
		errMsg = runErr.Error()
	}

	return traceResult{
		Pseudocode: h.pseudocode,
		Lanes:      lanes,
		Steps:      steps,
		Activities: activityStatesFromDebug(debugResult.Activities),
		RunKind:    runKind,
		TotalSteps: len(steps),
		Error:      errMsg,
	}, nil
}

func debugInvocationID(effect executor.DebugEffect) string {
	if effect.Kind == "child-workflow" && effect.ChildID != "" {
		return effect.Kind + ":" + effect.ChildID
	}
	if effect.ScheduledEventID > 0 {
		return effect.Kind + ":" + strconv.FormatInt(effect.ScheduledEventID, 10)
	}
	return effect.Kind + ":" + effect.ActivityID
}

func attachTraceEffects(steps []traceStep, invocations map[string]*traceEffectInvocation, events []history.Event) {
	for _, invocation := range invocations {
		status := debugEffectStatus(invocation.Base, events)
		for _, record := range invocation.Records {
			if record.StepIndex < 0 || record.StepIndex >= len(steps) {
				continue
			}
			effect := invocation.Base
			effect.Phase = record.Phase
			effect.Status = status
			effect.CallSeq = invocation.CallSeq
			effect.WaitSeq = invocation.WaitSeq
			effect.ResumeSeq = invocation.ResumeSeq
			steps[record.StepIndex].Effects = append(steps[record.StepIndex].Effects, effect)
		}
	}
}

func debugEffectStatus(effect traceEffect, events []history.Event) string {
	status := "scheduled"
	for _, event := range events {
		switch effect.Kind {
		case "activity":
			switch {
			case event.ActivityStarted != nil && event.ActivityStarted.ScheduledEventID == effect.ScheduledEventID:
				status = "running"
			case event.ActivityCompleted != nil && event.ActivityCompleted.ScheduledEventID == effect.ScheduledEventID:
				status = "completed"
			case event.ActivityFailed != nil && event.ActivityFailed.ScheduledEventID == effect.ScheduledEventID:
				status = "failed"
			case event.ActivityTimedOut != nil && event.ActivityTimedOut.ScheduledEventID == effect.ScheduledEventID:
				status = "timed_out"
			case event.ActivityCancelled != nil && event.ActivityCancelled.ScheduledEventID == effect.ScheduledEventID:
				status = "cancelled"
			}
		case "child-workflow":
			switch {
			case event.ChildWorkflowCompleted != nil && event.ChildWorkflowCompleted.ChildID == effect.ChildID:
				status = "completed"
			case event.ChildWorkflowFailed != nil && event.ChildWorkflowFailed.ChildID == effect.ChildID:
				status = "failed"
			case event.ChildWorkflowCancelled != nil && event.ChildWorkflowCancelled.ChildID == effect.ChildID:
				status = "cancelled"
			case event.ChildWorkflowTerminated != nil && event.ChildWorkflowTerminated.ChildID == effect.ChildID:
				status = "terminated"
			case event.ChildWorkflowStarted != nil && event.ChildWorkflowStarted.ChildID == effect.ChildID:
				status = "running"
			}
		}
	}
	return status
}

// assembleLanes finalizes each lane's EndKind by correlating recorded pending
// frontiers with lanes, and folds in the main-line / routine error state.
func assembleLanes(
	laneOrder map[string]int,
	laneAcc map[string]*laneInfo,
	tr *traceRun,
	runErr error,
	env replayEnv,
) []laneInfo {
	ordered := make([]string, len(laneOrder))
	for id, ord := range laneOrder {
		ordered[ord] = id
	}

	pendingByLane := tr.pendingByLane()
	errByLane := tr.errByLane()

	// A real (non-pending) main-line error becomes the "main" lane's error. The
	// event log's WorkflowFailed also counts as a main-line error.
	if _, ok := errByLane["main"]; !ok {
		if runErr != nil {
			errByLane["main"] = runErr.Error()
		} else if env.errMsg != "" {
			errByLane["main"] = env.errMsg
		}
	}

	lanes := make([]laneInfo, 0, len(ordered))
	for _, id := range ordered {
		info := *laneAcc[id]

		switch {
		case pendingByLane[id] != nil:
			wa := pendingByLane[id]
			info.EndKind = "pending-frontier"
			info.PendingFrontier = true
			info.WaitingActivity = wa
		case errByLane[id] != "":
			info.EndKind = "error"
			info.ErrorMsg = errByLane[id]
		default:
			info.EndKind = "completed"
		}
		lanes = append(lanes, info)
	}
	return lanes
}

// snapshotScope renders the active call frame into an independent,
// marshal-safe snapshot for a single step.
func snapshotScope(scope *runtime.Scope) map[string]any {
	all := scope.FrameVars()
	out := make(map[string]any, len(all))
	for name, val := range all {
		if name == "ctx" {
			continue
		}
		out[name] = snapshotValue(val)
	}
	return out
}
