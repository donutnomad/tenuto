package main

// replay_core.go holds the platform-neutral replay logic (no build tag) so it
// can be unit-tested with a normal `go test`. main.go keeps only the
// syscall/js bridge behind `//go:build js && wasm`.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/wlang/ast"
	"github.com/donutnomad/wlang/compiler"
	"github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
	"github.com/donutnomad/wlang/wflang"
)

// ── Event Types (mirrors engine/event.go, JSON-only subset) ──

type event struct {
	ID        int64  `json:"id"`
	Timestamp string `json:"timestamp"`

	WorkflowStarted   *workflowStartedEvent   `json:"workflowStarted,omitempty"`
	WorkflowCompleted *workflowCompletedEvent `json:"workflowCompleted,omitempty"`
	WorkflowFailed    *workflowFailedEvent    `json:"workflowFailed,omitempty"`

	ActivityScheduled *activityScheduledEvent `json:"activityScheduled,omitempty"`
	ActivityCompleted *activityCompletedEvent `json:"activityCompleted,omitempty"`
	ActivityFailed    *activityFailedEvent    `json:"activityFailed,omitempty"`

	TimerScheduled *timerScheduledEvent `json:"timerScheduled,omitempty"`
	TimerFired     *timerFiredEvent     `json:"timerFired,omitempty"`
	TimerCancelled *timerFiredEvent     `json:"timerCancelled,omitempty"`

	WorkflowSignaled        *workflowSignaledEvent        `json:"workflowSignaled,omitempty"`
	WorkflowUpdateAccepted  *workflowUpdateAcceptedEvent  `json:"workflowUpdateAccepted,omitempty"`
	WorkflowUpdateCompleted *workflowUpdateCompletedEvent `json:"workflowUpdateCompleted,omitempty"`

	MarkerRecorded *markerRecordedEvent `json:"markerRecorded,omitempty"`

	ChildWorkflowStarted   *childWorkflowStartedEvent   `json:"childWorkflowStarted,omitempty"`
	ChildWorkflowCompleted *childWorkflowCompletedEvent `json:"childWorkflowCompleted,omitempty"`
	ChildWorkflowFailed    *childWorkflowFailedEvent    `json:"childWorkflowFailed,omitempty"`
}

type workflowStartedEvent struct {
	Input map[string]any `json:"input,omitempty"`
}

type workflowCompletedEvent struct {
	Output map[string]any `json:"output,omitempty"`
}

type workflowFailedEvent struct {
	Error string `json:"error,omitempty"`
}

type activityScheduledEvent struct {
	ActivityID   string         `json:"activityId,omitempty"`
	ActivityType string         `json:"activityType,omitempty"`
	Input        map[string]any `json:"input,omitempty"`
}

type activityCompletedEvent struct {
	ActivityID       string         `json:"activityId,omitempty"`
	ScheduledEventID int64          `json:"scheduledEventId,omitempty"`
	ActivityType     string         `json:"activityType,omitempty"`
	Output           map[string]any `json:"output,omitempty"`
}

type activityFailedEvent struct {
	ActivityID       string `json:"activityId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
	ActivityType     string `json:"activityType,omitempty"`
	Error            string `json:"error,omitempty"`
}

type timerScheduledEvent struct {
	TimerID string `json:"timerId,omitempty"`
}

type timerFiredEvent struct {
	TimerID          string `json:"timerId,omitempty"`
	ScheduledEventID int64  `json:"scheduledEventId,omitempty"`
}

type workflowSignaledEvent struct {
	Name  string         `json:"name,omitempty"`
	Input map[string]any `json:"input,omitempty"`
}

type workflowUpdateAcceptedEvent struct {
	Name     string `json:"name,omitempty"`
	UpdateID string `json:"updateId,omitempty"`
}

type workflowUpdateCompletedEvent struct {
	Name     string `json:"name,omitempty"`
	UpdateID string `json:"updateId,omitempty"`
}

type markerRecordedEvent struct {
	MarkerName string         `json:"markerName,omitempty"`
	MarkerID   string         `json:"markerId,omitempty"`
	Value      map[string]any `json:"value,omitempty"`
}

type childWorkflowStartedEvent struct {
	Name       string         `json:"name,omitempty"`
	ChildID    string         `json:"childId,omitempty"`
	ChildRunID string         `json:"childRunId,omitempty"`
	Input      map[string]any `json:"input,omitempty"`
}

type childWorkflowCompletedEvent struct {
	ChildID    string         `json:"childId,omitempty"`
	ChildRunID string         `json:"childRunId,omitempty"`
	Output     map[string]any `json:"output,omitempty"`
}

type childWorkflowFailedEvent struct {
	ChildID    string `json:"childId,omitempty"`
	ChildRunID string `json:"childRunId,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ── Replay Handle ──

type replayHandle struct {
	pseudocode   string
	programJSON  []byte
	program      *ast.Program
	events       []event
	engineEvents []history.Event

	// stmtLines maps top-level statement index → first pseudocode line (0-based).
	stmtLines []int
	pathLines map[string]int
}

type activityState struct {
	ActivityID string         `json:"activityID"`
	Type       string         `json:"type"`
	Status     string         `json:"status"`
	Input      map[string]any `json:"input,omitempty"`
	Output     map[string]any `json:"output,omitempty"`
	Error      string         `json:"error,omitempty"`
	ChildRunID string         `json:"childRunID,omitempty"`
}

type condResult struct {
	Line   int    `json:"line"`
	Result bool   `json:"result"`
	Expr   string `json:"expr"`
}

type replayState struct {
	Pseudocode  string           `json:"pseudocode"`
	CurrentLine int              `json:"currentLine"`
	NodePath    string           `json:"nodePath,omitempty"`
	Variables   map[string]any   `json:"variables"`
	Activities  []activityState  `json:"activities"`
	Timers      []timerState     `json:"timers,omitempty"`
	Signals     map[string][]any `json:"signals,omitempty"`
	CondResults []condResult     `json:"condResults,omitempty"`
	TotalEvents int              `json:"totalEvents"`
	Error       string           `json:"error,omitempty"`
}

type timerState struct {
	TimerID string `json:"timerID"`
	Status  string `json:"status"`
}

func newReplayHandle(programJSON, eventsJSON []byte) (*replayHandle, error) {
	code, err := wflang.FormatPseudoCode(programJSON)
	if err != nil {
		code = programJSON
	}

	var events []event
	if err := json.Unmarshal(eventsJSON, &events); err != nil {
		return nil, fmt.Errorf("parse events: %w", err)
	}
	var engineEvents []history.Event
	if err := json.Unmarshal(eventsJSON, &engineEvents); err != nil {
		return nil, fmt.Errorf("parse engine events: %w", err)
	}
	normalizeDebugEventLinks(engineEvents)

	h := &replayHandle{
		pseudocode:   string(code),
		programJSON:  programJSON,
		events:       events,
		engineEvents: engineEvents,
	}
	h.buildLineMapping()
	return h, nil
}

func normalizeDebugEventLinks(events []history.Event) {
	// 同一 ActivityID 关闭后可以再用：按历史顺序取最近一次调度，正是这次结果所属的 Activity。
	scheduledByActivityID := make(map[string]int64)
	for i := range events {
		if scheduled := events[i].ActivityScheduled; scheduled != nil {
			scheduledByActivityID[scheduled.ActivityID] = events[i].ID
		}
		if completed := events[i].ActivityCompleted; completed != nil && completed.ScheduledEventID == 0 {
			completed.ScheduledEventID = scheduledByActivityID[completed.ActivityID]
		}
		if failed := events[i].ActivityFailed; failed != nil && failed.ScheduledEventID == 0 {
			failed.ScheduledEventID = scheduledByActivityID[failed.ActivityID]
		}
	}
}

func activityStatesFromDebug(items []executor.DebugActivity) []activityState {
	if len(items) == 0 {
		return nil
	}
	states := make([]activityState, 0, len(items))
	for _, item := range items {
		states = append(states, activityState{
			ActivityID: item.ActivityID,
			Type:       item.Type,
			Status:     item.Status,
			Input:      item.Input,
			Output:     item.Output,
			Error:      item.Error,
		})
	}
	return states
}

// buildLineMapping computes stmtLines by formatting each top-level
// statement individually and counting actual pseudocode output lines.
func (h *replayHandle) buildLineMapping() {
	var envelope map[string]any
	if err := json.Unmarshal(h.programJSON, &envelope); err != nil {
		return
	}
	progRaw, ok := envelope["program"].([]any)
	if !ok {
		return
	}

	importsRaw, _ := envelope["imports"].([]any)
	importLines := len(importsRaw)
	blankAfterImports := 0
	if importLines > 0 && len(progRaw) > 0 {
		blankAfterImports = 1
	}

	currentLine := importLines + blankAfterImports
	h.stmtLines = make([]int, len(progRaw))
	for i, stmt := range progRaw {
		h.stmtLines[i] = currentLine
		mini, err := json.Marshal([]any{stmt})
		if err != nil {
			currentLine++
			continue
		}
		out, err := wflang.FormatPseudoCode(mini)
		if err != nil {
			currentLine++
			continue
		}
		lineCount := strings.Count(string(out), "\n") + 1
		currentLine += lineCount
	}

	// Parse the AST for symbol extraction (raw parse, no type checking).
	prog, err := compiler.ParseProgram(h.programJSON)
	if err == nil {
		h.program = prog
		h.buildNestedLineMapping(envelope)
	}
}

func (h *replayHandle) buildNestedLineMapping(envelope map[string]any) {
	h.pathLines = make(map[string]int)
	lines := strings.Split(h.pseudocode, "\n")
	for i, node := range h.program.Body {
		start := h.stmtLines[i]
		end := len(lines)
		if i+1 < len(h.stmtLines) {
			end = h.stmtLines[i+1]
		}
		h.pathLines[node.Path()] = start
		cursor := start + 1
		for _, nested := range collectStepNodes(node)[1:] {
			firstLine, ok := formatStatementFirstLine(envelope, nested.Path())
			if !ok {
				continue
			}
			for line := cursor; line < end; line++ {
				if strings.TrimSpace(lines[line]) == firstLine {
					h.pathLines[nested.Path()] = line
					cursor = line + 1
					break
				}
			}
		}
	}
}

func collectStepNodes(root ast.Node) []ast.Node {
	var nodes []ast.Node
	var visit func(ast.Node)
	visit = func(node ast.Node) {
		if node == nil {
			return
		}
		if isStepNode(node) {
			nodes = append(nodes, node)
		}
		walkChildren(node, visit)
	}
	visit(root)
	return nodes
}

func isStepNode(node ast.Node) bool {
	switch node.(type) {
	case *ast.Let, *ast.Set, *ast.Return, *ast.IfStmt, *ast.Foreach, *ast.Fori,
		*ast.Break, *ast.Continue, *ast.Panic, *ast.ExprStmt, *ast.Routine,
		*ast.Defer, *ast.SelectStmt:
		return true
	default:
		return false
	}
}

func formatStatementFirstLine(envelope map[string]any, path string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 2 {
		return "", false
	}
	statement, ok := resolveJSONPointer(envelope, parts[:len(parts)-1])
	if !ok {
		return "", false
	}
	document := map[string]any{
		"lang":    "wflang/v1",
		"program": []any{statement},
	}
	if imports, exists := envelope["imports"]; exists {
		document["imports"] = imports
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return "", false
	}
	formatted, err := wflang.FormatPseudoCode(raw)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(formatted), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "import ") {
			return trimmed, true
		}
	}
	return "", false
}

func resolveJSONPointer(root any, parts []string) (any, bool) {
	current := root
	for _, rawPart := range parts {
		part := strings.ReplaceAll(strings.ReplaceAll(rawPart, "~1", "/"), "~0", "~")
		switch value := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = value[part]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(value) {
				return nil, false
			}
			current = value[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// ── Replay Environment Reconstruction ──

// replayEnv is the display state read directly from a prefix of events: the
// workflow's terminal outcome plus the timers and signals shown next to the
// replayed program. Execution itself goes through interpreter.ReplayWLangForDebug.
type replayEnv struct {
	workflowCompleted bool
	workflowOutput    map[string]any
	errMsg            string
	timers            map[string]*timerState
	signals           map[string][]any
}

// buildReplayEnv is shared by stepTo and buildTrace so both see identical state.
func buildReplayEnv(visibleEvents []event) replayEnv {
	env := replayEnv{
		timers:  make(map[string]*timerState),
		signals: make(map[string][]any),
	}
	for _, ev := range visibleEvents {
		switch {
		case ev.TimerScheduled != nil:
			env.timers[ev.TimerScheduled.TimerID] = &timerState{TimerID: ev.TimerScheduled.TimerID, Status: "scheduled"}
		case ev.TimerFired != nil:
			if ts, ok := env.timers[ev.TimerFired.TimerID]; ok {
				ts.Status = "fired"
			}
		case ev.TimerCancelled != nil:
			if ts, ok := env.timers[ev.TimerCancelled.TimerID]; ok {
				ts.Status = "cancelled"
			}
		case ev.WorkflowSignaled != nil:
			env.signals[ev.WorkflowSignaled.Name] = append(env.signals[ev.WorkflowSignaled.Name], ev.WorkflowSignaled.Input)
		case ev.WorkflowCompleted != nil:
			env.workflowCompleted = true
			env.workflowOutput = ev.WorkflowCompleted.Output
		case ev.WorkflowFailed != nil:
			env.errMsg = ev.WorkflowFailed.Error
		}
	}
	return env
}

// stepTo replays events up to eventIndex through the worker executor and uses
// its OnStep hook to track execution position.
func (h *replayHandle) stepTo(eventIndex int) map[string]any {
	limit := eventIndex
	if limit > len(h.events) {
		limit = len(h.events)
	}
	if limit < 0 {
		limit = 0
	}

	visibleEvents := h.events[:limit]

	env := buildReplayEnv(visibleEvents)
	workflowCompleted := env.workflowCompleted
	workflowOutput := env.workflowOutput
	errMsg := env.errMsg
	timers := env.timers
	signals := env.signals

	// Run the wlang program through the same workflow host API as the worker.
	var lastPath string
	var lastVars map[string]types.Value
	currentLine := 0

	var condResults []condResult
	type pendingIf struct {
		path string
		line int
		expr string
	}
	var pendingIfStmt *pendingIf

	debugResult, runErr := interpreter.ReplayWLangForDebug(
		context.Background(),
		h.programJSON,
		h.engineEvents[:limit],
		func(info runtime.StepInfo) error {
			node := info.Node
			path := node.Path()

			if pendingIfStmt != nil {
				result := strings.HasPrefix(path, pendingIfStmt.path+"/then/")
				condResults = append(condResults, condResult{
					Line:   pendingIfStmt.line,
					Result: result,
					Expr:   pendingIfStmt.expr,
				})
				pendingIfStmt = nil
			}

			if ifNode, ok := node.(*ast.IfStmt); ok {
				condExpr := ""
				if ifNode.Cond != nil {
					if b, err := json.Marshal(ifNode.Cond); err == nil {
						condExpr = string(b)
					}
				}
				pendingIfStmt = &pendingIf{
					path: path,
					line: h.pathToLine(path),
					expr: condExpr,
				}
			}

			lastPath = path
			lastVars = info.Scope.AllVars()
			return nil
		},
	)

	if pendingIfStmt != nil {
		condResults = append(condResults, condResult{
			Line:   pendingIfStmt.line,
			Result: false,
			Expr:   pendingIfStmt.expr,
		})
	}

	// Map the last OnStep path to a pseudocode line.
	if lastPath != "" {
		currentLine = h.pathToLine(lastPath)
	}

	// If the program ran to completion, point to the last line.
	if (runErr == nil && !debugResult.Blocked) || workflowCompleted {
		totalLines := len(strings.Split(h.pseudocode, "\n"))
		currentLine = totalLines
	}

	// Convert scope variables to JSON-serializable map.
	varsOut := make(map[string]any)
	if lastVars != nil {
		for name, val := range lastVars {
			if name == "ctx" {
				continue
			}
			varsOut[name] = valueToAny(val)
		}
	}
	if workflowCompleted && workflowOutput != nil {
		varsOut["_result"] = workflowOutput
	}

	if runErr != nil && errMsg == "" {
		errMsg = runErr.Error()
	}

	// Build timers list.
	var timerList []timerState
	for _, ts := range timers {
		timerList = append(timerList, *ts)
	}

	state := replayState{
		Pseudocode:  h.pseudocode,
		CurrentLine: currentLine,
		NodePath:    lastPath,
		Variables:   varsOut,
		Activities:  activityStatesFromDebug(debugResult.Activities),
		TotalEvents: len(h.events),
		Error:       errMsg,
	}
	if len(timerList) > 0 {
		state.Timers = timerList
	}
	if len(signals) > 0 {
		state.Signals = signals
	}
	if len(condResults) > 0 {
		state.CondResults = condResults
	}

	b, _ := json.Marshal(state)
	var result map[string]any
	json.Unmarshal(b, &result)
	return result
}

// pathToLine maps a JSON Pointer path (e.g. "/program/2") to a pseudocode line
// using stmtLines.
func (h *replayHandle) pathToLine(path string) int {
	if line, ok := h.pathLines[path]; ok {
		return line
	}
	idx := extractTopLevelIndex(path)
	if idx < 0 || idx >= len(h.stmtLines) {
		return 0
	}
	return h.stmtLines[idx]
}

// extractTopLevelIndex extracts N from "/program/N/..." paths.
func extractTopLevelIndex(path string) int {
	const prefix = "/program/"
	if !strings.HasPrefix(path, prefix) {
		return -1
	}
	rest := path[len(prefix):]
	slash := strings.IndexByte(rest, '/')
	numStr := rest
	if slash >= 0 {
		numStr = rest[:slash]
	}
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return -1
	}
	return n
}

// walkChildren visits each child of an AST node.
func walkChildren(n ast.Node, fn func(ast.Node)) {
	switch x := n.(type) {
	case *ast.Var:
		if x.Default != nil {
			fn(x.Default)
		}
	case *ast.MethodValue:
		fn(x.Receiver)
	case *ast.Array:
		for _, it := range x.Items {
			fn(it)
		}
	case *ast.IfStmt:
		fn(x.Cond)
		for _, s := range x.Then {
			fn(s)
		}
		for _, s := range x.Else {
			fn(s)
		}
	case *ast.IfExpr:
		fn(x.Cond)
		for _, s := range x.Then {
			fn(s)
		}
		for _, s := range x.Else {
			fn(s)
		}
	case *ast.Call:
		for _, a := range x.Args {
			fn(a)
		}
	case *ast.Let:
		if len(x.Bindings) > 0 {
			for _, b := range x.Bindings {
				fn(b.Expr)
			}
		} else if x.Expr != nil {
			fn(x.Expr)
		}
	case *ast.Set:
		fn(x.Expr)
	case *ast.Return:
		fn(x.Expr)
	case *ast.ExprStmt:
		fn(x.Expr)
	case *ast.Panic:
		fn(x.Expr)
	case *ast.Routine:
		if x.Call != nil {
			fn(x.Call)
		}
		for _, s := range x.Body {
			fn(s)
		}
	case *ast.Foreach:
		fn(x.Target)
		for _, s := range x.Do {
			fn(s)
		}
	case *ast.Fori:
		fn(x.From)
		if x.Cond != nil {
			fn(x.Cond)
		}
		if x.To != nil {
			fn(x.To)
		}
		if x.Step != nil {
			fn(x.Step)
		}
		for _, s := range x.Do {
			fn(s)
		}
		// 伪代码把三段式 for 的 post 写在循环体之后，按同一顺序访问才能逐行对上。
		for _, s := range x.Post {
			fn(s)
		}
	case *ast.Match:
		fn(x.Value)
		for _, c := range x.Cases {
			fn(c.When)
			for _, s := range c.Do {
				fn(s)
			}
		}
		for _, s := range x.Default {
			fn(s)
		}
	case *ast.SelectStmt:
		for _, c := range x.Cases {
			for _, s := range c.Do {
				fn(s)
			}
		}
		for _, s := range x.Default {
			fn(s)
		}
	case *ast.FuncLit:
		for _, s := range x.Body {
			fn(s)
		}
	case *ast.Defer:
		if x.Call != nil {
			fn(x.Call)
		}
		if x.Expr != nil {
			fn(x.Expr)
		}
	}
}

// valueToAny converts a types.Value to a JSON-serializable Go value.
func valueToAny(v types.Value) any {
	if v.IsNull() {
		return nil
	}
	raw := v.Go()
	if raw == nil {
		return nil
	}
	switch val := raw.(type) {
	case bool, string, float32, float64,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return val
	case map[string]any:
		return val
	case []any:
		return val
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Sprintf("%v", raw)
	}
	var out any
	json.Unmarshal(b, &out)
	return out
}
