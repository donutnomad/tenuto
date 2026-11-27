package interpreter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// bizWorld 驱动一个 Workflow 走完多个 Workflow Task：每个 Workflow Task 冷重放全部 History 得到命令，按命令写出
// 服务端会写的命令事件；外部事件（Activity 结果、Timer 触发、Signal、Update）由测试脚本追加。结束后用 sticky 会话按
// 同样的批次逐批激活，比较每批命令与最终结果。
type bizWorld struct {
	t       *testing.T
	wf      *program.WorkflowConfig
	events  []history.Event
	now     time.Time
	batches []bizBatch
	pending []history.Event // 上一个 Workflow Task 之后追加、尚未投递的事件
	acts    map[string]int64
	actType map[string]string
	timers  map[string]int64
	child   map[string]int64
	last    []history.Command
	err     error
	done    bool
}

type bizBatch struct {
	delivered []history.Event // 投递给本次激活的事件（含 WorkflowTaskScheduled/Started）
	committed []history.Event // WorkflowTaskCompleted 与命令事件
	through   int64
	commands  []history.Command
	coldErr   error
}

func newBizWorld(t *testing.T, name string, input string) *bizWorld {
	t.Helper()
	result, err := bizGen(t, name)
	if err != nil {
		t.Errorf("%s: generate: %v", name, err)
		w := &bizWorld{t: t, done: true, err: err}
		return w
	}
	w := &bizWorld{t: t, wf: result.Workflow, now: time.Date(2026, 11, 27, 8, 0, 0, 0, time.UTC),
		acts: map[string]int64{}, actType: map[string]string{}, timers: map[string]int64{}, child: map[string]int64{}}
	w.add(history.NewWorkflowStartedEvent(bizPayload(t, input), "", "", "", "", 0))
	return w
}

type bizGenerated struct {
	result sdkgen.Result
	err    error
}

// bizGenCache 按 Workflow 函数名缓存生成结果：同一个函数的多组输入共用一份程序。
var bizGenCache = map[string]bizGenerated{}

// bizGen 与 tenuto-workflow-gen 一样按包目录生成（internal/bizprobe）：go2wlang 在包目录里做类型检查。
func bizGen(t *testing.T, name string) (sdkgen.Result, error) {
	t.Helper()
	if cached, ok := bizGenCache[name]; ok {
		return cached.result, cached.err
	}
	dir, err := filepath.Abs("../../bizprobe")
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	for _, file := range files {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), "\nfunc "+name+"(") {
			result, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: name, FuncName: name, Filename: file, Dir: dir})
			bizGenCache[name] = bizGenerated{result: result, err: err}
			return result, err
		}
	}
	t.Fatalf("no func %s in %s", name, dir)
	return sdkgen.Result{}, nil
}

func bizPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	if raw == "" {
		return map[string]any{}
	}
	var out map[string]any
	if err := jsonvalue.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func (w *bizWorld) add(e history.Event) int64 {
	e.ID = int64(len(w.events) + 1)
	e.Timestamp = w.now
	w.events = append(w.events, e)
	w.pending = append(w.pending, e)
	return e.ID
}

func (w *bizWorld) advance(d time.Duration) { w.now = w.now.Add(d) }

// wft 执行一个 Workflow Task 并写出命令事件；返回命令。
func (w *bizWorld) wft() []history.Command {
	w.t.Helper()
	if w.done || w.err != nil {
		return nil
	}
	sched := w.add(history.NewWorkflowTaskScheduledEvent(0))
	w.add(history.NewWorkflowTaskStartedEvent(sched, "worker", "build", 0))
	delivered := w.pending
	w.pending = nil
	commands, err := NewWorkflowExecutor(w.wf, w.events).Execute()
	batch := bizBatch{delivered: delivered, through: int64(len(w.events)), commands: commands, coldErr: err}
	if err != nil {
		w.err = err
		w.batches = append(w.batches, batch)
		return nil
	}
	w.add(history.NewWorkflowTaskCompletedEvent(sched, "worker"))
	for _, c := range commands {
		switch c.Type {
		case history.CmdScheduleActivity:
			id := w.add(history.NewActivityScheduledEvent(history.ActivityScheduledEvent{ActivityID: c.ActivityID, ActivityType: c.ActivityType, Input: bizRoundTrip(w.t, c.Input)}))
			w.acts[c.ActivityID] = id
			w.actType[c.ActivityID] = c.ActivityType
		case history.CmdScheduleTimer:
			id := w.add(history.NewTimerScheduledEvent(c.TimerID, map[string]any{"delay_ns": int64(c.Delay)}, c.Delay.Milliseconds()))
			w.timers[c.TimerID] = id
		case history.CmdStartChildWorkflow:
			id := w.add(history.NewChildWorkflowStartedEvent(c.WorkflowType, c.ChildID, "run-"+c.ChildID, "", bizRoundTrip(w.t, c.Input)))
			w.child[c.ChildID] = id
		case history.CmdRecordMarker:
			w.add(history.NewMarkerRecordedEvent(c.MarkerName, c.MarkerID, bizRoundTrip(w.t, c.Input)))
		case history.CmdUpsertSearchAttributes:
			w.add(history.NewSearchAttributesUpsertedEvent(c.Input))
		case history.CmdCompleteWorkflowUpdate:
			w.add(history.NewWorkflowUpdateCompletedOutcomeEvent(c.Name, c.UpdateID, *c.UpdateOutcome))
		case history.CmdCancelTimer:
			w.add(history.NewTimerCancelledEvent(c.TimerID, w.timers[c.TimerID]))
			delete(w.timers, c.TimerID)
		case history.CmdRequestCancelActivity:
			sched := w.acts[c.ActivityID]
			req := w.add(history.NewActivityCancelRequestedEvent(c.ActivityID, sched))
			_ = req
		case history.CmdRequestCancelChildWorkflow:
			w.add(history.NewChildWorkflowCancelRequestedEvent(c.ChildID, "run-"+c.ChildID, w.child[c.ChildID]))
		case history.CmdCompleteWorkflow, history.CmdFailWorkflow, history.CmdCancelWorkflow, history.CmdContinueAsNew:
			w.done = true
		}
	}
	batch.committed = w.pending
	w.pending = nil
	w.batches = append(w.batches, batch)
	w.last = commands
	return commands
}

func bizRoundTrip(t *testing.T, v map[string]any) map[string]any {
	t.Helper()
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := jsonvalue.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// activityID 按调度顺序返回第 n 个（从 0 开始）类型为 typ 的未结束 Activity。
func (w *bizWorld) activityByType(typ string, n int) string {
	w.t.Helper()
	var ids []string
	for _, e := range w.events {
		if e.ActivityScheduled != nil && e.ActivityScheduled.ActivityType == typ {
			if _, open := w.acts[e.ActivityScheduled.ActivityID]; open {
				ids = append(ids, e.ActivityScheduled.ActivityID)
			}
		}
	}
	if n >= len(ids) {
		w.t.Fatalf("no open activity %s #%d (open: %v)", typ, n, ids)
	}
	return ids[n]
}

func (w *bizWorld) complete(typ string, n int, output string) {
	w.t.Helper()
	id := w.activityByType(typ, n)
	w.add(history.NewActivityCompletedEvent(history.ActivityCompletedEvent{ActivityID: id, ScheduledEventID: w.acts[id], ActivityType: typ, Output: bizPayload(w.t, output)}))
	delete(w.acts, id)
}

func (w *bizWorld) fail(typ string, n int, message, errType string, nonRetryable bool) {
	w.t.Helper()
	id := w.activityByType(typ, n)
	failure := &protocol.Failure{Message: message, Type: errType, NonRetryable: nonRetryable}
	w.add(history.NewActivityFailedEvent(history.ActivityFailedEvent{ActivityID: id, ScheduledEventID: w.acts[id], ActivityType: typ, Error: message, Failure: failure}))
	delete(w.acts, id)
}

func (w *bizWorld) cancelled(typ string, n int) {
	w.t.Helper()
	id := w.activityByType(typ, n)
	w.add(history.NewActivityCancelledEvent(history.ActivityCancelledEvent{ActivityID: id, ScheduledEventID: w.acts[id], ActivityType: typ}))
	delete(w.acts, id)
}

func (w *bizWorld) fireTimers() {
	for _, e := range w.events {
		if e.TimerScheduled != nil {
			if sched, ok := w.timers[e.TimerScheduled.TimerID]; ok {
				w.add(history.NewTimerFiredEvent(e.TimerScheduled.TimerID, sched))
				delete(w.timers, e.TimerScheduled.TimerID)
			}
		}
	}
}

func (w *bizWorld) completeChild(childID string, output string) {
	w.add(history.NewChildWorkflowCompletedEvent(childID, "run-"+childID, w.child[childID], bizPayload(w.t, output)))
	delete(w.child, childID)
}

func (w *bizWorld) signal(name, payload string) {
	w.add(history.NewWorkflowSignaledEvent(name, bizPayload(w.t, payload)))
}

func (w *bizWorld) update(name, updateID, args string) {
	if args == "" {
		w.add(history.NewWorkflowUpdateAcceptedEvent(name, updateID, protocol.Payloads{}))
		return
	}
	w.add(history.NewWorkflowUpdateAcceptedEvent(name, updateID, protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(args)}}))
}

func (w *bizWorld) cancelRequest() {
	w.add(history.NewWorkflowCancellationRequestedEvent("test"))
}

func (w *bizWorld) query(name string, input map[string]any) string {
	out, rejected, errText := QueryEvaluator{}.EvaluateQuery(context.Background(), w.wf, w.events, name, input)
	if rejected {
		return "query error: " + errText
	}
	return canonicalJSON(w.t, out)
}

// outcome 返回最后一批命令的终态描述；冷重放出错时返回错误。
func (w *bizWorld) outcome() string {
	if w.err != nil {
		return bizErrText(w.err)
	}
	return bizTerminal(w.t, w.last)
}

func bizErrText(err error) string {
	if executor.WorkflowTaskBlockedCause(err) != "" {
		return "blocked(" + executor.WorkflowTaskBlockedCause(err) + "): " + err.Error()
	}
	return "run-failed: " + err.Error()
}

func bizTerminal(t *testing.T, commands []history.Command) string {
	for _, c := range commands {
		switch c.Type {
		case history.CmdCompleteWorkflow:
			return canonicalJSON(t, c.Output)
		case history.CmdFailWorkflow:
			return "error: " + c.Error
		case history.CmdCancelWorkflow:
			return "cancelled"
		case history.CmdContinueAsNew:
			return "continue-as-new " + c.WorkflowType + " " + canonicalJSON(t, c.Input)
		}
	}
	return "pending: " + bizCommandSummary(commands)
}

func bizCommandSummary(commands []history.Command) string {
	parts := []string{}
	for _, c := range commands {
		parts = append(parts, fmt.Sprintf("%s(%s%s%s%s)", c.Type, c.ActivityType, c.TimerID, c.ChildID, c.UpdateID))
	}
	return strings.Join(parts, ",")
}

// sticky 用 sticky 会话按同样的批次重新执行，返回最终结果；某一批命令与冷重放不同时返回差异。
func (w *bizWorld) sticky() string {
	w.t.Helper()
	session := NewExecutionSession(context.Background(), w.wf, 1)
	defer session.Close()
	if _, err := session.Restore(context.Background(), NewSliceHistoryIterator(nil)); err != nil {
		w.t.Fatal(err)
	}
	var last []history.Command
	for i, b := range w.batches {
		act, err := session.Activate(context.Background(), history.WorkflowActivation{ActivationID: fmt.Sprint("a", i), ExecutionGeneration: 1, Events: b.delivered, ThroughEventID: b.through})
		if err != nil || b.coldErr != nil {
			if (err == nil) != (b.coldErr == nil) {
				return fmt.Sprintf("batch %d: sticky err=%v, cold err=%v", i, err, b.coldErr)
			}
			return bizErrText(err)
		}
		if got, want := bizCommandSummary(act.Commands), bizCommandSummary(b.commands); got != want {
			return fmt.Sprintf("batch %d commands differ: sticky=%s cold=%s", i, got, want)
		}
		if bizTerminal(w.t, act.Commands) != bizTerminal(w.t, b.commands) {
			return fmt.Sprintf("batch %d terminal differ: sticky=%s cold=%s", i, bizTerminal(w.t, act.Commands), bizTerminal(w.t, b.commands))
		}
		last = act.Commands
		if b.committed == nil && i == len(w.batches)-1 {
			break
		}
		if err := session.Commit(history.WorkflowTaskCompletionAck{Accepted: true, CommittedThroughEventID: b.through + int64(len(b.committed)), CommittedEvents: b.committed,
			ActivationID: act.ActivationID, CommandDigest: act.CommandDigest, ExecutionGeneration: 1}); err != nil {
			return fmt.Sprintf("batch %d commit: %v", i, err)
		}
	}
	return bizTerminal(w.t, last)
}

// check 比较冷重放与 sticky 的结果与期望值。
func (w *bizWorld) check(name, want string) {
	w.t.Helper()
	cold := w.outcome()
	sticky := w.sticky()
	if cold != want || sticky != want {
		w.t.Errorf("MISMATCH %s:\n  Go     = %s\n  冷重放 = %s\n  sticky = %s", name, want, cold, sticky)
	} else {
		w.t.Logf("ok %s = %s", name, cold)
	}
}
