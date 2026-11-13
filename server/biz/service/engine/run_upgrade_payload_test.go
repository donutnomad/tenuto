package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// 升级前的载荷结构兼容检查
// ============================================
// 测试策略:
// - Error Case: 每类载荷位置（Workflow 输入、Signal、future 变量里的 Signal、Selector 回调里的 Signal、名字是变量
//   拼接的 Signal、未关闭 Activity 的输入、Activity 结果、子 Workflow 结果、Update 参数、SideEffect、
//   MutableSideEffect）的类型在 latest 里改了字段名，History 里有这类载荷时 auto_upgrade Run 不换版，以 ConfigError
//   暂停，消息写出位置、类型与字段路径
// - Error Case: 名字无法静态确定的 Signal 解码处的类型改了字段名，History 里有 Signal 时同样暂停，消息写出 WLang
//   路径与「无法证明」；History 里没有 Signal 时照常换版
// - Edge Case: History 里没有改动过的位置时照常换版；同一对版本的对照结果被缓存
// - Happy Path: 暂停后 set-policy pinned 留在旧内容；发布兼容内容后 set-policy auto_upgrade 换到它
// ============================================

const payloadFlow = "payload-flow"

func registerPayloadFlow(t *testing.T, server *Server, types map[string]program.WLangTypeDescriptor) string {
	t.Helper()
	ref, err := server.Definitions().RegisterWorkflowInNamespace("default", programtest.PayloadWorkflow(payloadFlow, types))
	if err != nil {
		t.Fatal(err)
	}
	return ref.Version
}

// startPayloadRun 在 version 上建一个 auto_upgrade Run，History 由 events 写入；ActivityCompleted 的
// ScheduledEventID 为 0 时指向它之前最近的 ActivityScheduled。
func startPayloadRun(t *testing.T, server *Server, version string, events ...history.Event) *execution.Run {
	t.Helper()
	run := &execution.Run{
		ID: "payload-run", Namespace: "default", WorkflowID: payloadFlow, WorkflowType: payloadFlow,
		WorkflowConfigVersion: version, OriginalWorkflowConfigVersion: version,
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(run)) }); err != nil {
		t.Fatal(err)
	}
	var lastScheduled int64
	for _, event := range events {
		if completed := event.ActivityCompleted; completed != nil && completed.ScheduledEventID == 0 {
			copied := *completed
			copied.ScheduledEventID = lastScheduled
			event.ActivityCompleted = &copied
		}
		id, err := server.mgr.Append(run.ID, run.ID, run.ID, event)
		if err != nil {
			t.Fatal(err)
		}
		if event.ActivityScheduled != nil {
			lastScheduled = id
		}
	}
	return run
}

// payloadHistory 含每类载荷位置的一条载荷；第二个 orders.charge 还没有关闭。
func payloadHistory() []history.Event {
	args := protocol.Payloads{{Encoding: "json/plain-v1", Data: []byte(`{"order_id":"o-1"}`)}}
	record := map[string]any{"order_id": "o-1"}
	return []history.Event{
		{WorkflowStarted: &history.WorkflowStartedEvent{WorkflowID: payloadFlow, Name: payloadFlow, Input: record}},
		{WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "approve", Input: record}},
		{WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "later", Input: record}},
		{WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "pick", Input: record}},
		{WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "named-signal", Input: record}},
		{ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "orders.charge", Input: record}},
		{ActivityCompleted: &history.ActivityCompletedEvent{ActivityID: "#a1", ActivityType: "orders.charge", Output: record}},
		{ChildWorkflowStarted: &history.ChildWorkflowStartedEvent{Name: "child-flow", ChildID: "#c1", Input: record}},
		{ChildWorkflowCompleted: &history.ChildWorkflowCompletedEvent{ChildID: "#c1", Output: record}},
		{WorkflowUpdateAccepted: &history.WorkflowUpdateAcceptedEvent{Name: "approve-update", UpdateID: "u1", Args: &args}},
		{MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "SideEffect", MarkerID: "se1", Value: map[string]any{"value": record}}},
		{MarkerRecorded: &history.MarkerRecordedEvent{MarkerName: "MutableSideEffect", MarkerID: "flag_1", Value: map[string]any{"value": record}}},
		{ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a2", ActivityType: "orders.charge", Input: record}},
	}
}

func renamedOrderID(languageName string) program.WLangTypeDescriptor {
	return programtest.Record("example.com/v2/order", languageName, programtest.Field{JSON: "orderId", Type: "string"})
}

func TestAutoUpgradeRefusesPayloadsTheCandidateCannotRead(t *testing.T) {
	for _, test := range []struct {
		typeName string
		position string
	}{
		{"order.Input", "workflow input"},
		{"order.Signal", `signal "approve"`},
		{"order.Later", `signal "later"`},
		{"order.ChargeInput", `input of pending activity "orders.charge"`},
		{"order.ChargeOutput", `result of activity "orders.charge"`},
		{"order.ChildOutput", `result of child workflow "child-flow"`},
		{"order.UpdateArg", `argument 0 of update "approve-update"`},
		{"order.SideValue", "side effect value"},
		{"order.FlagValue", `mutable side effect "flag"`},
		{"order.Picked", `signal "pick"`},
		{"order.Named", `signal "named-signal"`},
	} {
		t.Run("Error Case: "+test.position, func(t *testing.T) {
			server := newTestServer()
			v1 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v1/order"))
			run := startPayloadRun(t, server, v1, payloadHistory()...)
			v2 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v2/order", renamedOrderID(test.typeName)))

			want := "auto upgrade to " + v2 + " refused: payloads recorded in history cannot be read by the candidate program: " +
				test.position + ": " + test.typeName + " -> " + test.typeName + ": order_id: field is missing in the new type (removed or renamed); "
			if conflict := server.upgradeCandidate(run).PayloadConflict; conflict == nil || !strings.HasPrefix(conflict.Error(), want) {
				t.Fatalf("payload conflict = %v, want prefix %q", conflict, want)
			}
			choice, err := chooseProgramFor(server, run)
			if err != nil {
				t.Fatal(err)
			}
			if choice.Version != v1 || !strings.HasPrefix(choice.ConfigError, want) {
				t.Fatalf("choice = %+v, want %s paused", choice, v1)
			}
			paused, err := server.mgr.Get(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if paused.WorkflowConfigVersion != v1 || !strings.HasPrefix(paused.ConfigError, want) {
				t.Fatalf("run version %s config error %q", paused.WorkflowConfigVersion, paused.ConfigError)
			}
		})
	}
}

// 名字取自输入字段的 Signal 无法确定位置：它的类型改了字段名时，History 里有任何 Signal 就不换版并暂停。
func TestAutoUpgradeRefusesUnresolvedSignalSiteChanges(t *testing.T) {
	server := newTestServer()
	v1 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v1/order"))
	signaled := []history.Event{payloadHistory()[0], {WorkflowSignaled: &history.WorkflowSignaledEvent{Name: "o-1", Input: map[string]any{"order_id": "o-1"}}}}
	run := startPayloadRun(t, server, v1, signaled...)
	v2 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v2/order", renamedOrderID("order.Dynamic")))

	want := "auto upgrade to " + v2 + " refused: the payload check cannot prove that payloads recorded in history stay readable: " +
		"candidate program: a signal with a non-constant name decoded into order.Dynamic at program[24].set.err.Get (the signal name is not a constant); signal types differ between the two versions; "
	choice, err := chooseProgramFor(server, run)
	if err != nil {
		t.Fatal(err)
	}
	if choice.Version != v1 || !strings.HasPrefix(choice.ConfigError, want) {
		t.Fatalf("choice = %+v, want %s paused with prefix %q", choice, v1, want)
	}

	t.Run("Edge Case: no signal in history", func(t *testing.T) {
		server := newTestServer()
		v1 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v1/order"))
		run := startPayloadRun(t, server, v1, payloadHistory()[0])
		v2 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v2/order", renamedOrderID("order.Dynamic")))
		if candidate := server.upgradeCandidate(run); candidate.Version != v2 || candidate.PayloadConflict != nil {
			t.Fatalf("candidate = %+v", candidate)
		}
	})
}

func TestAutoUpgradePayloadCheckLooksOnlyAtRecordedPayloads(t *testing.T) {
	server := newTestServer()
	v1 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v1/order"))
	// 只有 Workflow 输入：approve Signal 的类型改了，但 History 里没有这个 Signal。
	run := startPayloadRun(t, server, v1, payloadHistory()[0])
	v2 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v2/order", renamedOrderID("order.Signal")))
	for range 2 {
		candidate := server.upgradeCandidate(run)
		if candidate.Version != v2 || candidate.PayloadConflict != nil || candidate.PayloadCheckErr != nil {
			t.Fatalf("candidate = %+v", candidate)
		}
	}
	if cached := len(server.payloadCompat.results); cached != 1 {
		t.Fatalf("payload comparisons cached = %d, want 1", cached)
	}
	choice, err := chooseProgramFor(server, run)
	if err != nil {
		t.Fatal(err)
	}
	if choice.Version != v2 || choice.ConfigError != "" {
		t.Fatalf("choice = %+v, want %s", choice, v2)
	}
}

func TestPayloadConflictRecoversBySetPolicy(t *testing.T) {
	server := newTestServer()
	v1 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v1/order"))
	run := startPayloadRun(t, server, v1, payloadHistory()...)
	registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v2/order", renamedOrderID("order.Signal")))
	if choice, err := chooseProgramFor(server, run); err != nil || choice.ConfigError == "" {
		t.Fatalf("choice = %+v, %v; want a pause", choice, err)
	}

	t.Run("Happy Path: pinned stays on the current content", func(t *testing.T) {
		if _, err := server.SetRunPolicy(context.Background(), "default", run.ID, execution.RunPolicyPinned); err != nil {
			t.Fatal(err)
		}
		pinned, err := server.mgr.Get(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		choice, err := chooseProgramFor(server, pinned)
		if err != nil || choice != (execution.ProgramVersionChoice{Version: v1}) {
			t.Fatalf("choice = %+v, %v; want %s", choice, err, v1)
		}
	})

	t.Run("Happy Path: a compatible version is adopted after auto_upgrade resumes", func(t *testing.T) {
		added := programtest.Record("example.com/v3/order", "order.Signal",
			programtest.Field{JSON: "order_id", Type: "string"}, programtest.Field{JSON: "orderId", Type: "string"})
		v3 := registerPayloadFlow(t, server, programtest.PayloadTypes("example.com/v3/order", added))
		if _, err := server.SetRunPolicy(context.Background(), "default", run.ID, execution.RunPolicyAutoUpgrade); err != nil {
			t.Fatal(err)
		}
		resumed, err := server.mgr.Get(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		choice, err := chooseProgramFor(server, resumed)
		if err != nil || choice != (execution.ProgramVersionChoice{Version: v3}) {
			t.Fatalf("choice = %+v, %v; want %s", choice, err, v3)
		}
	})
}
