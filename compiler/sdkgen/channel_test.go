package sdkgen

import (
	"encoding/json"
	"strings"
	"testing"
)

// ============================================
// workflow.Channel sdkgen 识别 + go2wlang 透传测试
// ============================================
// 测试策略：
// - Happy Path : ch := NewChannel/NewBufferedChannel + ch.Send/Receive/Close 透传成 host 调用
//                （{"Send":[ch,...]} 等，接收者=channel 变量 → 分派 channelHandle 方法）
// - 回归       : channel 操作展示态放行——不 lower 成展示 Step（类比 wg.Go），且不报 unsupported
// ============================================

// Happy Path: ch.Send/Receive/Close 经 go2wlang 透传成 host 调用；channel 变量被识别、
// 操作不进 Activities 表、不报 unsupported。
func TestGenerateFromSource_Channel_TranspilesToHostCall(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func ChannelWorkflow(ctx workflow.Context) error {
	ch := workflow.NewBufferedChannel(ctx, 2)
	ch.Send(ctx, "a")
	var out map[string]any
	more := ch.Receive(ctx, &out)
	_ = more
	ch.Close(ctx)
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "ch-flow", FuncName: "ChannelWorkflow", Filename: "ch_workflow.go"})
	if err != nil {
		t.Fatalf("expected channel ops to transpile, got error: %v", err)
	}
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"NewBufferedChannel":[`, `"Send":[`, `"Receive":[`, `"Close":[`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("expected %s host call in program, got: %s", want, raw)
		}
	}
	// 回归：channel 操作不是 Activity，不进 Activities 表。
	for symbol := range result.Workflow.Activities {
		switch symbol {
		case "Send", "Receive", "Close", "NewBufferedChannel", "NewChannel":
			t.Fatalf("channel 操作不应进 Activities 表，got %q", symbol)
		}
	}
}

// 回归: 非缓冲 NewChannel 也被识别（覆盖 recordChannelAssignment 的 NewChannel 分支）。
func TestGenerateFromSource_UnbufferedChannel_Recognized(t *testing.T) {
	src := []byte(`package flows

import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func UnbufferedChannelWorkflow(ctx workflow.Context) error {
	ch := workflow.NewChannel(ctx)
	ch.Send(ctx, "x")
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "unbuf-ch", FuncName: "UnbufferedChannelWorkflow", Filename: "unbuf_ch.go"})
	if err != nil {
		t.Fatalf("expected unbuffered channel ops to transpile, got error: %v", err)
	}
	raw, err := json.Marshal(result.Workflow.Program.Program)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"NewChannel":[`, `"Send":[`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("expected %s host call in program, got: %s", want, raw)
		}
	}
}
