package program_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
)

// PayloadTypes 从 Program 的语句与类型表找出每类载荷位置解码用的类型。
func TestPayloadTypesFindsEveryPosition(t *testing.T) {
	types, err := program.PayloadTypes(programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order")))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for position, decoded := range types.Positions {
		names := make([]string, 0, len(decoded))
		for _, item := range decoded {
			names = append(names, item.Name)
		}
		got[position.String()] = strings.Join(names, ",")
	}
	want := map[string]string{
		"workflow input":   "order.Input",
		`signal "approve"`: "order.Signal",
		`signal "later"`:   "order.Later",
		`input of pending activity "orders.charge"`: "order.ChargeInput",
		`result of activity "orders.charge"`:        "order.ChargeOutput",
		`result of child workflow "child-flow"`:     "order.ChildOutput",
		`argument 0 of update "approve-update"`:     "order.UpdateArg",
		"side effect value":                         "order.SideValue",
		`mutable side effect "flag"`:                "order.FlagValue",
		`signal "pick"`:                             "order.Picked",
		`signal "named-signal"`:                     "order.Named",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PayloadTypes() = %v\nwant %v", got, want)
	}
	// 名字取自输入字段的 Signal 无法确定位置：记为未确定的解码处，带种类、类型、路径与原因。
	if len(types.Unresolved) != 1 {
		t.Fatalf("unresolved = %v, want one site", types.Unresolved)
	}
	site := types.Unresolved[0]
	if site.Position != (program.PayloadPosition{Kind: program.PayloadSignal}) || site.Type != "order.Dynamic" || site.Wire == nil {
		t.Fatalf("unresolved site = %+v", site)
	}
	if want := "a signal with a non-constant name decoded into order.Dynamic at program[24].set.err.Get (the signal name is not a constant)"; site.String() != want {
		t.Fatalf("unresolved site = %q, want %q", site.String(), want)
	}
}

// ComparePayloadTypes 只看 JSON 结构：改包路径不算变化；某个位置的类型改了字段名时只报这个位置，消息带新旧类型名
// 与字段路径；新增字段兼容。
func TestComparePayloadTypes(t *testing.T) {
	current := programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order"))
	renamed := func(name string) program.WLangTypeDescriptor {
		return programtest.Record("example.com/order2", name, programtest.Field{JSON: "orderId", Type: "string"})
	}
	t.Run("Happy Path: a package rename keeps every position compatible", func(t *testing.T) {
		got, err := program.ComparePayloadTypes(current, programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2")))
		if err != nil || len(got) != 0 {
			t.Fatalf("ComparePayloadTypes() = %v, %v", got, err)
		}
	})
	t.Run("Happy Path: an added field is compatible", func(t *testing.T) {
		added := programtest.Record("example.com/order2", "order.Signal",
			programtest.Field{JSON: "order_id", Type: "string"}, programtest.Field{JSON: "note", Type: "string"})
		got, err := program.ComparePayloadTypes(current, programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2", added)))
		if err != nil || len(got) != 0 {
			t.Fatalf("ComparePayloadTypes() = %v, %v", got, err)
		}
	})
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
		t.Run("Error Case: renamed field in "+test.typeName, func(t *testing.T) {
			candidate := programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2", renamed(test.typeName)))
			got, err := program.ComparePayloadTypes(current, candidate)
			if err != nil {
				t.Fatal(err)
			}
			want := []program.PayloadIncompatibility{{
				Position: got0Position(got), Current: test.typeName, Candidate: test.typeName,
				Issues: []protocol.WireIncompatibility{{Path: "order_id", Reason: "field is missing in the new type (removed or renamed)"}},
			}}
			if len(got) != 1 || !reflect.DeepEqual(got, want) || got[0].Position.String() != test.position {
				t.Fatalf("ComparePayloadTypes() = %+v, want one incompatibility at %s", got, test.position)
			}
			wantMessage := test.position + ": " + test.typeName + " -> " + test.typeName + ": order_id: field is missing in the new type (removed or renamed)"
			if got[0].String() != wantMessage {
				t.Fatalf("message = %q, want %q", got[0].String(), wantMessage)
			}
		})
	}
	t.Run("Edge Case: a position the current program does not decode is not checked", func(t *testing.T) {
		withoutSignal := programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order"))
		withoutSignal.Program.Program = withoutSignal.Program.Program[:1]
		got, err := program.ComparePayloadTypes(withoutSignal, programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2", renamed("order.Signal"))))
		if err != nil {
			t.Fatal(err)
		}
		// 只剩候选 Program 里未确定的 Signal 解码处（当前 Program 没有它，签名不同）；signal "approve" 不判定。
		if len(got) != 1 || got[0].Unresolved == nil || got[0].UnresolvedIn != "candidate" {
			t.Fatalf("ComparePayloadTypes() = %v", got)
		}
	})
	t.Run("Error Case: an unresolved site whose type changed is reported with its path", func(t *testing.T) {
		got, err := program.ComparePayloadTypes(current, programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2", renamed("order.Dynamic"))))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{
			"candidate program: a signal with a non-constant name decoded into order.Dynamic at program[24].set.err.Get (the signal name is not a constant); signal types differ between the two versions",
			"current program: a signal with a non-constant name decoded into order.Dynamic at program[24].set.err.Get (the signal name is not a constant); signal types differ between the two versions",
		}
		if len(got) != len(want) {
			t.Fatalf("ComparePayloadTypes() = %v, want %v", got, want)
		}
		for i := range want {
			if got[i].String() != want[i] || got[i].Position != (program.PayloadPosition{Kind: program.PayloadSignal}) {
				t.Fatalf("incompatibility %d = %q, want %q", i, got[i], want[i])
			}
		}
	})
	t.Run("Happy Path: an unresolved site does not block when its type keeps its shape", func(t *testing.T) {
		added := programtest.Record("example.com/order2", "order.Signal",
			programtest.Field{JSON: "order_id", Type: "string"}, programtest.Field{JSON: "note", Type: "string"})
		got, err := program.ComparePayloadTypes(current, programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes("example.com/order2", added)))
		if err != nil || len(got) != 0 {
			t.Fatalf("ComparePayloadTypes() = %v, %v", got, err)
		}
	})
	t.Run("Error Case: a site of unknown type is reported when any type shape changes", func(t *testing.T) {
		unknown := func(pkgPath string, override ...program.WLangTypeDescriptor) *program.WorkflowConfig {
			wf := programtest.PayloadWorkflow("payload-flow", programtest.PayloadTypes(pkgPath, override...))
			// dynamic 改成没有声明类型的变量：它的类型无法确定。
			statements := append([]any(nil), wf.Program.Program...)
			statements[23] = map[string]any{"let": map[string]any{"dynamic": map[string]any{"GetInfo": []any{map[string]any{"pkg": "workflow"}, map[string]any{"var": "ctx"}}}}}
			wf.Program.Program = statements
			return wf
		}
		base := unknown("example.com/order")
		got, err := program.ComparePayloadTypes(base, unknown("example.com/order2"))
		if err != nil || len(got) != 0 {
			t.Fatalf("package rename: ComparePayloadTypes() = %v, %v", got, err)
		}
		got, err = program.ComparePayloadTypes(base, unknown("example.com/order2", renamed("order.SideValue")))
		if err != nil {
			t.Fatal(err)
		}
		var unresolved []string
		for _, item := range got {
			if item.Unresolved != nil {
				unresolved = append(unresolved, item.String())
			}
		}
		want := "candidate program: a signal with a non-constant name decoded into an unknown type at program[24].set.err.Get (the signal name is not a constant); signal types differ between the two versions"
		if len(unresolved) != 2 || unresolved[0] != want {
			t.Fatalf("unresolved incompatibilities = %v, want %q and its current-program twin", unresolved, want)
		}
	})
}

func got0Position(got []program.PayloadIncompatibility) program.PayloadPosition {
	if len(got) == 0 {
		return program.PayloadPosition{}
	}
	return got[0].Position
}
