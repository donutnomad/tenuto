package interpreter

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// SideEffect / MutableSideEffect 返回 struct 的持久化
// ============================================
// 测试策略:
// - Happy Path: 编译后的 workflow，SideEffect 与 MutableSideEffect 的闭包返回 struct。首次执行解码出的值、
//   marker 经存储（JSON）往返后 replay 解码出的值，都等于闭包返回的原值
// - 守护: marker 里持久化的是字段内容（JSON tag 为键）
// ============================================

const sideEffectStructSrc = `package flows

import "github.com/donutnomad/tenuto/sdk/workflow"

type Quote struct {
	Region string ` + "`json:\"region\"`" + `
	Amount int64  ` + "`json:\"amount\"`" + `
}

type Result struct {
	Region        string ` + "`json:\"region\"`" + `
	Amount        int64  ` + "`json:\"amount\"`" + `
	MutableRegion string ` + "`json:\"mutable_region\"`" + `
	MutableAmount int64  ` + "`json:\"mutable_amount\"`" + `
}

func W(ctx workflow.Context, input map[string]any) (Result, error) {
	var quote Quote
	if err := workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return Quote{Region: "sg", Amount: 4200}
	}).Get(ctx, &quote); err != nil {
		return Result{}, err
	}
	var mutable Quote
	if err := workflow.MutableSideEffect(ctx, "quote", func(ctx workflow.Context) any {
		return Quote{Region: "eu", Amount: 77}
	}, func(a, b any) bool { return false }).Get(ctx, &mutable); err != nil {
		return Result{}, err
	}
	return Result{Region: quote.Region, Amount: quote.Amount, MutableRegion: mutable.Region, MutableAmount: mutable.Amount}, nil
}
`

func sideEffectStructOutput(t *testing.T, commands []history.Command) map[string]any {
	t.Helper()
	cmd, ok := findCompleteCmd(commands)
	if !ok {
		t.Fatalf("workflow must complete, got %+v", commands)
	}
	// 输出经一次 JSON 往返归一数字类型，首次执行与 replay 用同一种表示比较。
	return dbRoundTrip(t, cmd.Output)
}

func TestSideEffectStructValueSurvivesPersistenceAndReplay(t *testing.T) {
	nodes := compileWLangSource(t, []byte(sideEffectStructSrc), "sdk-se-struct", "W", "se_struct.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	want := map[string]any{"region": "sg", "amount": float64(4200), "mutable_region": "eu", "mutable_amount": float64(77)}

	// 首次执行：两个 marker 与完成命令同批产出。
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := sideEffectStructOutput(t, commands); !reflect.DeepEqual(got, want) {
		t.Fatalf("first run decoded %v, want %v", got, want)
	}

	// marker 落库是 JSON：struct 的字段必须在里面。
	events := []history.Event{started, history.NewWorkflowTaskCompletedEvent(0, "")}
	for i, command := range commands {
		if command.Type != history.CmdRecordMarker {
			continue
		}
		stored, err := json.Marshal(command.Input)
		if err != nil {
			t.Fatal(err)
		}
		persisted := dbRoundTrip(t, command.Input)
		value, _ := persisted["value"].(map[string]any)
		if len(value) != 2 {
			t.Fatalf("marker %s/%s persisted as %s, want the struct fields", command.MarkerName, command.MarkerID, stored)
		}
		events = append(events, history.Event{ID: int64(i + 2), Timestamp: time.Now(), MarkerRecorded: &history.MarkerRecordedEvent{
			MarkerName: command.MarkerName, MarkerID: command.MarkerID, Value: persisted,
		}})
	}
	if len(events) != 4 {
		t.Fatalf("want SideEffect and MutableSideEffect markers, got %+v", commands)
	}

	// replay：marker 从存储读回，解码出的值与首次执行一致。
	replayed, err := NewWorkflowExecutor(nodes, events).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if got := sideEffectStructOutput(t, replayed); !reflect.DeepEqual(got, want) {
		t.Fatalf("replay decoded %v, want %v", got, want)
	}
}

const mutableSideEffectStructEqualsSrc = `package flows

import "github.com/donutnomad/tenuto/sdk/workflow"

type Quote struct {
	Region string ` + "`json:\"region\"`" + `
	Amount int64  ` + "`json:\"amount\"`" + `
}

func W(ctx workflow.Context, input map[string]any) (Quote, error) {
	var first Quote
	if err := workflow.MutableSideEffect(ctx, "quote", func(ctx workflow.Context) any {
		return Quote{Region: "sg", Amount: 4200}
	}, func(a, b any) bool {
		next := a.(Quote)
		prev := b.(Quote)
		return next.Amount == prev.Amount
	}).Get(ctx, &first); err != nil {
		return Quote{}, err
	}
	var second Quote
	if err := workflow.MutableSideEffect(ctx, "quote", func(ctx workflow.Context) any {
		return Quote{Region: "sg", Amount: 4200}
	}, func(a, b any) bool {
		next := a.(Quote)
		prev := b.(Quote)
		return next.Amount == prev.Amount
	}).Get(ctx, &second); err != nil {
		return Quote{}, err
	}
	return second, nil
}
`

// Edge Case: MutableSideEffect 的 equals 拿到的旧值是从 marker 解回的同类型 struct，
// 值未变化时第二次调用复用旧 marker，不产新命令。
func TestMutableSideEffectStructEqualsSeesPersistedValue(t *testing.T) {
	nodes := compileWLangSource(t, []byte(mutableSideEffectStructEqualsSrc), "sdk-mse-struct", "W", "mse_struct.go")
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	markers := 0
	for _, command := range commands {
		if command.Type == history.CmdRecordMarker {
			markers++
		}
	}
	if markers != 1 {
		t.Fatalf("an unchanged struct value must reuse the first marker, got %d markers: %+v", markers, commands)
	}
	want := map[string]any{"region": "sg", "amount": float64(4200)}
	if got := sideEffectStructOutput(t, commands); !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
}
