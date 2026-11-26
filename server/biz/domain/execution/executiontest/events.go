package executiontest

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// EventRoundTripContract 断言存储层原样保存每一种事件的每一个字段：对 history.Event 的每个子事件，
// 用反射把全部字段填成非零值，追加后读回，要求与写入前（经 execution.NormalizeEventFailures 归一化）深度相等。
// 字段由反射枚举，事件类型或字段新增后自动进入断言。
func EventRoundTripContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	eventType := reflect.TypeOf(history.Event{})
	for i := 0; i < eventType.NumField(); i++ {
		field := eventType.Field(i)
		if field.Type.Kind() != reflect.Pointer {
			continue
		}
		// UpdateOutcome 的 Success 与 Failure 二选一，两种形状各测一次。
		variants := []bool{false}
		if field.Name == "WorkflowUpdateCompleted" {
			variants = []bool{false, true}
		}
		for _, outcomeFailure := range variants {
			name := field.Name
			if outcomeFailure {
				name += "/FailureOutcome"
			}
			t.Run(name, func(t *testing.T) {
				fill := eventFiller{outcomeFailure: outcomeFailure}
				event := history.Event{Timestamp: time.Date(2026, 11, 10, 1, 2, 3, 123456789, time.UTC)}
				sub := reflect.New(field.Type.Elem())
				fill.fill(sub.Elem(), nil)
				fill.requireFilled(t, sub.Elem(), field.Name, nil)
				reflect.ValueOf(&event).Elem().Field(i).Set(sub)

				want, err := execution.NormalizeEventFailures(event)
				if err != nil {
					t.Fatal(err)
				}
				manager := newManager(t)
				runID := "run-" + field.Name
				createRun(t, manager, execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-" + field.Name, WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
				want.ID, err = manager.Append(runID, runID, runID, event)
				if err != nil {
					t.Fatal(err)
				}
				events, err := manager.GetEvents(runID)
				if err != nil {
					t.Fatal(err)
				}
				if len(events) != 1 {
					t.Fatalf("events = %d, want 1", len(events))
				}
				if !reflect.DeepEqual(events[0], want) {
					t.Fatalf("event changed in storage:\n got %s\nwant %s", describeEvent(events[0]), describeEvent(want))
				}
			})
		}
	}
}

// EventNumberTypesContract 断言事件载荷里的数字读回后类型固定：整数是 int64（超过 int64 的是 uint64），
// 带小数的是 float64，与写入时的 Go 类型和存储后端无关。
func EventNumberTypesContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	createRun(t, manager, execution.NewRun(execution.NewRunOptions{ID: "run-numbers", WorkflowID: "wf-numbers", WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
	input := map[string]any{
		"int": 5, "whole_float": float64(3), "fraction": 1.5,
		"above_2_53": int64(1)<<53 + 1, "above_int64": uint64(math.MaxUint64),
		"nested": map[string]any{"list": []any{7, int32(8)}},
	}
	if _, err := manager.Append("run-numbers", "run-numbers", "run-numbers", history.NewWorkflowSignaledEvent("numbers", input)); err != nil {
		t.Fatal(err)
	}
	events, err := manager.GetEvents("run-numbers")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"int": int64(5), "whole_float": int64(3), "fraction": 1.5,
		"above_2_53": int64(1)<<53 + 1, "above_int64": uint64(math.MaxUint64),
		"nested": map[string]any{"list": []any{int64(7), int64(8)}},
	}
	if got := events[0].WorkflowSignaled.Input; !reflect.DeepEqual(got, want) {
		t.Fatalf("signal input = %#v, want %#v", got, want)
	}
}

func describeEvent(event history.Event) string {
	data, err := history.CanonicalEventJSON(event)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s (go: %#v)", data, reflect.ValueOf(event).FieldByName(string(event.EventType())).Interface())
}

var (
	payloadType = reflect.TypeOf(protocol.Payload{})
	outcomeType = reflect.TypeOf(protocol.UpdateOutcome{})
	anyMapType  = reflect.TypeOf(map[string]any{})
)

// eventFiller 把一个值的全部字段填成非零值。递归类型（Failure.Cause）只展开一层。
type eventFiller struct {
	outcomeFailure bool
	next           int64
}

func (f *eventFiller) number() int64 {
	f.next++
	return int64(1)<<53 + f.next
}

func onStack(stack []reflect.Type, typ reflect.Type) bool {
	for _, seen := range stack {
		if seen == typ {
			return true
		}
	}
	return false
}

func (f *eventFiller) fill(value reflect.Value, stack []reflect.Type) {
	typ := value.Type()
	switch typ {
	case payloadType:
		value.Set(reflect.ValueOf(protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: fmt.Appendf(nil, `{"n":%d}`, f.number())}))
		return
	case outcomeType:
		outcome := protocol.UpdateOutcome{}
		if f.outcomeFailure {
			failure := reflect.New(reflect.TypeOf(protocol.Failure{}))
			f.fill(failure.Elem(), stack)
			outcome.Failure = failure.Interface().(*protocol.Failure)
		} else {
			outcome.Success = &protocol.Payload{}
			f.fill(reflect.ValueOf(outcome.Success).Elem(), stack)
		}
		value.Set(reflect.ValueOf(outcome))
		return
	case anyMapType:
		value.Set(reflect.ValueOf(map[string]any{
			"text": "value", "integer": f.number(), "above_int64": uint64(math.MaxUint64), "fraction": 1.5, "flag": true,
			"list": []any{f.number(), "item"}, "nested": map[string]any{"integer": f.number()},
		}))
		return
	}
	switch typ.Kind() {
	case reflect.String:
		value.SetString(fmt.Sprintf("text-%d", f.number()))
	case reflect.Int, reflect.Int64:
		value.SetInt(f.number())
	case reflect.Float64:
		value.SetFloat(2.5)
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Pointer:
		if onStack(stack, typ.Elem()) {
			return
		}
		value.Set(reflect.New(typ.Elem()))
		f.fill(value.Elem(), stack)
	case reflect.Struct:
		stack = append(stack, typ)
		for i := 0; i < typ.NumField(); i++ {
			f.fill(value.Field(i), stack)
		}
	case reflect.Slice:
		value.Set(reflect.MakeSlice(typ, 1, 1))
		f.fill(value.Index(0), stack)
	case reflect.Map:
		value.Set(reflect.MakeMap(typ))
		element := reflect.New(typ.Elem()).Elem()
		f.fill(element, stack)
		value.SetMapIndex(reflect.ValueOf("key").Convert(typ.Key()), element)
	default:
		panic(fmt.Sprintf("event filler: unsupported kind %s (%s); teach the filler this type", typ.Kind(), typ))
	}
}

// requireFilled 断言 fill 没有留下零值字段，新增字段的类型 fill 不认识时在这里失败。
func (f *eventFiller) requireFilled(t *testing.T, value reflect.Value, path string, stack []reflect.Type) {
	t.Helper()
	typ := value.Type()
	if typ == outcomeType {
		outcome := value.Interface().(protocol.UpdateOutcome)
		if (outcome.Success != nil) == (outcome.Failure != nil) {
			t.Fatalf("%s: outcome must carry exactly one of success and failure", path)
		}
		if outcome.Failure != nil {
			f.requireFilled(t, reflect.ValueOf(outcome.Failure).Elem(), path+".Failure", stack)
		}
		return
	}
	switch typ.Kind() {
	case reflect.Pointer:
		if onStack(stack, typ.Elem()) {
			return
		}
		if value.IsNil() {
			t.Fatalf("%s is nil", path)
		}
		f.requireFilled(t, value.Elem(), path, stack)
	case reflect.Struct:
		stack = append(stack, typ)
		for i := 0; i < typ.NumField(); i++ {
			f.requireFilled(t, value.Field(i), path+"."+typ.Field(i).Name, stack)
		}
	case reflect.Slice:
		if value.Len() == 0 {
			t.Fatalf("%s is empty", path)
		}
		if typ.Elem().Kind() != reflect.Uint8 {
			f.requireFilled(t, value.Index(0), path+"[0]", stack)
		}
	case reflect.Map:
		if value.Len() == 0 {
			t.Fatalf("%s is empty", path)
		}
	default:
		if value.IsZero() {
			t.Fatalf("%s is zero", path)
		}
	}
}

// EventsByDedupeKeysContract 断言按去重键读取只返回去重键属于 keys 的事件，按事件 ID 升序；Update 的记录按
// execution.UpdateDedupeKeys 读到的事件组装，与在完整 History 上组装的结果相同；终态事件随同一次读取返回，
// 已接受、未完成的 Update 按 Run 结束失败。
func EventsByDedupeKeysContract(t *testing.T, newManager func(t *testing.T) execution.ExecutionManager) {
	manager := newManager(t)
	const runID = "run-dedupe-keys"
	createRun(t, manager, execution.NewRun(execution.NewRunOptions{ID: runID, WorkflowID: "wf-dedupe-keys", WorkflowConfigVersion: "v1", CreatedAt: contractNow}))
	payload, err := protocol.NewJSONPayload(map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RunInTx(func(tx execution.ExecutionTx) error {
		for _, event := range []history.Event{
			history.NewWorkflowSignaledEvent("noise", nil),
			history.NewWorkflowUpdateAcceptedEvent("place", "u1", nil),
			history.NewWorkflowSignaledEvent("noise", nil),
			history.NewWorkflowUpdateAcceptedEvent("place", "u2", nil),
			{WorkflowUpdateCompleted: &history.WorkflowUpdateCompletedEvent{Name: "place", UpdateID: "u1", Outcome: &protocol.UpdateOutcome{Success: &payload}}},
			history.NewWorkflowCompletedEvent(nil),
		} {
			if _, _, err := tx.AppendIdempotent(runID, runID, runID, event); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	all, err := manager.GetEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, updateID := range []string{"u1", "u2", "missing"} {
		events, err := manager.GetEventsByDedupeKeys(runID, execution.UpdateDedupeKeys(updateID))
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < len(events); i++ {
			if events[i].ID <= events[i-1].ID {
				t.Fatalf("%s: events are not in event ID order: %d then %d", updateID, events[i-1].ID, events[i].ID)
			}
		}
		got, gotOK := execution.UpdateRecordFromEvents(updateID, events)
		want, wantOK := execution.UpdateRecordFromEvents(updateID, all)
		if gotOK != wantOK || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: record from dedupe keys = %+v (%v), from full history = %+v (%v)", updateID, got, gotOK, want, wantOK)
		}
	}
	if events, err := manager.GetEventsByDedupeKeys(runID, execution.UpdateDedupeKeys("u1")); err != nil || len(events) != 3 {
		t.Fatalf("u1 events = %d, err = %v, want accepted, completed and the terminal event", len(events), err)
	}
	if record, ok := execution.UpdateRecordFromEvents("u2", all); !ok || record.Outcome == nil || record.Outcome.Failure == nil {
		t.Fatalf("u2 = %+v (found %v), want the failure from the run closing", record, ok)
	}
}
