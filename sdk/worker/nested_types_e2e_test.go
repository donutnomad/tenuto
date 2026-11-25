package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

// nestedParentSrc 的每个 struct 类型只出现在一处 Workflow 边界：子 Workflow 输入、Activity 输入、Activity 结果、
// Signal 载荷、Update 参数与结果、Query 结果、continue-as-new 参数。嵌套混合 struct、slice、map、指针与 array，
// 含递归类型。
const nestedParentSrc = `package nest

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/nest"
)

type ChildIn struct {
	Label  string   ` + "`json:\"label\"`" + `
	Levels []Level1 ` + "`json:\"levels\"`" + `
}

type Level1 struct {
	Name  string            ` + "`json:\"name\"`" + `
	Inner *Level2           ` + "`json:\"inner\"`" + `
	ByKey map[string]Level2 ` + "`json:\"by_key\"`" + `
}

type Level2 struct {
	Grid  [2]Level3 ` + "`json:\"grid\"`" + `
	Items []Level3  ` + "`json:\"items\"`" + `
}

type Level3 struct {
	Tags map[string][]string ` + "`json:\"tags\"`" + `
	Deep *Level4             ` + "`json:\"deep\"`" + `
}

type Level4 struct {
	Note  string  ` + "`json:\"note\"`" + `
	Flags [2]bool ` + "`json:\"flags\"`" + `
}

type ActIn struct {
	Batch []Packet ` + "`json:\"batch\"`" + `
}

type Packet struct {
	ID    string          ` + "`json:\"id\"`" + `
	Parts map[string]Part ` + "`json:\"parts\"`" + `
}

type Part struct {
	Sizes    [3]int64 ` + "`json:\"sizes\"`" + `
	Sub      *Part    ` + "`json:\"sub\"`" + `
	Children []Part   ` + "`json:\"children\"`" + `
}

type ActOut struct {
	Batch []PacketOut ` + "`json:\"batch\"`" + `
}

type PacketOut struct {
	ID    string             ` + "`json:\"id\"`" + `
	Parts map[string]PartOut ` + "`json:\"parts\"`" + `
}

type PartOut struct {
	Sizes    [3]int64  ` + "`json:\"sizes\"`" + `
	Sub      *PartOut  ` + "`json:\"sub\"`" + `
	Children []PartOut ` + "`json:\"children\"`" + `
}

type Signal struct {
	Route []Hop ` + "`json:\"route\"`" + `
}

type Hop struct {
	Station string             ` + "`json:\"station\"`" + `
	Next    *Hop               ` + "`json:\"next\"`" + `
	Meta    map[string][]int64 ` + "`json:\"meta\"`" + `
}

type UpdIn struct {
	Rows [][]Cell ` + "`json:\"rows\"`" + `
}

type Cell struct {
	V   string ` + "`json:\"v\"`" + `
	Alt *Cell  ` + "`json:\"alt\"`" + `
}

type UpdOut struct {
	Count int     ` + "`json:\"count\"`" + `
	Rows  [][]Cell ` + "`json:\"rows\"`" + `
}

type QueryState struct {
	Stage string  ` + "`json:\"stage\"`" + `
	Steps []QStep ` + "`json:\"steps\"`" + `
}

type QStep struct {
	Name string           ` + "`json:\"name\"`" + `
	Subs map[string]QStep ` + "`json:\"subs\"`" + `
}

type Restart struct {
	Round int     ` + "`json:\"round\"`" + `
	Carry []Crumb ` + "`json:\"carry\"`" + `
}

type Crumb struct {
	Path []string ` + "`json:\"path\"`" + `
	Mark *Crumb   ` + "`json:\"mark\"`" + `
}

func Parent(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	if input["round"] == nil {
		return nil, workflow.ContinueAsNew(ctx, "nest-parent", Restart{Round: 1, Carry: []Crumb{{Path: []string{"a", "b"}}, {}}})
	}
	updated := false
	var update UpdOut
	if err := workflow.SetUpdateHandler(ctx, "rows", func(ctx workflow.Context, rows UpdIn) (UpdOut, error) {
		update = UpdOut{Count: len(rows.Rows), Rows: rows.Rows}
		updated = true
		return update, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "state", func(ctx workflow.Context, _ map[string]any) map[string]any {
		return map[string]any{"state": QueryState{Stage: "running", Steps: []QStep{{Name: "a", Subs: map[string]QStep{"b": {Name: "b"}}}}}}
	}); err != nil {
		return nil, err
	}

	var signal Signal
	if err := workflow.WaitSignal(ctx, "route").Get(ctx, &signal); err != nil {
		return nil, err
	}

	var act ActOut
	if err := workflow.ExecuteActivity(ctx, nest.Echo, ActIn{Batch: []Packet{
		{ID: "p1", Parts: map[string]Part{"x": {Sizes: [3]int64{1, 2, 3}, Children: []Part{{Sizes: [3]int64{4, 5, 6}}}}}},
		{ID: "p2"},
	}}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &act); err != nil {
		return nil, err
	}

	var child map[string]any
	if err := workflow.ExecuteChildWorkflow(ctx, "nest-child", ChildIn{
		Label: "deep",
		Levels: []Level1{{
			Name: "l1",
			ByKey: map[string]Level2{"k": {
				Grid:  [2]Level3{{Tags: map[string][]string{"t": []string{"x", "y"}}}, {}},
				Items: []Level3{{Tags: map[string][]string{"empty": nil}}},
			}},
		}},
	}).Get(ctx, &child); err != nil {
		return nil, err
	}

	if err := workflow.Await(ctx, func() bool { return updated }); err != nil {
		return nil, err
	}
	return map[string]any{"carry": input["carry"], "signal": signal, "activity": act, "child": child, "update": update}, nil
}
`

const nestedChildSrc = `package nest

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func Child(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	return input, nil
}
`

// 只作为子 Workflow 输入、Activity 输入与结果、Signal 载荷、Update 参数与结果、Query 结果、continue-as-new
// 参数出现的深层嵌套 struct 类型经 sdk/client 与 sdk/worker 在内存 Server 上跨过边界后原样回来；用历史从头重放没有
// 非确定性错误；两次生成的产物内容版本相同。
func TestNestedTypesCrossEveryWorkflowBoundary(t *testing.T) {
	hostPort, stop := startExampleServer()
	defer stop()
	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	parent, err := sdkgen.GenerateFromSource([]byte(nestedParentSrc), sdkgen.Options{WorkflowType: "nest-parent", FuncName: "Parent", Filename: "nest.go"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := sdkgen.GenerateFromSource([]byte(nestedParentSrc), sdkgen.Options{WorkflowType: "nest-parent", FuncName: "Parent", Filename: "nest.go"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := program.WorkflowConfigVersion(parent.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	second, err := program.WorkflowConfigVersion(again.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(parent.ProgramJSON, again.ProgramJSON) || first != second {
		t.Fatalf("two generations of the same source differ: content version %s vs %s", first, second)
	}
	for _, id := range []string{"nest.ChildIn", "nest.Level4", "nest.ActIn", "nest.Part", "nest.Signal", "nest.Hop", "nest.UpdIn", "nest.Cell", "nest.UpdOut", "nest.QueryState", "nest.QStep", "nest.Restart", "nest.Crumb"} {
		if _, ok := parent.Workflow.Program.Types[id]; !ok {
			t.Errorf("type table misses %s", id)
		}
	}
	for symbol, ref := range parent.Workflow.Activities {
		ref.Type, ref.TaskQueue = symbol, "nest"
		parent.Workflow.Activities[symbol] = ref
	}
	child, err := sdkgen.GenerateFromSource([]byte(nestedChildSrc), sdkgen.Options{WorkflowType: "nest-child", FuncName: "Child", Filename: "child.go"})
	if err != nil {
		t.Fatal(err)
	}

	registry := activity.NewRegistry()
	if err := registry.Register("nest.Echo", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return in, nil
	}); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(c, worker.Options{
		Workflows:          []registration.Definition{definitionFor(t, parent.Workflow), definitionFor(t, child.Workflow)},
		Activities:         registry,
		ActivityTaskQueues: []string{"nest"},
		Identity:           "nest-worker",
		BuildID:            "nest-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Stop()
	if err := w.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "nest-1"}, "nest-parent", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// Query 在 continue-as-new 之后的 Run 里才有处理器；之后的 Signal 与 Update 都发给这个 Run。
	query := waitForQuery(ctx, t, c, "nest-1")
	if want := `{"state":{"stage":"running","steps":[{"name":"a","subs":{"b":{"name":"b","subs":null}}}]}}`; query != want {
		t.Fatalf("query = %s\nwant %s", query, want)
	}

	signal := `{"route":[{"station":"a","next":{"station":"b","next":null,"meta":null},"meta":{"m":[1,2]}},{"station":"c","next":null,"meta":{}}]}`
	if err := c.SignalWorkflow(ctx, "nest-1", "", "route", json.RawMessage(signal)); err != nil {
		t.Fatal(err)
	}

	rows := `{"rows":[[{"v":"a","alt":{"v":"b","alt":null}}],[],null]}`
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: "nest-1", UpdateName: "rows", Args: []any{json.RawMessage(rows)}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		t.Fatal(err)
	}
	var updated json.RawMessage
	if err := handle.Get(ctx, &updated); err != nil {
		t.Fatal(err)
	}
	wantUpdate := `{"count":3,"rows":[[{"alt":{"alt":null,"v":"b"},"v":"a"}],[],null]}`
	if got := canonicalJSON(t, updated); got != wantUpdate {
		t.Fatalf("update result = %s\nwant %s", got, wantUpdate)
	}

	var out json.RawMessage
	if err := run.Get(ctx, &out); err != nil {
		t.Fatal(err)
	}
	want := canonicalJSON(t, json.RawMessage(`{
		"carry": [{"path":["a","b"],"mark":null},{"path":null,"mark":null}],
		"signal": `+signal+`,
		"activity": {"batch":[
			{"id":"p1","parts":{"x":{"sizes":[1,2,3],"sub":null,"children":[{"sizes":[4,5,6],"sub":null,"children":null}]}}},
			{"id":"p2","parts":null}
		]},
		"child": {"label":"deep","levels":[{"name":"l1","inner":null,"by_key":{"k":{
			"grid":[{"tags":{"t":["x","y"]},"deep":null},{"tags":null,"deep":null}],
			"items":[{"tags":{"empty":null},"deep":null}]
		}}}]},
		"update": `+wantUpdate+`
	}`))
	if got := canonicalJSON(t, out); got != want {
		t.Fatalf("result = %s\nwant %s", got, want)
	}

	// 当前 Run（continue-as-new 之后的那个）的历史从头重放，产出同样的完成命令。
	var events []historypkg.Event
	for it := c.GetWorkflowHistory(ctx, "nest-1", ""); it.HasNext(); {
		event, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, *event)
	}
	last := -1
	for i, event := range events {
		if event.WorkflowTaskCompleted != nil {
			last = i
		}
	}
	commands, err := interpreter.NewWorkflowExecutor(parent.Workflow, events[:last]).Execute()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("replay produced %+v, want the completion", commands)
	}
	replayed, err := json.Marshal(commands[0].Output)
	if err != nil {
		t.Fatal(err)
	}
	if got := canonicalJSON(t, replayed); got != want {
		t.Fatalf("replayed result = %s\nwant %s", got, want)
	}
}

// waitForQuery 重试查询，直到 Run 登记了 Query 处理器。
func waitForQuery(ctx context.Context, t *testing.T, c client.Client, workflowID string) string {
	t.Helper()
	for {
		value, err := c.QueryWorkflow(ctx, workflowID, "", "state")
		if err == nil {
			var raw json.RawMessage
			if err := value.Get(&raw); err != nil {
				t.Fatal(err)
			}
			return canonicalJSON(t, raw)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("query: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func canonicalJSON(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
