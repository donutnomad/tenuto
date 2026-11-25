package worker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

const activityIDWorkflowSrc = `package orders

import (
	"fmt"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/orders"
)

func Settle(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	var ids []string
	call := func(activityID string) error {
		var out map[string]any
		if err := workflow.ExecuteActivity(ctx, orders.Echo, map[string]any{}, workflow.ActivityOptions{
			ActivityID:          activityID,
			StartToCloseTimeout: time.Minute,
		}).Get(ctx, &out); err != nil {
			return err
		}
		id, _ := out["id"].(string)
		ids = append(ids, id)
		return nil
	}
	for i := 0; i < 2; i++ {
		if err := call(fmt.Sprintf("charge-%d", i)); err != nil {
			return nil, err
		}
	}
	for i := 0; i < 2; i++ {
		if err := call("poll"); err != nil {
			return nil, err
		}
	}
	if err := call(""); err != nil {
		return nil, err
	}
	return map[string]any{"ids": strings.Join(ids, ",")}, nil
}
`

// ActivityOptions.ActivityID 经 sdk/client 与 sdk/worker 在内存 Server 上跑完：Sprintf 计算的 ID、关闭后复用的 ID
// 与缺省 ID 都出现在 ActivityScheduled 事件与 activity.GetInfo(ctx).ActivityID 里；用历史从头重放没有不确定性错误。
func TestActivityIDReachesHistoryAndActivityInfo(t *testing.T) {
	hostPort, stop := startExampleServer()
	defer stop()
	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	generated, err := sdkgen.GenerateFromSource([]byte(activityIDWorkflowSrc), sdkgen.Options{WorkflowType: "settle", FuncName: "Settle", Filename: "orders.go"})
	if err != nil {
		t.Fatal(err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type, ref.TaskQueue = symbol, "orders"
		generated.Workflow.Activities[symbol] = ref
	}
	registry := activity.NewRegistry()
	if err := registry.Register("orders.Echo", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		return map[string]any{"id": activity.GetInfo(ctx).ActivityID}, nil
	}); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(c, worker.Options{
		Workflows:          []registration.Definition{definitionFor(t, generated.Workflow)},
		Activities:         registry,
		ActivityTaskQueues: []string{"orders"},
		Identity:           "orders-worker",
		BuildID:            "orders-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.Start(ctx)
	defer w.Stop()
	if err := w.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "settle-1"}, "settle", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := run.Get(ctx, &out); err != nil {
		t.Fatal(err)
	}
	const want = "charge-0,charge-1,poll,poll,#a5"
	if out["ids"] != want {
		t.Fatalf("activity.GetInfo(ctx).ActivityID values = %v, want %s", out["ids"], want)
	}

	var events []historypkg.Event
	var scheduled []string
	for it := c.GetWorkflowHistory(ctx, "settle-1", ""); it.HasNext(); {
		event, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, *event)
		if event.ActivityScheduled != nil {
			scheduled = append(scheduled, event.ActivityScheduled.ActivityID)
		}
	}
	if got := strings.Join(scheduled, ","); got != want {
		t.Fatalf("ActivityScheduled ActivityIDs = %s, want %s", got, want)
	}
	last := -1
	for i, event := range events {
		if event.WorkflowTaskCompleted != nil {
			last = i
		}
	}
	commands, err := interpreter.NewWorkflowExecutor(generated.Workflow, events[:last]).Execute()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow || commands[0].Output["ids"] != want {
		t.Fatalf("replay produced %+v, want the same completion", commands)
	}
}
