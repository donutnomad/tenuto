package cancellation_test

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	cancellation "github.com/donutnomad/tenuto/docs/examples/cancellation"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/sdk/workflow"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 等待付款时取消：AwaitPayment 确认取消，ReleaseSeat 在 disconnected context 上完成，Run 以 Cancelled 结束。
func TestBookTripReleasesSeatWhenCancelled(t *testing.T) {
	src, err := os.ReadFile("booking.go")
	if err != nil {
		t.Fatal(err)
	}
	generated, err := sdkgen.GenerateFromSource(src, sdkgen.Options{WorkflowType: "book-trip", FuncName: "BookTrip", Filename: "booking.go"})
	if err != nil {
		t.Fatal(err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type = "booking." + symbol[strings.LastIndex(symbol, ".")+1:]
		generated.Workflow.Activities[symbol] = ref
	}
	registry := activity.NewRegistry()
	for name, fn := range map[string]func(context.Context, map[string]any) (map[string]any, error){
		"booking.HoldSeat": cancellation.HoldSeat, "booking.AwaitPayment": cancellation.AwaitPayment, "booking.ReleaseSeat": cancellation.ReleaseSeat,
	} {
		if err := registry.Register(name, fn); err != nil {
			t.Fatal(err)
		}
	}
	executions := memory.NewExecutionManager()
	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: executions}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	defer server.Stop()
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, generated.Workflow); err != nil {
		t.Fatal(err)
	}
	serverCtx, stopServer := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(runtimehttp.NewRuntimeHTTPHandler(server))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	defer httpServer.Close()
	defer stopServer()

	c, err := client.Dial(client.Options{HostPort: httpServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w, err := worker.New(c, worker.Options{Activities: registry, WorkflowConcurrency: 1, ActivityConcurrency: 4, Identity: "booking-worker", BuildID: "booking-test"})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(context.Background())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "trip-t-1"}, "book-trip", map[string]any{"trip": "t-1"})
	if err != nil {
		t.Fatal(err)
	}

	// 心跳记在 Activity 的当前状态里，不写 History：等 AwaitPayment 记下第一次心跳。
	waitFor(t, func() bool {
		states, err := executions.ListActivityStates(run.GetRunID())
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range states {
			if state.LastHeartbeatAt != nil {
				return true
			}
		}
		return false
	})
	if err := c.CancelWorkflow(ctx, run.GetID(), run.GetRunID()); err != nil {
		t.Fatal(err)
	}
	// Workflow 确认取消并返回取消错误：Get 返回的错误满足 IsCanceledError。
	if err := run.Get(ctx, nil); !workflow.IsCanceledError(err) {
		t.Fatalf("Get = %v, want a canceled error", err)
	}
	if completed := count(t, c, run, historypkg.EventActivityCompleted); completed != 2 {
		t.Fatalf("completed activities = %d, want HoldSeat and ReleaseSeat", completed)
	}
}

func count(t *testing.T, c client.Client, run client.WorkflowRun, typ historypkg.EventType) int {
	t.Helper()
	n := 0
	events := c.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID())
	for events.HasNext() {
		event, err := events.Next()
		if err != nil {
			t.Fatal(err)
		}
		if event.EventType() == typ {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
