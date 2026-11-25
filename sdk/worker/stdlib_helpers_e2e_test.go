package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/fstest"
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

const quoteWorkflowSrc = `package quotes

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/quotes"
)

type Line struct {
	SKU   string
	Qty   int
	Cents int
}

func (l Line) Total() int { return l.Qty * l.Cents }

func parseLine(raw string) (Line, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return Line{}, fmt.Errorf("line %s: want sku:qty:cents", raw)
	}
	qty, err := strconv.Atoi(parts[1])
	if err != nil {
		return Line{}, fmt.Errorf("line %s: %w", raw, err)
	}
	cents, err := strconv.Atoi(parts[2])
	if err != nil {
		return Line{}, fmt.Errorf("line %s: %w", raw, err)
	}
	return Line{SKU: strings.ToUpper(strings.TrimSpace(parts[0])), Qty: qty, Cents: cents}, nil
}

func price(ctx workflow.Context, sku string) (int, error) {
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, quotes.Discount, map[string]any{"sku": sku}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return 0, err
	}
	percent, _ := out["percent"].(string)
	return strconv.Atoi(percent)
}

func Quote(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	raw, _ := input["lines"].(string)
	total := 0
	var skus []string
	for _, item := range strings.Split(raw, ";") {
		line, err := parseLine(item)
		if err != nil {
			return nil, err
		}
		percent, err := price(ctx, line.SKU)
		if err != nil {
			return nil, err
		}
		total += line.Total() * (100 - percent) / 100
		skus = append(skus, line.SKU)
	}
	return map[string]any{
		"summary": fmt.Sprintf("%s: %d.%02d", strings.Join(skus, ","), total/100, total%100),
		"rounded": math.Round(2.5),
	}, nil
}
`

// 一个用到标准库函数与同包 helper 的 Workflow 经 sdk/client 与 sdk/worker 在内存 Server 上跑完，
// 再用它的历史从头重放：重放产出同样的完成命令，没有不确定性错误。
func TestStdlibAndHelpersWorkflowRunsAndReplays(t *testing.T) {
	hostPort, stop := startExampleServer()
	defer stop()
	c, err := client.Dial(client.Options{HostPort: hostPort})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	generated, err := sdkgen.GenerateFromSource([]byte(quoteWorkflowSrc), sdkgen.Options{WorkflowType: "quote", FuncName: "Quote", Filename: "quotes.go"})
	if err != nil {
		t.Fatal(err)
	}
	for symbol, ref := range generated.Workflow.Activities {
		ref.Type, ref.TaskQueue = symbol, "quotes"
		generated.Workflow.Activities[symbol] = ref
	}
	registry := activity.NewRegistry()
	if err := registry.Register("quotes.Discount", func(_ context.Context, input map[string]any) (map[string]any, error) {
		if input["sku"] == "AB-1" {
			return map[string]any{"percent": "10"}, nil
		}
		return map[string]any{"percent": "0"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(c, worker.Options{
		Workflows:          []registration.Definition{definitionFor(t, generated.Workflow)},
		Activities:         registry,
		ActivityTaskQueues: []string{"quotes"},
		Identity:           "quotes-worker",
		BuildID:            "quotes-v1",
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

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "quote-1"}, "quote", map[string]any{"lines": " ab-1 :3:250;cd-2:1:99"})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := run.Get(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if out["summary"] != "AB-1,CD-2: 7.74" || fmt.Sprint(out["rounded"]) != "3" {
		t.Fatalf("output = %+v, want summary AB-1,CD-2: 7.74 and rounded 3", out)
	}

	var events []historypkg.Event
	for it := c.GetWorkflowHistory(ctx, "quote-1", ""); it.HasNext(); {
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
	if last < 0 || events[len(events)-1].WorkflowCompleted == nil {
		t.Fatalf("history does not end with a completed workflow task and WorkflowCompleted: %+v", events)
	}
	commands, err := interpreter.NewWorkflowExecutor(generated.Workflow, events[:last]).Execute()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow || commands[0].Output["summary"] != out["summary"] {
		t.Fatalf("replay produced %+v, want the same completion", commands)
	}
}

func definitionFor(t *testing.T, config *program.WorkflowConfig) registration.Definition {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: data}}}
}
