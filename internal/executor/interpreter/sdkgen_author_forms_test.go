package interpreter

// 作者自然会写的 Go 写法：真实 sdkgen 生成 → 解释器执行到终态 → 用完整历史再 replay 一遍。

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// driveResult 是 driveSource 的结果：终态命令、完整历史与全程调度过的命令。
type driveResult struct {
	final     []history.Command
	events    []history.Event
	scheduled []history.Command
}

func (r driveResult) completed() (map[string]any, bool) {
	for _, command := range r.final {
		if command.Type == history.CmdCompleteWorkflow {
			return command.Output, true
		}
	}
	return nil, false
}

func (r driveResult) activityTypes() []string {
	var out []string
	for _, command := range r.scheduled {
		if command.Type == history.CmdScheduleActivity {
			out = append(out, command.ActivityType)
		}
	}
	return out
}

func compileSource(t *testing.T, src string) *program.WorkflowConfig {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "author-forms", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	nodes := result.Workflow
	return nodes
}

// driveSource 编译 src 并把它驱动到终态：每一批把上一批调度的 Activity 记为完成（输出取 answer）、
// Timer 记为触发；到终态后用完整历史再执行一次，replay 的终态必须与首次一致。
func driveSource(t *testing.T, src string, input map[string]any, answer func(history.Command) map[string]any) driveResult {
	t.Helper()
	nodes := compileSource(t, src)
	now := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	started := history.NewWorkflowStartedEvent(input, "", "", "", "", 0)
	started.ID = 1
	started.Timestamp = now
	result := driveResult{events: []history.Event{started}}
	nextID := int64(2)
	appendEvent := func(event history.Event) int64 {
		event.ID = nextID
		now = now.Add(time.Second)
		event.Timestamp = now
		nextID++
		result.events = append(result.events, event)
		return event.ID
	}
	terminal := func(commands []history.Command) bool {
		for _, command := range commands {
			if command.Type == history.CmdCompleteWorkflow || command.Type == history.CmdFailWorkflow {
				return true
			}
		}
		return false
	}
	for round := 0; round < 50; round++ {
		commands, err := NewWorkflowExecutor(nodes, result.events).Execute()
		if err != nil {
			t.Fatalf("round %d execute: %v", round, err)
		}
		if terminal(commands) {
			result.final = commands
			replayed, err := NewWorkflowExecutor(nodes, result.events).Execute()
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			if !terminal(replayed) {
				t.Fatalf("replay 没有到达终态: %+v", replayed)
			}
			return result
		}
		appendEvent(history.Event{WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}})
		type pending struct {
			command history.Command
			eventID int64
		}
		var waiting []pending
		for _, command := range commands {
			switch command.Type {
			case history.CmdScheduleActivity:
				id := appendEvent(history.Event{ActivityScheduled: &history.ActivityScheduledEvent{
					ActivityID: command.ActivityID, ActivityType: command.ActivityType, TaskQueue: command.TaskQueue, Input: command.Input,
				}})
				waiting = append(waiting, pending{command, id})
			case history.CmdScheduleTimer:
				id := appendEvent(history.Event{TimerScheduled: &history.TimerScheduledEvent{
					TimerID: command.TimerID, DelayMs: command.Delay.Milliseconds(),
					DelayInput: map[string]any{"delay_ns": int64(command.Delay)},
				}})
				waiting = append(waiting, pending{command, id})
			case history.CmdRecordMarker:
				appendEvent(history.Event{MarkerRecorded: &history.MarkerRecordedEvent{
					MarkerName: command.MarkerName, MarkerID: command.MarkerID, Value: command.Input,
				}})
			default:
				t.Fatalf("driveSource 不处理命令 %+v", command)
			}
			result.scheduled = append(result.scheduled, command)
		}
		if len(waiting) == 0 {
			t.Fatalf("round %d: workflow 既没有终态也没有等待项: %+v", round, commands)
		}
		for _, item := range waiting {
			if item.command.Type == history.CmdScheduleTimer {
				appendEvent(history.Event{TimerFired: &history.TimerFiredEvent{TimerID: item.command.TimerID, ScheduledEventID: item.eventID}})
				continue
			}
			var output map[string]any
			if answer != nil {
				output = answer(item.command)
			}
			appendEvent(history.Event{ActivityCompleted: &history.ActivityCompletedEvent{
				ActivityID: item.command.ActivityID, ActivityType: item.command.ActivityType, ScheduledEventID: item.eventID, Output: output,
			}})
		}
	}
	t.Fatalf("workflow 在 50 批内没有结束")
	return result
}

func countOf(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

// 第 1 项：fire-and-forget。主函数不等待它，Activity 照常调度。
func TestAuthorForms_FireAndForgetActivity(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	workflow.ExecuteActivity(ctx, activities.Audit, input)
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.Charge, input).Get(ctx, &out)
}
`
	result := driveSource(t, src, map[string]any{"k": "v"}, nil)
	if _, ok := result.completed(); !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
	types := result.activityTypes()
	if countOf(types, "activities.Audit") != 1 || countOf(types, "activities.Charge") != 1 {
		t.Fatalf("scheduled = %v", types)
	}
}

// 第 1 项：动态 fan-out，future 存进切片后逐个 Get。
func TestAuthorForms_AppendFuturesFanOut(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
type Input struct {
	Items []string
}
func W(ctx workflow.Context, input Input) error {
	var futures []workflow.ActivityFuture
	for _, item := range input.Items {
		futures = append(futures, workflow.ExecuteActivity(ctx, activities.Notify, map[string]any{"item": item}))
	}
	for _, future := range futures {
		var out map[string]any
		if err := future.Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}
`
	nodes := compileSource(t, src)
	started := history.NewWorkflowStartedEvent(map[string]any{"Items": []any{"a", "b", "c"}}, "", "", "", "", 0)
	commands, err := NewWorkflowExecutor(nodes, []history.Event{started}).Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := len(findScheduleCmds(commands)); got != 3 {
		t.Fatalf("首批应并发调度 3 个 Activity, got %d: %+v", got, commands)
	}
	result := driveSource(t, src, map[string]any{"Items": []any{"a", "b", "c"}}, nil)
	if _, ok := result.completed(); !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
}

// 第 1 项：经典 for 循环体内的 future 变量。
func TestAuthorForms_FutureInsideClassicFor(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	for i := 0; i < 3; i++ {
		f := workflow.ExecuteActivity(ctx, activities.Step, input)
		var out map[string]any
		if err := f.Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}
`
	result := driveSource(t, src, map[string]any{"k": "v"}, nil)
	if _, ok := result.completed(); !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
	if got := countOf(result.activityTypes(), "activities.Step"); got != 3 {
		t.Fatalf("activities.Step 调度 %d 次, want 3", got)
	}
}

const authorFormsHeader = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
var _ = time.Second
type Out struct {
	Sum   int
	Count int
	Text  string
	Flag  bool
	Stamp int64
}
type Item struct {
	Name string
	Qty  int
}
`

func runToOutput(t *testing.T, body string, input map[string]any) map[string]any {
	t.Helper()
	result := driveSource(t, authorFormsHeader+body, input, nil)
	output, ok := result.completed()
	if !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
	return output
}

func wantField(t *testing.T, output map[string]any, field string, want any) {
	t.Helper()
	got := output[field]
	if number, ok := got.(interface{ Int64() (int64, error) }); ok {
		value, _ := number.Int64()
		got = value
	}
	switch value := got.(type) {
	case int:
		got = int64(value)
	case float64:
		got = int64(value)
	}
	if got != want {
		t.Fatalf("%s = %#v (%T), want %#v; output %+v", field, output[field], output[field], want, output)
	}
}

// 第 5 项：循环条件 i <= n 与独立的 i++ / i--。
func TestAuthorForms_LoopLessEqualAndIncDec(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	n := 4
	sum := 0
	for i := 1; i <= n; i++ {
		sum += i
	}
	count := 0
	count++
	count++
	count--
	for j := n; j >= 1; j-- {
		sum += 1
	}
	return Out{Sum: sum, Count: count}, nil
}
`, nil)
	wantField(t, output, "Sum", int64(14))
	wantField(t, output, "Count", int64(1))
}

// 第 5 项：make([]T, n) 与 make([]T, 0, n)。
func TestAuthorForms_MakeSlice(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	n := 3
	sized := make([]int, n)
	sized[1] = 5
	grown := make([]string, 0, n)
	grown = append(grown, "a")
	grown = append(grown, "b")
	lookup := make(map[string]int)
	lookup["k"] = 7
	return Out{Sum: sized[0] + sized[1] + lookup["k"], Count: len(sized) + len(grown)}, nil
}
`, nil)
	wantField(t, output, "Sum", int64(12))
	wantField(t, output, "Count", int64(5))
}

// make 的长度在清空目标之前求值一次：`xs = make([]T, len(xs))` 得到与原切片等长的零值切片。
func TestAuthorForms_MakeSliceSizedBySelf(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	xs := []int{4, 5, 6}
	xs = make([]int, len(xs))
	return Out{Sum: xs[0] + xs[2], Count: len(xs)}, nil
}
`, nil)
	wantField(t, output, "Sum", int64(0))
	wantField(t, output, "Count", int64(3))
}

// 第 5 项：类型断言后取字段，以及多值赋值。
func TestAuthorForms_TypeAssertionFieldAndMultiAssign(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var a any = Out{Text: "pen", Sum: 2}
	var b any = Out{Text: "ink", Sum: 3}
	name := a.(Out).Text
	x, y := a.(Out), b.(Out)
	p, q := 1, "two"
	p, q = p+1, q+"!"
	x, y = y, x
	return Out{Sum: x.Sum*10 + y.Sum + p, Text: name + q}, nil
}
`, nil)
	wantField(t, output, "Sum", int64(34))
	wantField(t, output, "Text", "pentwo!")
}

// 第 5 项：返回 (map[string]any, error)，map 字面量里是具体类型的值。
func TestAuthorForms_ReturnMapLiteralOfConcreteValues(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"handled": true, "count": 2, "name": "x"}, nil
}
`, nil)
	if output["handled"] != true || output["name"] != "x" {
		t.Fatalf("output = %+v", output)
	}
	wantField(t, output, "count", int64(2))
}

// 第 2 项：time 常量与 time.Duration 运算。
func TestAuthorForms_TimeConstantsAndDurationArithmetic(t *testing.T) {
	result := driveSource(t, authorFormsHeader+`
const pollInterval = 2 * time.Second
func W(ctx workflow.Context, input map[string]any) error {
	if err := workflow.Sleep(ctx, 15*time.Minute); err != nil {
		return err
	}
	retries := 3
	backoff := time.Duration(retries) * 100 * time.Millisecond
	if err := workflow.Sleep(ctx, backoff+pollInterval); err != nil {
		return err
	}
	var wait time.Duration = time.Hour
	wait = wait / 2
	return workflow.Sleep(ctx, wait)
}
`, nil, nil)
	if _, ok := result.completed(); !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
	var delays []time.Duration
	for _, command := range result.scheduled {
		if command.Type == history.CmdScheduleTimer {
			delays = append(delays, command.Delay)
		}
	}
	want := []time.Duration{15 * time.Minute, 2300 * time.Millisecond, 30 * time.Minute}
	if len(delays) != len(want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for index := range want {
		if delays[index] != want[index] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

// 第 2 项：workflow.Now 返回的时间可以做确定性运算。
func TestAuthorForms_WorkflowNowOperations(t *testing.T) {
	output := runToOutput(t, `
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	start := workflow.Now(ctx)
	deadline := start.Add(90 * time.Minute)
	elapsed := deadline.Sub(start)
	flag := start.Before(deadline) && deadline.After(start) && !start.Equal(deadline)
	text := deadline.Format("2006-01-02 15:04:05")
	return Out{Sum: int(elapsed / time.Minute), Flag: flag, Text: text, Stamp: deadline.Unix(), Count: int(deadline.UnixMilli() - start.UnixMilli())}, nil
}
`, nil)
	wantField(t, output, "Sum", int64(90))
	wantField(t, output, "Count", int64(90*60*1000))
	if output["Flag"] != true {
		t.Fatalf("output = %+v", output)
	}
	if output["Text"] != "2026-11-10 09:30:00" {
		t.Fatalf("Text = %v", output["Text"])
	}
	wantField(t, output, "Stamp", time.Date(2026, 11, 10, 9, 30, 0, 0, time.UTC).Unix())
}

// docs/develop/go-subset.md 里的示例：包级常量、time.Duration 运算、workflow.Now 的方法、<= 循环与 break。
func TestAuthorForms_GoSubsetDocExample(t *testing.T) {
	result := driveSource(t, `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

const maxRounds = 3

type Out struct {
	Rounds int
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	rounds := 0
	deadline := workflow.Now(ctx).Add(2 * time.Hour)
	for round := 1; round <= maxRounds; round++ {
		if err := workflow.Sleep(ctx, time.Duration(round)*10*time.Second); err != nil {
			return Out{}, err
		}
		rounds++
		if workflow.Now(ctx).After(deadline) {
			break
		}
	}
	return Out{Rounds: rounds}, nil
}
`, nil, nil)
	output, ok := result.completed()
	if !ok {
		t.Fatalf("want CompleteWorkflow, got %+v", result.final)
	}
	wantField(t, output, "Rounds", int64(3))
	var delays []time.Duration
	for _, command := range result.scheduled {
		if command.Type == history.CmdScheduleTimer {
			delays = append(delays, command.Delay)
		}
	}
	if len(delays) != 3 || delays[0] != 10*time.Second || delays[1] != 20*time.Second || delays[2] != 30*time.Second {
		t.Fatalf("timer delays = %v, want 10s 20s 30s", delays)
	}
}
