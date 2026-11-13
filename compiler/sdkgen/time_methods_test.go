package sdkgen_test

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// Worker 没有绑定的 time.Time 方法与 time.Duration 的方法在生成期带 file:line 拒绝，报错列出可用的方法；
// 绑定的方法在解释器里按 Go 语义运行。
func TestTimeMethodsInWorkflow(t *testing.T) {
	generate := func(body string) (sdkgen.Result, error) {
		src := `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func W(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	now := workflow.Now(ctx)
	` + body + `
}
`
		return sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{WorkflowID: "time-methods", FuncName: "W", Filename: "w.go"})
	}

	rejected := []struct {
		name string
		body string
		want []string
	}{
		{"time.Time.Year", "return map[string]any{\"y\": now.Year()}, nil", []string{"w.go:11:", "time.Time 的方法 Year", "Add、After、Before、Equal、Format、String、Sub、Unix、UnixMilli"}},
		{"time.Time method value", "f := now.Weekday\n\t_ = f\n\treturn nil, nil", []string{"w.go:11:", "time.Time 的方法 Weekday"}},
		{"time.Duration.Seconds", "d := 3 * time.Second\n\treturn map[string]any{\"s\": d.Seconds()}, nil", []string{"w.go:12:", "time.Duration 的方法 Seconds", "int64 纳秒"}},
		{"Sub result method", "return map[string]any{\"ms\": now.Sub(now).Milliseconds()}, nil", []string{"w.go:11:", "time.Duration 的方法 Milliseconds"}},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			_, err := generate(tc.body)
			if err == nil {
				t.Fatal("want generation error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
		})
	}

	t.Run("bound methods run", func(t *testing.T) {
		result, err := generate(`later := now.Add(90 * time.Second)
	return map[string]any{
		"sub":    later.Sub(now),
		"after":  later.After(now),
		"before": later.Before(now),
		"equal":  now.Equal(now),
		"unix":   later.Unix(),
		"milli":  later.UnixMilli(),
		"format": later.Format(time.DateTime),
		"string": later.String(),
	}, nil`)
		if err != nil {
			t.Fatal(err)
		}
		started := historypkg.NewWorkflowStartedEvent(map[string]any{}, "", "", "time-methods", "", 0)
		started.ID = 1
		started.Timestamp = time.Unix(1_800_000_000, 0)
		commands, err := interpreter.NewWorkflowExecutor(result.Workflow, []historypkg.Event{started}).Execute()
		if err != nil {
			t.Fatal(err)
		}
		if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
			t.Fatalf("want completion, got %+v", commands)
		}
		later := started.Timestamp.Add(90 * time.Second)
		want := map[string]any{
			"sub": int64(90 * time.Second), "after": true, "before": false, "equal": true,
			"unix": later.Unix(), "milli": later.UnixMilli(), "format": later.UTC().Format(time.DateTime),
		}
		for key, value := range want {
			if commands[0].Output[key] != value {
				t.Fatalf("output %s = %#v, want %#v (all %+v)", key, commands[0].Output[key], value, commands[0].Output)
			}
		}
		if commands[0].Output["string"] == nil {
			t.Fatalf("String() missing: %+v", commands[0].Output)
		}
	})
}
